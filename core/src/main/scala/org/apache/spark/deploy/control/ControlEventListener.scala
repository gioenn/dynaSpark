/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.control

import scala.collection.mutable.{HashMap, HashSet, ListBuffer}
import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.ui.jobs.JobProgressListener
import org.apache.spark.ui.jobs.UIData._

/**
  *
  * Created by Matteo on 20/07/2016.
  *
  *
  * All access to the data structures in this class must be synchronized on the
  * class, since the UI thread and the EventBus loop may otherwise be reading and
  * updating the internal data structures concurrently.
  */
class ControlEventListener(conf: SparkConf) extends JobProgressListener(conf) with Logging {

  // Application:
  var totaldurationremaining = -1L
  var totalStageRemaining = -1L

  // Data from spark-defaults.conf
  val ALPHA: Double = conf.get("spark.control.alpha").toDouble
  val DEADLINE: Int = conf.get("spark.control.deadline").toInt
  var executorNeeded: Int = conf.get("spark.control.maxexecutor").toInt

  // Master
  def master: String = conf.get("spark.master")

  def appId: String = conf.get("spark.app.id")

  // Jobs:
  val jobIdToController = new HashMap[JobId, ControllerJob]

  // Stages:
  val activePendingStages = new HashMap[StageId, StageInfo]
  val stageIdToDeadline = new HashMap[StageId, Long]
  val stageIdToCore = new HashMap[StageId, Double]
  val stageIdToDuration = new HashMap[StageId, Long]
  val stageIdToNumRecords = new HashMap[StageId, Long]
  val stageIdToParentsIds = new HashMap[StageId, List[StageId]]
  var firstStageId: StageId = -1
  var lastStageId: StageId = -1
  var stageIdsToComputeNominalRecord = scala.collection.mutable.Set[StageId]()

  // Executor
  var executorAvailable = Set[ExecutorId]()
  var executorBinded = Set[ExecutorId]()
  var execIdToStageId = new HashMap[ExecutorId, Long].withDefaultValue(0)
  var stageIdToExecId = new HashMap[Int, Set[ExecutorId]].withDefaultValue(Set())
  var executorIdToInfo = new HashMap[ExecutorId, ExecutorInfo]
  var executorNeededIndexAvaiable = List[Int]()
  var executorNeededPendingStages = new HashMap[StageId, Int]
  var deadlineApp: Long = 0

  val heuristicType = conf.getInt("spark.control.heuristic", 0)
  val heuristic: HeuristicBase =
    if (heuristicType == 1 && conf.contains("spark.control.stagecores") && conf.contains("spark.control.stagedeadlines") && conf.contains("spark.control.stage"))
      new HeuristicFixed(conf)
    else if (heuristicType == 2)
      new HeuristicControlUnlimited(conf)
    else
      new HeuristicControl(conf)


  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    val jobGroup = for (
      props <- Option(jobStart.properties);
      group <- Option(props.getProperty(SparkContext.SPARK_JOB_GROUP_ID))
    ) yield group
    val jobData: JobUIData =
      new JobUIData(
        jobId = jobStart.jobId,
        submissionTime = Option(jobStart.time).filter(_ >= 0),
        stageIds = jobStart.stageIds,
        jobGroup = jobGroup,
        status = JobExecutionStatus.RUNNING)

    jobStart.stageInfos.foreach(x => pendingStages(x.stageId) = x)
    // Compute (a potential underestimate of) the number of tasks that will be run by this job.
    // This may be an underestimate because the job start event references all of the result
    // stages' transitive stage dependencies, but some of these stages might be skipped if their
    // output is available from earlier runs.
    // See https://github.com/apache/spark/pull/3009 for a more extensive discussion.
    jobData.numTasks = {
      val allStages = jobStart.stageInfos
      val missingStages = allStages.filter(_.completionTime.isEmpty)
      missingStages.map(_.numTasks).sum
    }
    jobIdToData(jobStart.jobId) = jobData
    activeJobs(jobStart.jobId) = jobData
    for (stageId <- jobStart.stageIds) {
      stageIdToActiveJobIds.getOrElseUpdate(stageId, new HashSet[StageId]).add(jobStart.jobId)
    }
    // If there's no information for a stage, store the StageInfo received from the scheduler
    // so that we can display stage descriptions for pending stages:
    for (stageInfo <- jobStart.stageInfos) {
      stageIdToInfo.getOrElseUpdate(stageInfo.stageId, stageInfo)
      stageIdToData.getOrElseUpdate((stageInfo.stageId, stageInfo.attemptId), new StageUIData)
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = synchronized {
    val controller = jobIdToController(jobEnd.jobId)
    controller.stop()
    val jobData = activeJobs.remove(jobEnd.jobId).getOrElse {
      logWarning(s"Job completed for unknown job ${jobEnd.jobId}")
      new JobUIData(jobId = jobEnd.jobId)
    }
    jobData.completionTime = Option(jobEnd.time).filter(_ >= 0)

    jobData.stageIds.foreach(pendingStages.remove)
    for (stageId <- jobData.stageIds) {
      stageIdToActiveJobIds.get(stageId).foreach { jobsUsingStage =>
        jobsUsingStage.remove(jobEnd.jobId)
        if (jobsUsingStage.isEmpty) {
          stageIdToActiveJobIds.remove(stageId)
        }
        stageIdToInfo.get(stageId).foreach { stageInfo =>
          if (stageInfo.submissionTime.isEmpty) {
            // if this stage is pending, it won't complete, so mark it as "skipped":
            skippedStages += stageInfo
            jobData.numSkippedStages += 1
            jobData.numSkippedTasks += stageInfo.numTasks
          }
        }
      }
    }
    // firstStageId = -1
    jobIdToController(jobEnd.jobId).stop()
    jobIdToController.remove(jobEnd.jobId)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = synchronized {
    val jobId = stageIdToActiveJobIds(stageCompleted.stageInfo.stageId)
    val controller = jobIdToController(jobId.head)
    for (execid <- stageIdToExecId(stageCompleted.stageInfo.stageId)) {
      val workerUrl = "spark://Worker@" +
        executorIdToInfo(execid).executorHost + ":9999"
      controller.unBind(workerUrl, appId, execid, stageCompleted.stageInfo.stageId)
    }
    val stage = stageCompleted.stageInfo
    totaldurationremaining -= stageIdToDuration(stage.stageId)

    stageIdToInfo(stage.stageId) = stage
    val stageData = stageIdToData.getOrElseUpdate((stage.stageId, stage.attemptId), {
      logWarning("Stage completed for unknown stage " + stage.stageId)
      new StageUIData
    })

    for ((id, info) <- stageCompleted.stageInfo.accumulables) {
      stageData.accumulables(id) = info
    }

    if (stageIdsToComputeNominalRecord.contains(stage.stageId)) {
      stageIdToParentsIds(stage.stageId).foreach { id =>
        logInfo("STAGE ID: " + id + " INPUT RECORDS: " + stageIdToData(id, 0).inputRecords +
          " SHUFFLE READ RECORDS: " + stageIdToData(id, 0).shuffleReadRecords +
          " OUTPUT RECORDS: " + stageIdToData(id, 0).outputRecords +
          " SHUFFLE WRITE RECORDS: " + stageIdToData(id, 0).shuffleWriteRecords)
      }
      logInfo("STAGE ID: " + stage.stageId + " INPUT RECORDS: " +
        stageIdToData(stage.stageId, 0).inputRecords +
        " SHUFFLE READ RECORDS: " + stageIdToData(stage.stageId, 0).shuffleReadRecords +
        " OUTPUT RECORDS: " + stageIdToData(stage.stageId, 0).outputRecords +
        " SHUFFLE WRITE RECORDS: " + stageIdToData(stage.stageId, 0).shuffleWriteRecords)
      var recordsRead = stageIdToParentsIds(stage.stageId).foldLeft(0L) {
        (agg, x) =>
          agg + stageIdToData(x, 0).outputRecords + stageIdToData(x, 0).shuffleWriteRecords
      }
      if (recordsRead == 0) {
        recordsRead = stageIdToParentsIds(stage.stageId).foldLeft(0L) {
          (agg, x) =>
            agg + stageIdToData(x, 0).inputRecords + stageIdToData(x, 0).shuffleReadRecords
        }
      }
      if (recordsRead == 0) {
        recordsRead = stageData.inputRecords + stageData.shuffleReadRecords
      }
      if (stageData.numCompleteTasks == recordsRead || recordsRead == 0) {
        recordsRead = stageData.outputRecords + stageData.shuffleWriteRecords
      }
      logInfo("RECORDS FOR COMPUTE NOMINAL RATE: " + recordsRead)
      heuristic.computeNominalRecord(stage, stageIdToData(stage.stageId, 0).executorRunTime,
        recordsRead)
      stageIdsToComputeNominalRecord.remove(stage.stageId)
    }

    activeStages.remove(stage.stageId)
    if (stage.failureReason.isEmpty) {
      completedStages += stage
    } else {
      failedStages += stage
    }

    for (
      activeJobsDependentOnStage <- stageIdToActiveJobIds.get(stage.stageId);
      jobId <- activeJobsDependentOnStage;
      jobData <- jobIdToData.get(jobId)
    ) {
      jobData.numActiveStages -= 1
      if (stage.failureReason.isEmpty) {
        if (!stage.submissionTime.isEmpty) {
          jobData.completedStageIndices.add(stage.stageId)
        }
      } else {
        jobData.numFailedStages += 1
      }
    }
    for (execId <- stageIdToExecId(stage.stageId)) {
      executorAvailable += execId
      executorBinded -= execId
    }
    activePendingStages.remove(stage.stageId)
    for (stage <- activePendingStages) {
      val stageId = stage._2.stageId
      val controller = jobIdToController(stageIdToActiveJobIds(stageId).head)

      val newDeadline = heuristic.computeDeadlineStage(System.currentTimeMillis(),
        deadlineApp,
        totalStageRemaining,
        totaldurationremaining,
        stageIdToDeadline(stageId),
        stageId)
      stageIdToDeadline(stageId) = newDeadline
      val numRecord = stageIdToNumRecords.getOrElse(stageId, 0)
      if (numRecord != 0) {
        stageIdToCore(stageId) = heuristic.computeCoreStage(newDeadline, numRecord.asInstanceOf[Number].longValue, stageId)
      } else {
        stageIdToCore(stageId) = heuristic.computeCoreStage(stageId = stageId, firstStage = true)
      }
      if (stageId == lastStageId) {
        stageIdToCore(stageId) = heuristic.computeCoreStage(newDeadline, numRecord.asInstanceOf[Number].longValue, stageId = stageId, lastStage = true)
      }
      val stageExecNeeded = heuristic.computeCoreForExecutors(stageIdToCore(stageId),
        stageId,
        stageId == lastStageId).size
      if (executorAvailable.size >= stageExecNeeded) {
        totalStageRemaining -= 1
        executorNeededIndexAvaiable = (0 until stageExecNeeded).toList
        // LAUNCH BIND
        for (exec <- executorAvailable.toList.take(stageExecNeeded)) {
          onExecutorAssigned(SparkListenerExecutorAssigned(exec, stage._2.stageId))
        }
      }
    }
  }

  override def onStageWeightSubmitted
  (stageSubmitted: SparkStageWeightSubmitted): Unit = synchronized {
    val stage = stageSubmitted.stageInfo
    val genstage = if (firstStageId != -1) 1 else 0
    stageIdToParentsIds(stage.stageId) = stageSubmitted.parentsIds

    if (totaldurationremaining == -1L) {
      totaldurationremaining = stageSubmitted.totalduration
    }
    if (totalStageRemaining == -1L) {
      totalStageRemaining = stageSubmitted.stageIds.size - 1 + genstage
    }
    stageIdToDuration(stage.stageId) = stageSubmitted.duration

    // if (stageWeight == 0.0) lastStageId = stage.stageId
    val jobId = stageIdToActiveJobIds(stage.stageId)
    logInfo("JobID of stageId " + stage.stageId.toString + " : " + jobId.toString())
    if (stageSubmitted.genstage) {
      logInfo("FIRST STAGE FIRST JOB GENERATES/LOADS DATA")
      firstStageId = stage.stageId
      val controller = new ControllerJob(conf,
        System.currentTimeMillis() + (ALPHA * DEADLINE).toLong)

      stageIdToDeadline(stage.stageId) = heuristic.computeDeadlineStage(stage.submissionTime.get,
        deadlineApp,
        totalStageRemaining,
        totaldurationremaining,
        stageIdToDuration(stage.stageId),
        stage.stageId,
        firstStage = true)

      if (completedStages.nonEmpty) {
        stageIdToCore(stage.stageId) = heuristic.computeCoreStage(stageId = completedStages.toList.head.stageId, firstStage = true)
      } else {
        stageIdToCore(stage.stageId) = heuristic.computeCoreStage(stageId = stage.stageId, firstStage = true)
      }

      jobIdToController(jobId.head) = controller
      logInfo(jobIdToController.toString())

    } else {
      if (deadlineApp == 0) deadlineApp = System.currentTimeMillis() + (ALPHA * DEADLINE).toLong
      val controller = jobIdToController.getOrElse(jobId.head,
        new ControllerJob(conf, deadlineApp))
      jobIdToController(jobId.head) = controller
      var start = stage.submissionTime.get
      //      if (activeStages.nonEmpty) {
      //        start = start + activeStages.map(x => stageIdToDeadline(x._1)).min
      //      }
      val deadlineStage = heuristic.computeDeadlineStage(start,
        deadlineApp,
        totalStageRemaining,
        totaldurationremaining,
        stageIdToDuration(stage.stageId),
        stage.stageId)
      stageIdToDeadline(stage.stageId) = deadlineStage
      logInfo("NOMINAL RATE PASSED = " + stageSubmitted.nominalrate.toString)
      heuristic.NOMINAL_RATE_RECORD_S = stageSubmitted.nominalrate
      if (stageSubmitted.nominalrate > 0.0) {
        // FIND RECORD IN INPUT
        logInfo("PARENTS IDS: " + stageSubmitted.parentsIds.toString)
        val numRecord = stageSubmitted.parentsIds.foldLeft(0L) {
          (agg, x) =>
            agg + stageIdToData(x, 0).outputRecords + stageIdToData(x, 0).shuffleWriteRecords
        }
        stageIdToNumRecords(stage.stageId) = numRecord
        if (numRecord == 0) {
          val numRecord = stageSubmitted.parentsIds.foldLeft(0L) {
            (agg, x) =>
              agg + stageIdToData(x, 0).inputRecords + stageIdToData(x, 0).shuffleReadRecords
          }
          stageIdToNumRecords(stage.stageId) = numRecord
          if (numRecord != 0) {
            stageIdToCore(stage.stageId) = heuristic.computeCoreStage(deadlineStage,
              numRecord,
              stage.stageId)

          } else {
            logError("STAGEID: " + stage.stageId + " NUM RECORD == 0")
            stageIdToCore(stage.stageId) = heuristic.computeCoreStage(stageId = stage.stageId, firstStage = true)
          }
        } else {
          stageIdToCore(stage.stageId) = heuristic.computeCoreStage(deadlineStage, numRecord, stage.stageId)
        }
      } else {
        stageIdToCore(stage.stageId) = heuristic.computeCoreStage(stageId = stage.stageId, firstStage = true)
        stageIdsToComputeNominalRecord.add(stage.stageId)
      }
      val lastStage = stage.stageId == lastStageId
      if (stage.stageId == lastStageId) {
        stageIdToCore(stage.stageId) = heuristic.computeCoreStage(deadlineStage,
          stageIdToNumRecords(stage.stageId), stage.stageId, lastStage = true)
      }
      // ASK MASTER NEEDED EXECUTORS
      val coreForExecutors = heuristic.computeCoreForExecutors(stageIdToCore(stage.stageId), stage.stageId, lastStage)

      controller.askMasterNeededExecutors(master, firstStageId, coreForExecutors, appId)
      executorNeeded = coreForExecutors.size
      if (coreForExecutors.contains(-1)) {
        controller.kill(master, appId, executorAvailable.toSeq)
      }

    }

    logInfo("DEADLINE STAGES: " + stageIdToDeadline.toString)
    logInfo("CORE STAGES: " + stageIdToCore.toString)
    logInfo("EXEC AVAIL: " + executorAvailable.toString)
    logInfo("ACTIVE STAGES: " + activeStages.toString)
    logInfo("ACTIVE PENDING STAGES: " + activePendingStages.toString)
    if (executorAvailable.size >= executorNeeded) {
      activeStages(stage.stageId) = stage
      totalStageRemaining -= 1

      pendingStages.remove(stage.stageId)

      stageIdToInfo(stage.stageId) = stage
      val stageData = stageIdToData.getOrElseUpdate((stage.stageId, stage.attemptId),
        new StageUIData)

      stageData.description = Option(stageSubmitted.properties).flatMap {
        p => Option(p.getProperty(SparkContext.SPARK_JOB_DESCRIPTION))
      }

      for (
        activeJobsDependentOnStage <- stageIdToActiveJobIds.get(stage.stageId);
        jobId <- activeJobsDependentOnStage;
        jobData <- jobIdToData.get(jobId)
      ) {
        jobData.numActiveStages += 1

        // If a stage retries again, it should be removed from completedStageIndices set
        jobData.completedStageIndices.remove(stage.stageId)
      }

      executorNeededIndexAvaiable = (0 until executorNeeded).toList
      // LAUNCH BIND
      for (exec <- executorAvailable.toList.take(executorNeeded)) {
        onExecutorAssigned(SparkListenerExecutorAssigned(exec, stage.stageId))
      }
    } else {
      logError("NOT ENOUGH RESOURSE TO DO START STAGE NEED " +
        (executorNeeded - executorAvailable.size).toString + " EXEC")
      logInfo("Waiting for executor available...")
      activePendingStages(stage.stageId) = stage
      executorNeededPendingStages(stage.stageId) = executorNeeded
    }

  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    val info = taskEnd.taskInfo
    // If stage attempt id is -1, it means the DAGScheduler had no idea which attempt this task
    // completion event is for. Let's just drop it here. This means we might have some speculation
    // tasks on the web ui that's never marked as complete.
    if (info != null && taskEnd.stageAttemptId != -1) {
      val stageData = stageIdToData.getOrElseUpdate((taskEnd.stageId, taskEnd.stageAttemptId), {
        logWarning("Task end for unknown stage " + taskEnd.stageId)
        new StageUIData
      })

      for (accumulableInfo <- info.accumulables) {
        stageData.accumulables(accumulableInfo.id) = accumulableInfo
      }

      val execSummaryMap = stageData.executorSummary
      val execSummary = execSummaryMap.getOrElseUpdate(info.executorId, new ExecutorSummary)

      taskEnd.reason match {
        case Success =>
          execSummary.succeededTasks += 1
        case _ =>
          execSummary.failedTasks += 1
      }
      execSummary.taskTime += info.duration
      stageData.numActiveTasks -= 1

      val errorMessage: Option[String] =
        taskEnd.reason match {
          case org.apache.spark.Success =>
            stageData.completedIndices.add(info.index)
            stageData.numCompleteTasks += 1
            None
          case e: ExceptionFailure => // Handle ExceptionFailure because we might have accumUpdates
            stageData.numFailedTasks += 1
            Some(e.toErrorString)
          case e: TaskFailedReason => // All other failure cases
            stageData.numFailedTasks += 1
            Some(e.toErrorString)
        }

      val taskMetrics = Option(taskEnd.taskMetrics)
      taskMetrics.foreach { m =>
        val oldMetrics = stageData.taskData.get(info.taskId).flatMap(_.metrics)
        updateAggregateMetrics(stageData, info.executorId, m, oldMetrics)
      }

      val taskData = stageData.taskData.getOrElseUpdate(info.taskId, TaskUIData(info, None))
      taskData.updateTaskInfo(info)
      taskData.updateTaskMetrics(taskMetrics)
      taskData.errorMessage = errorMessage

      for (
        activeJobsDependentOnStage <- stageIdToActiveJobIds.get(taskEnd.stageId);
        jobId <- activeJobsDependentOnStage;
        jobData <- jobIdToData.get(jobId)
      ) {
        jobData.numActiveTasks -= 1
        taskEnd.reason match {
          case Success =>
            jobData.numCompletedTasks += 1
          case _ =>
            jobData.numFailedTasks += 1
        }
      }
    }
  }

  /**
    * Called when the driver registers a new executor.
    */
  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = synchronized {
    executorAvailable += executorAdded.executorId
    executorIdToInfo(executorAdded.executorId) = executorAdded.executorInfo
    logInfo("EXECUTOR AVAILABLE: " + executorAvailable.toString)
    for (stage <- activePendingStages) {
      val stageId = stage._2.stageId
      val controller = jobIdToController(stageIdToActiveJobIds(stageId).head)

      val newDeadline = heuristic.computeDeadlineStage(System.currentTimeMillis(),
        deadlineApp,
        totalStageRemaining,
        totaldurationremaining,
        stageIdToDuration(stageId),
        stageId)
      stageIdToDeadline(stageId) = newDeadline
      val numRecord = stageIdToNumRecords.getOrElse(stageId, 0)
      if (numRecord != 0) {
        stageIdToCore(stageId) = heuristic.computeCoreStage(newDeadline,
          numRecord.asInstanceOf[Number].longValue,
          stageId)
      } else {
        stageIdToCore(stageId) = heuristic.computeCoreStage(stageId = stageId, firstStage = true)
      }
      val stageExecNeeded = heuristic.computeCoreForExecutors(stageIdToCore(stageId), stageId,
        stageId == lastStageId).size
      if (executorAvailable.size >= stageExecNeeded) {
        totalStageRemaining -= 1
        executorNeededIndexAvaiable = (0 until stageExecNeeded).toList
        // LAUNCH BIND
        for (exec <- executorAvailable.toList.take(stageExecNeeded)) {
          onExecutorAssigned(SparkListenerExecutorAssigned(exec, stage._2.stageId))
        }
      }
    }
  }

  override def onExecutorAssigned
  (executorAssigned: SparkListenerExecutorAssigned): Unit = synchronized {
    val stageId = executorAssigned.stageId
    execIdToStageId(executorAssigned.executorId) = stageId
    stageIdToExecId(stageId) += executorAssigned.executorId
    logInfo("Assigned stage %s to executor %s".format(
      stageId, executorAssigned.executorId))
    val jobId = stageIdToActiveJobIds(stageId)
    val workerUrl = "spark://Worker@" +
      executorIdToInfo(executorAssigned.executorId).executorHost + ":9999"
    val controller = jobIdToController.getOrElse(jobId.head,
      new ControllerJob(conf, deadlineApp))
    jobIdToController(jobId.head) = controller
    val index = executorNeededIndexAvaiable.last
    executorNeededIndexAvaiable = executorNeededIndexAvaiable.dropRight(1)
    val lastStage = stageId == lastStageId
    if (stageId != firstStageId && !stageIdsToComputeNominalRecord.contains(stageId)) {

      val taskForExecutorId = heuristic.computeTaskForExecutors(stageIdToCore(stageId), stageIdToInfo(stageId).numTasks, lastStage)(index)
      val (coreMin, coreMax, coreToStart) = heuristic.computeCores(stageIdToCore(stageId), index, stageId, lastStage)

      controller.scaleExecutor(workerUrl, appId, executorAssigned.executorId, coreToStart)
      controller.initControllerExecutor(
        workerUrl,
        appId,
        executorAssigned.executorId,
        stageId,
        coreMin = coreMin,
        coreMax,
        stageIdToDeadline(stageId),
        coreToStart,
        taskForExecutorId)
    } else {
      val taskForExecutorId = heuristic.computeTaskForExecutors(stageIdToCore(stageId),
        stageIdToInfo(stageId).numTasks, lastStage)(index)
      controller.scaleExecutor(workerUrl, "", executorAssigned.executorId, controller.coreForVM)
      controller.bindWithTasks(workerUrl, appId, executorAssigned.executorId, stageId, taskForExecutorId)
    }
    executorAvailable -= executorAssigned.executorId
    executorBinded += executorAssigned.executorId
  }

}


