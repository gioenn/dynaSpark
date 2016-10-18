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
class ControlEventListener(conf: SparkConf) extends SparkListener with Logging {

  // Application:
  @volatile var startTime = -1L
  @volatile var endTime = -1L
  var totaldurationremaining = -1L

  val ALPHA: Double = conf.get("spark.control.alpha").toDouble
  val DEADLINE: Int = conf.get("spark.control.deadline").toInt
  var executorNeeded: Int = conf.get("spark.control.maxexecutor").toInt
  var coreForVM: Int = conf.get("spark.control.coreforvm").toInt
  val coreMin: Double = conf.getDouble("spark.control.coremin", 0.0)

  // Master
  def master: String = conf.get("spark.master")

  def appid: String = conf.get("spark.app.id")

  // Jobs:
  val activeJobs = new HashMap[Int, JobUIData]
  val jobIdToData = new HashMap[Int, JobUIData]
  val jobIdToController = new HashMap[Int, ControllerJob]

  // Stages:
  val pendingStages = new HashMap[Int, StageInfo]
  val activeStages = new HashMap[Int, StageInfo]
  val activePendingStages = new HashMap[Int, StageInfo]
  val completedStages = ListBuffer[StageInfo]()
  val skippedStages = ListBuffer[StageInfo]()
  val failedStages = ListBuffer[StageInfo]()
  val stageIdToData = new HashMap[(Int, Int), StageUIData]
  val stageIdToInfo = new HashMap[Int, StageInfo]
  val stageIdToActiveJobIds = new HashMap[Int, HashSet[Int]]

  val stageIdToDeadline = new HashMap[Int, Long]
  val stageIdToCore = new HashMap[Int, Int]
  val stageIdToWeight = new HashMap[Int, Double]
  val stageIdToDuration = new HashMap[Int, Long]
  val stageIdToNumRecords = new HashMap[Int, Long]
  val stageIdToParentsIds = new HashMap[Int, List[Int]]

  var firstStageId: Int = -1
  var lastStageId: Int = -1
  var stageIdsToComputeNominalRecord = scala.collection.mutable.Set[Int]()

  var parallelStages = new HashMap[Int, ListBuffer[Int]]

  // Executor
  var executorAvailable = Set[String]()
  var executorBinded = Set[String]()
  var execIdToStageId = new HashMap[String, Long].withDefaultValue(0)
  var stageIdToExecId = new HashMap[Int, Set[String]].withDefaultValue(Set())
  var executorIdToInfo = new HashMap[String, ExecutorInfo]
  var executorNeededIndexAvaiable = List[Int]()
  var executorNeededPendingStages = new HashMap[Int, Int]
  var deadlineApp: Long = 0


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
      stageIdToActiveJobIds.getOrElseUpdate(stageId, new HashSet[Int]).add(jobStart.jobId)
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
      controller.unbind(workerUrl, execid, stageCompleted.stageInfo.stageId)
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
      controller.computeNominalRecord(stage, stageIdToData(stage.stageId, 0).executorRunTime,
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
      val weight = average(List(stageIdToWeight(stageId),
        (totaldurationremaining / stageIdToDuration(stageId)) - 1))
      val newDeadline = controller.computeDeadlineStage(stage._2,
        weight,
        System.currentTimeMillis(), ALPHA, DEADLINE)
      stageIdToDeadline(stageId) = newDeadline
      val numRecord = stageIdToNumRecords.getOrElse(stageId, 0)
      if (numRecord != 0) {
        stageIdToCore(stageId) = controller.computeCoreStage(newDeadline,
          numRecord.asInstanceOf[Number].longValue)
      } else {
        stageIdToCore(stageId) = controller.computeCoreFirstStage(stage._2)
      }
      if (stageId == lastStageId) {
        stageIdToCore(stageId) = controller.fixCoreLastStage(stageId, newDeadline,
          numRecord.asInstanceOf[Number].longValue)
      }
      val stageExecNeeded = controller.computeCoreForExecutors(stageIdToCore(stageId),
        stageId == lastStageId).size
      if (executorAvailable.size >= stageExecNeeded) {
        executorNeededIndexAvaiable = (0 until stageExecNeeded).toList
        // LAUNCH BIND
        for (exec <- executorAvailable.toList.take(stageExecNeeded)) {
          onExecutorAssigned(SparkListenerExecutorAssigned(exec, stage._2.stageId))
        }
      }
    }
  }

  def average[T]( ts: Iterable[T] )( implicit num: Numeric[T] ): Double = {
    num.toDouble( ts.sum ) / ts.size
  }

  override def onStageWeightSubmitted
  (stageSubmitted: SparkStageWeightSubmitted): Unit = synchronized {
    val stage = stageSubmitted.stageInfo
    val genstage = if (firstStageId != -1) 1 else 0
    stageIdToParentsIds(stage.stageId) = stageSubmitted.parentsIds
    if (totaldurationremaining == -1L) totaldurationremaining = stageSubmitted.totalduration
    var stageWeight = 0.0
    if (stageSubmitted.stageIds.nonEmpty) {
      stageWeight = stageSubmitted.stageIds.size - completedStages.size -
        activePendingStages.size - activeStages.size - 1 + genstage
    }
    stageIdToWeight(stage.stageId) = stageWeight
    stageWeight = average(List(stageWeight,
      (totaldurationremaining / stageSubmitted.duration) - 1))
    if (stageWeight < 0) stageWeight = 0.0
    // if (stageWeight == 0.0) lastStageId = stage.stageId
    stageIdToDuration(stage.stageId) = stageSubmitted.duration
    logInfo("STAGE ID " + stage.stageId +" WEIGHT: " + stageWeight)
    val jobId = stageIdToActiveJobIds(stage.stageId)
    logInfo("JobID of stageId " + stage.stageId.toString + " : " + jobId.toString())
    if (stageSubmitted.genstage) {
      logInfo("FIRST STAGE FIRST JOB GENERATES/LOADS DATA")
      firstStageId = stage.stageId
      val controller = new ControllerJob(conf,
        System.currentTimeMillis() + (ALPHA * DEADLINE).toLong)
      stageIdToDeadline(stage.stageId) = controller.computeDeadlineFirstStage(stage, stageWeight)
      if (completedStages.nonEmpty) {
        stageIdToCore(stage.stageId) = controller.computeCoreFirstStage(completedStages.toList.head)
      } else {
        stageIdToCore(stage.stageId) = controller.computeCoreFirstStage(stage)
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
      val deadlineStage = controller.computeDeadlineStage(stage, stageWeight, start, ALPHA, DEADLINE)
      stageIdToDeadline(stage.stageId) = deadlineStage
      logInfo("NOMINAL RATE PASSED = " + stageSubmitted.nominalrate.toString)
      controller.NOMINAL_RATE_RECORD_S = stageSubmitted.nominalrate
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
            stageIdToCore(stage.stageId) = controller.computeCoreStage(deadlineStage,
              numRecord)
          } else {
            logError("STAGEID: " + stage.stageId + " NUM RECORD == 0")
            stageIdToCore(stage.stageId) = controller.computeCoreFirstStage(stage)
          }
        } else {
          stageIdToCore(stage.stageId) = controller.computeCoreStage(deadlineStage, numRecord)
        }
      } else {
        stageIdToCore(stage.stageId) = controller.computeCoreFirstStage(stage)
        stageIdsToComputeNominalRecord.add(stage.stageId)
      }
      val lastStage = stage.stageId == lastStageId
      if (stage.stageId == lastStageId) {
        stageIdToCore(stage.stageId) = controller.fixCoreLastStage(stage.stageId, deadlineStage,
          stageIdToNumRecords(stage.stageId))
      }
      // ASK MASTER NEEDED EXECUTORS
      val coreForExecutors = controller.computeCoreForExecutors(stageIdToCore(stage.stageId), lastStage)
      controller.askMasterNeededExecutors(master, firstStageId, coreForExecutors, appid)
      executorNeeded = coreForExecutors.size
      if (coreForExecutors.contains(-1)) {
        controller.kill(master, appid, executorAvailable.toSeq)
      }

    }

    logInfo("DEADLINE STAGES: " + stageIdToDeadline.toString)
    logInfo("CORE STAGES: " + stageIdToCore.toString)
    logInfo("EXEC AVAIL: " + executorAvailable.toString)
    logInfo("ACTIVE STAGES: " + activeStages.toString)
    logInfo("ACTIVE PENDING STAGES: " + activePendingStages.toString)
    if (executorAvailable.size >= executorNeeded) {
      activeStages(stage.stageId) = stage
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

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = synchronized {
    val taskInfo = taskStart.taskInfo
    if (taskInfo != null) {
      val metrics = TaskMetrics.empty
      val stageData = stageIdToData.getOrElseUpdate((taskStart.stageId, taskStart.stageAttemptId), {
        logWarning("Task start for unknown stage " + taskStart.stageId)
        new StageUIData
      })
      stageData.numActiveTasks += 1
      stageData.taskData.put(taskInfo.taskId, TaskUIData(taskInfo, Some(metrics)))
    }
    for (
      activeJobsDependentOnStage <- stageIdToActiveJobIds.get(taskStart.stageId);
      jobId <- activeJobsDependentOnStage;
      jobData <- jobIdToData.get(jobId)
    ) {
      jobData.numActiveTasks += 1
    }
  }

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) {
    // Do nothing: because we don't do a deep copy of the TaskInfo, the TaskInfo in
    // stageToTaskInfos already has the updated status.
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
    * Upon receiving new metrics for a task, updates the per-stage and per-executor-per-stage
    * aggregate metrics by calculating deltas between the currently recorded metrics and the new
    * metrics.
    */
  def updateAggregateMetrics(
                              stageData: StageUIData,
                              execId: String,
                              taskMetrics: TaskMetrics,
                              oldMetrics: Option[TaskMetricsUIData]) {
    val execSummary = stageData.executorSummary.getOrElseUpdate(execId, new ExecutorSummary)

    val shuffleWriteDelta =
      taskMetrics.shuffleWriteMetrics.bytesWritten -
        oldMetrics.map(_.shuffleWriteMetrics.bytesWritten).getOrElse(0L)
    stageData.shuffleWriteBytes += shuffleWriteDelta
    execSummary.shuffleWrite += shuffleWriteDelta

    val shuffleWriteRecordsDelta =
      taskMetrics.shuffleWriteMetrics.recordsWritten -
        oldMetrics.map(_.shuffleWriteMetrics.recordsWritten).getOrElse(0L)
    stageData.shuffleWriteRecords += shuffleWriteRecordsDelta
    execSummary.shuffleWriteRecords += shuffleWriteRecordsDelta

    val shuffleReadDelta =
      taskMetrics.shuffleReadMetrics.totalBytesRead -
        oldMetrics.map(_.shuffleReadMetrics.totalBytesRead).getOrElse(0L)
    stageData.shuffleReadTotalBytes += shuffleReadDelta
    execSummary.shuffleRead += shuffleReadDelta

    val shuffleReadRecordsDelta =
      taskMetrics.shuffleReadMetrics.recordsRead -
        oldMetrics.map(_.shuffleReadMetrics.recordsRead).getOrElse(0L)
    stageData.shuffleReadRecords += shuffleReadRecordsDelta
    execSummary.shuffleReadRecords += shuffleReadRecordsDelta

    val inputBytesDelta =
      taskMetrics.inputMetrics.bytesRead -
        oldMetrics.map(_.inputMetrics.bytesRead).getOrElse(0L)
    stageData.inputBytes += inputBytesDelta
    execSummary.inputBytes += inputBytesDelta

    val inputRecordsDelta =
      taskMetrics.inputMetrics.recordsRead -
        oldMetrics.map(_.inputMetrics.recordsRead).getOrElse(0L)
    stageData.inputRecords += inputRecordsDelta
    execSummary.inputRecords += inputRecordsDelta

    val outputBytesDelta =
      taskMetrics.outputMetrics.bytesWritten -
        oldMetrics.map(_.outputMetrics.bytesWritten).getOrElse(0L)
    stageData.outputBytes += outputBytesDelta
    execSummary.outputBytes += outputBytesDelta

    val outputRecordsDelta =
      taskMetrics.outputMetrics.recordsWritten -
        oldMetrics.map(_.outputMetrics.recordsWritten).getOrElse(0L)
    stageData.outputRecords += outputRecordsDelta
    execSummary.outputRecords += outputRecordsDelta

    val diskSpillDelta =
      taskMetrics.diskBytesSpilled - oldMetrics.map(_.diskBytesSpilled).getOrElse(0L)
    stageData.diskBytesSpilled += diskSpillDelta
    execSummary.diskBytesSpilled += diskSpillDelta

    val memorySpillDelta =
      taskMetrics.memoryBytesSpilled - oldMetrics.map(_.memoryBytesSpilled).getOrElse(0L)
    stageData.memoryBytesSpilled += memorySpillDelta
    execSummary.memoryBytesSpilled += memorySpillDelta

    val timeDelta =
      taskMetrics.executorRunTime - oldMetrics.map(_.executorRunTime).getOrElse(0L)
    stageData.executorRunTime += timeDelta
  }

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) {
    for ((taskId, sid, sAttempt, accumUpdates) <- executorMetricsUpdate.accumUpdates) {
      val stageData = stageIdToData.getOrElseUpdate((sid, sAttempt), {
        logWarning("Metrics update for task in unknown stage " + sid)
        new StageUIData
      })
      val taskData = stageData.taskData.get(taskId)
      val metrics = TaskMetrics.fromAccumulatorInfos(accumUpdates)
      taskData.foreach { t =>
        if (!t.taskInfo.finished) {
          updateAggregateMetrics(stageData, executorMetricsUpdate.execId, metrics, t.metrics)
          // Overwrite task metrics
          t.updateTaskMetrics(Some(metrics))
        }
      }
    }
  }

  override def onApplicationStart(appStarted: SparkListenerApplicationStart) {
    startTime = appStarted.time
  }

  override def onApplicationEnd(appEnded: SparkListenerApplicationEnd) {
    endTime = appEnded.time
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
      val weight = average(List(stageIdToWeight(stageId),
        (totaldurationremaining / stageIdToDuration(stageId)) - 1))
      val newDeadline = controller.computeDeadlineStage(stage._2,
        weight,
        System.currentTimeMillis(), ALPHA, DEADLINE)
      stageIdToDeadline(stageId) = newDeadline
      val numRecord = stageIdToNumRecords.getOrElse(stageId, 0)
      if (numRecord != 0) {
        stageIdToCore(stageId) = controller.computeCoreStage(newDeadline,
          numRecord.asInstanceOf[Number].longValue)
      } else {
        stageIdToCore(stageId) = controller.computeCoreFirstStage(stage._2)
      }
      val stageExecNeeded = controller.computeCoreForExecutors(stageIdToCore(stageId),
        stageId == lastStageId).size
      if (executorAvailable.size >= stageExecNeeded) {
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
      val coreForExecutors = controller.computeCoreForExecutors(stageIdToCore(stageId),
        lastStage)
      logInfo(coreForExecutors.toString())
      val coreToStart = coreForExecutors(index)
      val taskForExecutorId = controller.computeTaskForExecutors(
        stageIdToCore(stageId),
        stageIdToInfo(stageId).numTasks, lastStage)(index)
      val maxCore = math.min(coreForExecutors(index) * controller.OVERSCALE, coreForVM)
      controller.scaleExecutor(workerUrl, appid, executorAssigned.executorId, coreToStart)
      controller.initControllerExecutor(
        workerUrl,
        executorAssigned.executorId,
        stageId,
        coreMin = coreMin,
        maxCore,
        stageIdToDeadline(stageId),
        coreToStart,
        taskForExecutorId)
    } else {
      val taskForExecutorId = controller.computeTaskForExecutors(stageIdToCore(stageId),
        stageIdToInfo(stageId).numTasks, lastStage)(index)
      controller.bindwithtasks(workerUrl, executorAssigned.executorId, stageId, taskForExecutorId)
      controller.scaleExecutor(
        workerUrl, "", executorAssigned.executorId, controller.coreForVM)
    }
    executorAvailable -= executorAssigned.executorId
    executorBinded += executorAssigned.executorId
  }
}
