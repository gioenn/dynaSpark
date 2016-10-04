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

import org.apache.spark.deploy.DeployMessages.{KillExecutors, UnregisterApplication}
import org.apache.spark.deploy.master.Master
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{RpcAddress, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.{SecurityManager, SparkConf}

import scala.collection.mutable.{HashMap, ListBuffer}


class ControllerJob(conf: SparkConf, deadlineJobMillisecond: Long) extends Logging {

  var NOMINAL_RATE_RECORD_S: Double = conf.getDouble("spark.control.nominalrate", 1000.0)
  val OVERSCALE: Int = conf.getInt("spark.control.overscale", 2)
  val minCoreExec: Int = conf.getInt("spark.control.mincore", 2)

  val NOMINAL_RATE_DATA_S: Double = conf.getDouble(
    "spark.control.nominalratedata", 48000000.0)

  val DEADLINE_TIMEOUT = 300000

  val numMaxExecutor: Int = conf.getInt("spark.control.maxexecutor", 4)
  val coreForVM: Int = conf.getInt("spark.control.coreforvm", 8)

  var numExecutor = 0
  var coreForExecutor = new HashMap[Int, Int]

  val securityMgr = new SecurityManager(conf)
  val rpcEnv = RpcEnv.create("ControllEnv", "localhost", 6666, conf, securityMgr, clientMode = true)
  val controllerEndpoint = rpcEnv.setupEndpoint("ControllJob",
    new ControllerJob(rpcEnv, "ControllEnv", "ControllJob", conf, securityMgr))
  // rpcEnv.awaitTermination()


  def stop(): Unit = {
    rpcEnv.stop(controllerEndpoint)
  }

  def computeDeadlineStage(stage: StageInfo, weight: Long, startTime: Long,
                           alpha: Double, deadline: Long): Long = {
    var deadline = (deadlineJobMillisecond - startTime) / (weight + 1)
    if (deadline < 0) {
      logError("ALPHA DEADLINE NEGATIVE -> ALPHA DEADLINE NOT SATISFIED")
      deadline = (((deadlineJobMillisecond + (1 - alpha) * deadline) - startTime)
        / (weight + 1)).toLong
    }
    if (deadline < 0) {
      logError("DEADLINE NEGATIVE -> DEADLINE NOT SATISFIED")
      deadline = DEADLINE_TIMEOUT
    }
    deadline
  }

  def computeNominalRecord(stage: StageInfo, duration: Long, recordsRead: Double): Unit = {
    // val duration = (stage.completionTime.get - stage.submissionTime.get) / 1000.0
    NOMINAL_RATE_RECORD_S = recordsRead / (duration / 1000.0)
    logInfo("DURATION STAGE ID " + stage.stageId + " : " + duration)
    logInfo("NOMINAL RECORD/S STAGE ID " + stage.stageId + " : " + NOMINAL_RATE_RECORD_S)
    conf.set("spark.control.nominalrate", NOMINAL_RATE_RECORD_S.toString)
  }

  def computeCoreStage(deadlineStage: Long, numRecord: Long): Int = {
    logInfo("NumRecords: " + numRecord.toString +
      " DeadlineStage : " + deadlineStage.toString +
      " NominalRate: " + NOMINAL_RATE_RECORD_S.toString)
    OVERSCALE * math.ceil((numRecord / (deadlineStage / 1000.0)) / NOMINAL_RATE_RECORD_S).toInt
  }

  def computeDeadlineFirstStage(stage: StageInfo, weight: Long): Long = {
    val deadline = (deadlineJobMillisecond - stage.submissionTime.get) / (weight + 1)
    if (deadline < 0) {
      logError("DEADLINE NEGATIVE -> DEADLINE NOT SATISFIED")
    }
    deadline
  }

  def computeCoreFirstStage(stage: StageInfo): Int = {
    numMaxExecutor * coreForVM
  }

  def computeCoreStageFromSize(deadlineStage: Long, totalSize: Long): Int = {
    logInfo("TotalSize RDD First Stage: " + totalSize.toString)
    if (deadlineStage > 0) {
      OVERSCALE * math.ceil(totalSize / (deadlineStage / 1000.0) / NOMINAL_RATE_DATA_S).toInt
    } else {
      numMaxExecutor * coreForVM
    }
  }

  def computeTaskForExecutors(coresToBeAllocated: Int, totalTasksStage: Int): List[Int] = {
    numExecutor = math.ceil(coresToBeAllocated.toDouble / coreForVM.toDouble).toInt
    if (numExecutor > numMaxExecutor) {
      logError("NUM EXECUTORS TOO HIGH: %d > NUM MAX EXECUTORS %d".format(
        numExecutor, numMaxExecutor
      ))
      List(-1)
    } else {
      var coresToAllocate = math.max(coresToBeAllocated, numMaxExecutor * minCoreExec)
      val coresToAllocateRes = coresToAllocate % numMaxExecutor
      if (coresToAllocateRes != 0) {
        coresToAllocate = coresToAllocate + (numMaxExecutor - coresToAllocateRes)
      }
      numExecutor = numMaxExecutor
      if ((coresToAllocate / numExecutor) <= 1) {
        numExecutor = math.ceil(coresToAllocate.toDouble / coreForVM.toDouble).toInt
      }
      var coresToStart = math.ceil(coresToAllocate.toDouble / OVERSCALE).toInt
      var n = numExecutor
      var coresForExecutor = new ListBuffer[Int]()
      while (coresToStart > 0 && n > 0) {
        val a = math.floor(coresToStart / n).toInt
        coresToStart -= a
        n -= 1
        coresForExecutor += a
      }
      val coresPerExecutor = coresForExecutor.toList
      val taskForOneCore = math.floor(totalTasksStage / (coresToAllocate / OVERSCALE)).toInt
      var remainingTasks = totalTasksStage.toInt
      val taskPerExecutor = (0 until numExecutor).map { i =>
        if (remainingTasks > taskForOneCore * coresPerExecutor(i)) {
          remainingTasks -= taskForOneCore * coresPerExecutor(i)
          taskForOneCore * coresPerExecutor(i)
        } else {
          val temp = remainingTasks
          remainingTasks = 0
          temp
        }
      }
      val taskForExecutor = scala.collection.mutable.IndexedSeq(taskPerExecutor: _*)
      var j = taskForExecutor.size - 1
      while (remainingTasks > 0 && j >= 0) {
        taskForExecutor(j) += 1
        remainingTasks -= 1
        j -= 1
        if (j < 0) j = taskForExecutor.size - 1
      }
      taskForExecutor.toList
    }
  }

  def computeCoreForExecutors(coresToBeAllocated: Int): List[Int] = {
    numExecutor = math.ceil(coresToBeAllocated.toDouble / coreForVM.toDouble).toInt
    if (numExecutor > numMaxExecutor) {
      logError("NUM EXECUTORS TOO HIGH: %d > NUM MAX EXECUTORS %d".format(
        numExecutor, numMaxExecutor
      ))
      List(-1)
    } else {
      var coresToAllocate = math.max(coresToBeAllocated, numMaxExecutor * minCoreExec)
      val coresToAllocateRes = coresToAllocate % numMaxExecutor
      if (coresToAllocateRes != 0) {
        coresToAllocate = coresToAllocate + (numMaxExecutor - coresToAllocateRes)
      }
      numExecutor = numMaxExecutor
      if ((coresToAllocate / numExecutor) <= 1) {
        numExecutor = math.ceil(coresToAllocate.toDouble / coreForVM.toDouble).toInt
      }
      var coresToStart = math.ceil(coresToAllocate.toDouble / OVERSCALE).toInt
      var n = numExecutor
      var coresPerExecutor = new ListBuffer[Int]()
      while (coresToStart > 0 && n > 0) {
        val a = math.floor(coresToStart / n).toInt
        coresToStart -= a
        n -= 1
        coresPerExecutor += a
      }
      coresPerExecutor.toList
    }
  }

  def scaleExecutor(workerUrl: String, appId: String, executorId: String, core: Int): Unit = {
    val workerEndpoint = rpcEnv.setupEndpointRefByURI(workerUrl)
    workerEndpoint.send(ScaleExecutor(appId, executorId, core))
  }

  def bindwithtasks(
                     workerUrl: String, executorId: String, stageId: Long, tasks: Int): Unit = {
    val workerEndpoint = rpcEnv.setupEndpointRefByURI(workerUrl)
    workerEndpoint.send(BindWithTasks(
      executorId, stageId.toInt, tasks))
    logInfo("SEND BIND TO WORKER EID %s, SID %s WITH TASKS %d".format
    (executorId, stageId, tasks))
  }

  def unbind(workerUrl: String, executorId: String, stageId: Long): Unit = {
    val workerEndpoint = rpcEnv.setupEndpointRefByURI(workerUrl)
    workerEndpoint.send(UnBind(executorId))
    logInfo("SEND UNBIND TO WORKER EID %s, SID %s".format
    (executorId, stageId))
  }

  def initControllerExecutor(workerUrl: String, executorId: String,
                             stageId: Long, coreMin: Int, coreMax: Int,
                             deadline: Long, core: Int, tasksForExecutor: Int): Unit = {
    val workerEndpoint = rpcEnv.setupEndpointRefByURI(workerUrl)
    workerEndpoint.send(InitControllerExecutor(
      executorId, stageId, coreMin, coreMax, tasksForExecutor, deadline, core))
    logInfo("SEND INIT TO EXECUTOR CONTROLLER EID %s, SID %s, TASK %s, DL %s, C %s".format
    (executorId, stageId, tasksForExecutor, deadline, core))
  }

  def askMasterNeededExecutors
  (masterUrl: String, stageId: Long, coreNeeded: Int, appname: String): Unit = {
    val masterRef = rpcEnv.setupEndpointRef(
      RpcAddress.fromSparkURL(masterUrl), Master.ENDPOINT_NAME)
    masterRef.send(NeededCoreForExecutors(stageId, computeCoreForExecutors(coreNeeded), appname))
    logInfo("SEND NEEDED CORE TO MASTER %s, %s, %s, %s".format
    (masterUrl, stageId, computeCoreForExecutors(coreNeeded), appname))

  }

  def kill(masterUrl: String, appid: String, executorIds: Seq[String]): Unit = {
    val masterRef = rpcEnv.setupEndpointRef(
      RpcAddress.fromSparkURL(masterUrl), Master.ENDPOINT_NAME)
    masterRef.send(UnregisterApplication(appid))
    masterRef.ask[Boolean](KillExecutors(appid, executorIds))
  }

  class ControllerJob(
                       override val rpcEnv: RpcEnv,
                       systemName: String,
                       endpointName: String,
                       val conf: SparkConf,
                       val securityMgr: SecurityManager)
    extends ThreadSafeRpcEndpoint with Logging {
  }

}
