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


class ControllerJob(conf: SparkConf, appDeadlineJobMillisecond: Long) extends Logging {
  val coreForVM: Int = conf.getInt("spark.control.coreforvm", 8)
  val securityMgr = new SecurityManager(conf)
  val rpcEnv = RpcEnv.create("ControllEnv", "localhost", 6666, conf, securityMgr, clientMode = true)
  val controllerEndpoint = rpcEnv.setupEndpoint("ControllJob",
    new ControllerJob(rpcEnv, "ControllEnv", "ControllJob", conf, securityMgr))
  // rpcEnv.awaitTermination()


  def stop(): Unit = {
    rpcEnv.stop(controllerEndpoint)
  }

//  def computeDeadlineStage(weight: Double, startTime: Long,
//                           alpha: Double, deadline: Long): Long = {
//    var stageDeadline = ((appDeadlineJobMillisecond - startTime) / (weight + 1)).toLong
//    if (stageDeadline < 0) {
//      logError("ALPHA DEADLINE NEGATIVE -> ALPHA DEADLINE NOT SATISFIED")
//      stageDeadline = 1
//    }
//    stageDeadline
//  }

//  def computeNominalRecord(stage: StageInfo, duration: Long, recordsRead: Double): Unit = {
//    // val duration = (stage.completionTime.get - stage.submissionTime.get) / 1000.0
//    NOMINAL_RATE_RECORD_S = recordsRead / (duration / 1000.0)
//    logInfo("DURATION STAGE ID " + stage.stageId + " : " + duration)
//    logInfo("NOMINAL RECORD/S STAGE ID " + stage.stageId + " : " + NOMINAL_RATE_RECORD_S)
//    conf.set("spark.control.nominalrate", NOMINAL_RATE_RECORD_S.toString)
//  }

//  def fixCoreLastStage(stageId: Int, deadline: Long, numRecord: Long): Int = {
//    numExecutor = math.ceil(computeCoreStage(deadline, numRecord).toDouble
//      / coreForVM.toDouble).toInt
//    NOMINAL_RATE_RECORD_S = NOMINAL_RATE_RECORD_S * (1 - ((numMaxExecutor - numExecutor).toDouble
//      / numMaxExecutor.toDouble))
//    logInfo("New Last Stage Nominal Rate: " + NOMINAL_RATE_RECORD_S.toString)
//    computeCoreStage(deadline, numRecord)
//  }
//
//  def computeCoreStage(deadlineStage: Long, numRecord: Long): Int = {
//    logInfo("NumRecords: " + numRecord.toString +
//      " DeadlineStage : " + deadlineStage.toString +
//      " NominalRate: " + NOMINAL_RATE_RECORD_S.toString)
//    if (deadlineStage > 1) {
//      OVERSCALE * math.ceil((numRecord / (deadlineStage / 1000.0)) / NOMINAL_RATE_RECORD_S).toInt
//    } else {
//      coreForVM * numMaxExecutor
//    }
//
//  }

//  def computeDeadlineFirstStage(stage: StageInfo, weight: Double): Long = {
//    val deadline = ((appDeadlineJobMillisecond - stage.submissionTime.get) / (weight + 1)).toLong
//    if (deadline < 0) {
//      logError("DEADLINE NEGATIVE -> DEADLINE NOT SATISFIED")
//    }
//    deadline
//  }

//  def computeCoreFirstStage(stage: StageInfo): Int = {
//    numMaxExecutor * coreForVM
//  }

//  def computeCoreStageFromSize(deadlineStage: Long, totalSize: Long): Int = {
//    logInfo("TotalSize RDD First Stage: " + totalSize.toString)
//    if (deadlineStage > 1) {
//      OVERSCALE * math.ceil(totalSize / (deadlineStage / 1000.0) / NOMINAL_RATE_DATA_S).toInt
//    } else {
//      numMaxExecutor * coreForVM
//    }
//  }

//  def computeTaskForExecutors(coresToBeAllocated: Int, totalTasksStage: Int,
//                              last: Boolean): List[Int] = {
//    numExecutor = math.ceil(coresToBeAllocated.toDouble / coreForVM.toDouble).toInt
//    if (numExecutor > numMaxExecutor) {
//      logError("NUM EXECUTORS TOO HIGH: %d > NUM MAX EXECUTORS %d".format(
//        numExecutor, numMaxExecutor
//      ))
//      List(-1)
//    } else {
//      numExecutor = numMaxExecutor
//      var remainingTasks = totalTasksStage.toInt
//      var z = numExecutor
//      var taskPerExecutor = new ListBuffer[Int]()
//      while (remainingTasks > 0 && z > 0) {
//        val a = math.floor(remainingTasks / z).toInt
//        remainingTasks -= a
//        z -= 1
//        taskPerExecutor += a
//      }
//      val taskForExecutor = scala.collection.mutable.IndexedSeq(taskPerExecutor: _*)
//      var j = taskForExecutor.size - 1
//      while (remainingTasks > 0 && j >= 0) {
//        taskForExecutor(j) += 1
//        remainingTasks -= 1
//        j -= 1
//        if (j < 0) j = taskForExecutor.size - 1
//      }
//      taskForExecutor.toList
//    }
//  }

//  def computeCoreForExecutors(coresToBeAllocated: Int, last: Boolean): IndexedSeq[Double] = {
//    numExecutor = math.ceil(coresToBeAllocated.toDouble / coreForVM.toDouble).toInt
//    if (numExecutor > numMaxExecutor) {
//      logError("NUM EXECUTORS TOO HIGH: %d > NUM MAX EXECUTORS %d".format(
//        numExecutor, numMaxExecutor
//      ))
//      IndexedSeq(-1)
//    } else {
//      var coresToStart = 0
//      coresToStart = math.ceil(coresToBeAllocated.toDouble / OVERSCALE).toInt
//      numExecutor = numMaxExecutor
//      (1 to numMaxExecutor).map { x =>
//        math.ceil((coresToStart / numExecutor.toDouble) / CQ) * CQ
//      }
//    }
//  }

  def scaleExecutor(workerUrl: String, appId: String, executorId: String, core: Double): Unit = {
    val workerEndpoint = rpcEnv.setupEndpointRefByURI(workerUrl)
    workerEndpoint.send(ScaleExecutor(appId, executorId, core))
  }

  def bindwithtasks(
                     workerUrl: String, executorId: String, stageId: Long, tasks: Int): Unit = {
    if (tasks > 0) {
      val workerEndpoint = rpcEnv.setupEndpointRefByURI(workerUrl)
      workerEndpoint.send(BindWithTasks(
        executorId, stageId.toInt, tasks))
      logInfo("SEND BIND TO WORKER EID %s, SID %s WITH TASKS %d".format
      (executorId, stageId, tasks))
    }
  }

  def unbind(workerUrl: String, executorId: String, stageId: Long): Unit = {
    val workerEndpoint = rpcEnv.setupEndpointRefByURI(workerUrl)
    workerEndpoint.send(UnBind(executorId, stageId.toInt))
    logInfo("SEND UNBIND TO WORKER EID %s, SID %s".format
    (executorId, stageId))
  }

  def initControllerExecutor(workerUrl: String, executorId: String,
                             stageId: Long, coreMin: Double, coreMax: Double,
                             deadline: Long, core: Double, tasksForExecutor: Int): Unit =
    synchronized {
      if (tasksForExecutor > 0) {
        val workerEndpoint = rpcEnv.setupEndpointRefByURI(workerUrl)
        workerEndpoint.send(InitControllerExecutor(
          executorId, stageId, coreMin, coreMax, tasksForExecutor, deadline, core))
        logInfo("SEND INIT TO EXECUTOR CONTROLLER EID %s, SID %s, TASK %s, DL %s, C %s".format
        (executorId, stageId, tasksForExecutor, deadline, core))
      }
    }

  def askMasterNeededExecutors
  (masterUrl: String, stageId: Long,
   coreForExecutors: IndexedSeq[Double], appname: String): Unit = {
    val masterRef = rpcEnv.setupEndpointRef(
      RpcAddress.fromSparkURL(masterUrl), Master.ENDPOINT_NAME)
    masterRef.send(NeededCoreForExecutors(stageId, coreForExecutors, appname))
    logInfo("SEND NEEDED CORE TO MASTER %s, %s, %s, %s".format
    (masterUrl, stageId, coreForExecutors, appname))

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
