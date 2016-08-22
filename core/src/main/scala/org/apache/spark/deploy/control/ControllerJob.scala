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

import org.apache.spark.deploy.master.Master
import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.rpc.{RpcAddress, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.scheduler.StageInfo
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages._
import org.apache.spark.internal.Logging

import scala.collection.mutable.HashMap
import scala.concurrent.duration.Deadline

class ControllerJob(conf: SparkConf, deadlineJobMillisecond: Long) extends Logging {

  val ALPHA: Double = conf.get("spark.control.alpha").toDouble // 0.8
  val NOMINAL_RATE_RECORD_S: Double = conf.get("spark.control.nominalrate").toDouble // 1000.0
  val OVERSCALE: Int = conf.get("spark.control.overscale").toInt // 2

  val NOMINAL_RATE_DATA_S: Double = conf.get(
    "spark.control.nominalratedata").toDouble  // 48000000.0

  val numMaxExecutor: Int = conf.get("spark.control.maxexecutor").toInt // 4
  val coreForVM: Int = conf.get("spark.control.coreforvm").toInt  // 8

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

  def computeDeadlineStage(stage: StageInfo, weight: Long): Long = {
    val deadline = (ALPHA * (deadlineJobMillisecond - stage.submissionTime.get)
      / (weight + 1)).toLong
    if (deadline < 0) {
      logError("DEADLINE NEGATIVE -> DEADLINE NOT SATISFIED")
    }
    deadline
  }

  def computeCoreStage(deadlineStage: Long, numRecord: Long): Int = {
    logInfo("NumRecords: " + numRecord.toString +
      " DeadlineStage : " + deadlineStage.toString +
      " NominalRate: " + NOMINAL_RATE_RECORD_S.toString)
    OVERSCALE * math.ceil((numRecord / (deadlineStage / 1000.0)) / NOMINAL_RATE_RECORD_S).toInt
  }

  def computeDeadlineFirstStage(stage: StageInfo, weight: Long): Long = {
    val deadline = (ALPHA * (deadlineJobMillisecond - stage.submissionTime.get)
      / (weight + 1)).toLong
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
    if(deadlineStage > 0) {
      OVERSCALE * math.ceil(totalSize / (deadlineStage / 1000.0) / NOMINAL_RATE_DATA_S).toInt
    } else {
      numMaxExecutor * coreForVM
    }
  }

  def computeTaskForExecutors(coresToBeAllocated: Int, totalTasksStage: Int): IndexedSeq[Int] = {
    numExecutor = math.ceil(coresToBeAllocated.toDouble / coreForVM.toDouble).toInt

    if (numExecutor > numMaxExecutor) {
      numExecutor = numMaxExecutor
    }

    val coresPerExecutor = (1 to numExecutor).map {
      i => if (coresToBeAllocated % numExecutor >= i) {
        1 + (coresToBeAllocated / numExecutor)
      } else coresToBeAllocated / numExecutor
    }

    val remainingTasks = totalTasksStage - coresPerExecutor.foldLeft(0) {
      (agg, x) => totalTasksStage * x / coresToBeAllocated + agg
    }

    val taskPerExecutor = (0 until numExecutor).map { i =>
      if (i < remainingTasks) {
        totalTasksStage * coresPerExecutor(i) / coresToBeAllocated + 1
      }
      else {
        totalTasksStage * coresPerExecutor(i) / coresToBeAllocated
      }
    }

    // val taskPerExecutor = scala.collection.mutable.IndexedSeq((0 until numExecutor).map {
    //  tasks * coresPerExecutor(_) / coresToBeAllocated
    // }: _*)

    // val remainingTasks = tasks - taskPerExecutor.sum

    // (0 until remainingTasks).foreach { i =>
    //  taskPerExecutor(i % numExecutor) = taskPerExecutor(i % numExecutor) + 1
    // }

    taskPerExecutor
  }

  def computeCoreForExecutors(coresToBeAllocated: Int): IndexedSeq[Int] = {
    numExecutor = math.ceil(coresToBeAllocated.toDouble / coreForVM.toDouble).toInt

    if (numExecutor > numMaxExecutor) {
      logError("NUM EXECUTORS TOO HIGH: %d > NUM MAX EXECUTORS %d".format(
        numExecutor, numMaxExecutor
      ))
      numExecutor = numMaxExecutor
      val coreForExecutors = (1 to numExecutor).map {
        i => coreForVM
      }
      coreForExecutors
    } else {
      val coresPerExecutor = (1 to numExecutor).map {
        i => if (coresToBeAllocated % numExecutor >= i) {
          1 + (coresToBeAllocated / numExecutor / OVERSCALE)
        } else coresToBeAllocated / numExecutor / OVERSCALE
      }
      coresPerExecutor
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

  class ControllerJob(
                       override val rpcEnv: RpcEnv,
                       systemName: String,
                       endpointName: String,
                       val conf: SparkConf,
                       val securityMgr: SecurityManager)
    extends ThreadSafeRpcEndpoint with Logging {
  }

}