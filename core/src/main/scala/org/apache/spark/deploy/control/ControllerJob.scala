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

  def scaleExecutor(workerUrl: String, appId: String, executorId: String, core: Double): Unit = {
    val workerEndpoint = rpcEnv.setupEndpointRefByURI(workerUrl)
    workerEndpoint.send(ScaleExecutor(appId, executorId, core))
  }

  def bindWithTasks(
                     workerUrl: String, applicationId: String, executorId: String, stageId: Long, tasks: Int): Unit = {
    if (tasks > 0) {
      val workerEndpoint = rpcEnv.setupEndpointRefByURI(workerUrl)
      workerEndpoint.send(BindWithTasks(applicationId,
        executorId, stageId.toInt, tasks))
      logInfo("SEND BIND TO WORKER EID %s, SID %s WITH TASKS %d APP %s".format
      (executorId, stageId, tasks, applicationId))
    }
  }

  def unBind(workerUrl: String, applicationId: String, executorId: String, stageId: Long): Unit = {
    val workerEndpoint = rpcEnv.setupEndpointRefByURI(workerUrl)
    workerEndpoint.send(UnBind(applicationId, executorId, stageId.toInt))
    logInfo("SEND UNBIND TO WORKER EID %s, SID %s APP %s".format
    (executorId, stageId, applicationId))
  }

  def initControllerExecutor(workerUrl: String, applicationId: String, executorId: String,
                             stageId: Long, coreMin: Double, coreMax: Double,
                             deadline: Long, core: Double, tasksForExecutor: Int): Unit =
    synchronized {
      if (tasksForExecutor > 0) {
        val workerEndpoint = rpcEnv.setupEndpointRefByURI(workerUrl)
        workerEndpoint.send(InitControllerExecutor( applicationId,
          executorId, stageId, coreMin, coreMax, tasksForExecutor, deadline, core))
        logInfo("SEND INIT TO EXECUTOR CONTROLLER EID %s, SID %s, TASK %s, DL %s, C %s, APP %s".format
        (executorId, stageId, tasksForExecutor, deadline, core, applicationId))
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
