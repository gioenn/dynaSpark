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

package org.apache.spark.deploy.worker

import org.apache.spark.{SecurityManager, SparkConf, TaskState}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RegisteredExecutor, _}
import org.apache.spark.util.{ThreadUtils, Utils}

import scala.collection.mutable.HashMap
import scala.util.{Failure, Success}

/**
  * Created by Matteo on 21/07/2016.
  */
class ControllerProxy
(rpcEnvWorker: RpcEnv, val driverUrl: String, val execId: Int, val appId: String, val pollon: ControllerPollon) {

  var proxyEndpoint: RpcEndpointRef = _
  val ENDPOINT_NAME: String =
    "ControllerProxy-%s".format(driverUrl.split(":").last + "-" + execId.toString)
  var totalTask: Int = _
  var controllerExecutor: ControllerExecutor = _
  var taskLaunched: Int = 0
  var taskCompleted: Int = 0
  var taskFailed: Int = 0

  val conf = new SparkConf
  val securityMgr = new SecurityManager(conf)
  val rpcEnv = RpcEnv.create("Controller", rpcEnvWorker.address.host, 5555, conf, securityMgr)

  var executorStageId: Int = -1

  var pollonKnowsMe: Boolean = false


  def start() {
    proxyEndpoint = rpcEnv.setupEndpoint(ENDPOINT_NAME, createProxyEndpoint(driverUrl))
  }

  def stop() {
    rpcEnv.stop(proxyEndpoint)
  }

  protected def createProxyEndpoint(driverUrl: String): ProxyEndpoint = {
    new ProxyEndpoint(rpcEnv, driverUrl)
  }

  def getAddress: String = {
    "spark://" + ENDPOINT_NAME + "@" + proxyEndpoint.address.toString
  }


  class ProxyEndpoint(override val rpcEnv: RpcEnv,
                      driverUrl: String) extends ThreadSafeRpcEndpoint with Logging {

    protected val executorIdToAddress = new HashMap[String, RpcAddress]

    private val executorRefMap = new HashMap[String, RpcEndpointRef]

    @volatile var driver: Option[RpcEndpointRef] = Some(rpcEnv.setupEndpointRefByURI(driverUrl))


    def sendToDriver(message: Any): Unit = {
      driver match {
        case Some(ref) => ref.send(message)
        case None =>
          logWarning(
            s"Dropping $message because the connection to driver has not yet been established")
      }
    }

    override def receive: PartialFunction[Any, Unit] = {
      case StatusUpdate(executorId, taskId, state, data) =>
        if (state == TaskState.FINISHED) {
          if (controllerExecutor != null) controllerExecutor.completedTasks += 1
          taskCompleted += 1
          logDebug("EID: %s, Completed: %d, Launched: %d, Total: %d".format(executorId,
            taskCompleted, taskLaunched, totalTask))
          if (taskCompleted == totalTask) {
            driver.get.send(ExecutorFinishedTask(executorId, executorStageId))
            taskCompleted = 0
            taskLaunched = 0
            totalTask = 0
            executorStageId = -1
            this.synchronized {
              if (pollonKnowsMe) {
                pollon.decreaseActiveExecutors()
                pollonKnowsMe = false
              }
            }
            if (controllerExecutor != null) controllerExecutor.stop()
          }
        }
        if ((TaskState.LOST == state) || (TaskState.FAILED == state)
          || (TaskState.KILLED == state)) {
          taskFailed += 1
          driver.get.send(Bind(appId, execId.toString, executorStageId))
          this.synchronized {
            if (!pollonKnowsMe) {
              pollon.increaseActiveExecutors()
              pollonKnowsMe = true
            }
          }
        }
        driver.get.send(StatusUpdate(executorId, taskId, state, data))

      case RegisteredExecutor =>
        logInfo("Already Registered Before ACK also driver knows about executor")

      case RegisterExecutorFailed(message) =>
        executorRefMap(
          executorIdToAddress(execId.toString).host).send(RegisterExecutorFailed(message))

      case LaunchTask(taskId, data) =>
        if (taskLaunched == totalTask && taskFailed == 0) {
          logInfo("Killed TID " + taskId.toString + " EID " + execId.toString)
          driver.get.send(StatusUpdate(execId.toString, taskId, TaskState.KILLED, data))
        } else {
          executorRefMap(executorIdToAddress(execId.toString).host).send(LaunchTask(taskId, data))
          taskLaunched += 1
          if (taskFailed > 0) taskFailed -= 1
          if (taskLaunched == totalTask) {
            driver.get.send(UnBind(appId, execId.toString, executorStageId))
          }

        }

      case StopExecutor =>
        logInfo("Asked to terminate Executor")
        executorRefMap(executorIdToAddress(execId.toString).host).send(StopExecutor)
        if (controllerExecutor != null) controllerExecutor.stop()

      case Bind(applicationId, executorId, stageId) =>
        logInfo("Received Binding EID " + executorId + " SID " + stageId.toString)
        driver.get.send(Bind(applicationId, executorId, stageId))
        executorStageId = stageId
        this.synchronized {
          if (!pollonKnowsMe) {
            pollon.increaseActiveExecutors()
            pollonKnowsMe = true
          }
        }
        taskCompleted = 0
        taskLaunched = 0

      case UnBind(applicationId, executorId, stageId) =>
        driver.get.send(UnBind(applicationId, executorId, stageId))
        if (controllerExecutor != null) controllerExecutor.stop()
        executorStageId = -1
        this.synchronized {
          if (pollonKnowsMe) {
            pollon.decreaseActiveExecutors()
            pollonKnowsMe = false
          }
        }

      case ExecutorScaled(timestamp, executorId, cores, newFreeCores) =>
        ControllerProxy.this.synchronized {
          var core = math.round(cores).toInt
          if (core == 0  && cores != 0.0) core = 1
          var deltaFreeCore = core - (taskLaunched - taskCompleted)
          if (deltaFreeCore > core) {
            deltaFreeCore = core
            logInfo("Delta Free Core > CORE")
          }
          driver.get.send(ExecutorScaled(timestamp,
            executorId, cores, deltaFreeCore))
          logInfo("CORES: %f, RUNNING: %d, DELTA: %d".format(
            cores, taskLaunched - taskCompleted, deltaFreeCore))
        }
    }

    override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
      case RegisterExecutor(executorId, executorRef, hostPort, cores, logUrls) =>
        logInfo("Connecting to driver: " + driverUrl)
        rpcEnv.asyncSetupEndpointRefByURI(driverUrl).flatMap { ref =>
          // This is a very fast action so we can use "ThreadUtils.sameThread"
          driver = Some(ref)
          logInfo(ref.address.toString)
          ref.ask[Boolean](RegisterExecutor(executorId, self, hostPort, cores, logUrls))
        }(ThreadUtils.sameThread).onComplete {
          // This is a very fast action so we can use "ThreadUtils.sameThread"
          case Success(msg) =>
          // Always receive `true`. Just ignore it
          case Failure(e) =>
            logError(s"Cannot register with driver: $driverUrl", e)
            System.exit(1)
        }(ThreadUtils.sameThread)
        val executorAddress = if (executorRef.address != null) {
          executorRef.address
        } else {
          context.senderAddress
        }
        executorIdToAddress(executorId) = executorAddress
        this.synchronized {
          executorRefMap.put(executorAddress.host, executorRef)
        }
        executorRef.send(RegisteredExecutor)
        context.reply(true)

      case RetrieveSparkProps =>
        val sparkProperties = driver.get.askWithRetry[Seq[(String, String)]](RetrieveSparkProps)
        context.reply(sparkProperties)
    }
  }

}