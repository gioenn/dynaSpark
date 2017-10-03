package org.apache.spark.deploy.worker

import java.util.{Timer, TimerTask}

import org.apache.spark.internal.Logging

import scala.collection.mutable

/**
  * Created by Simone Ripamonti on 07/06/2017.
  */
abstract class ControllerPollonAbstract(val maximumCores: Int, val Ts: Long) extends Logging {
  type ApplicationId = String
  type ExecutorId = String
  type Cores = Double
  logInfo("Max cores allocable: " + maximumCores + ", TSample: " + Ts)
  private val timer = new Timer()
  private var desiredCores = new mutable.HashMap[(ApplicationId, ExecutorId), Cores]()
  private var correctedCores = new mutable.HashMap[(ApplicationId, ExecutorId), Cores]()
  protected var activeExecutors = new mutable.HashMap[(ApplicationId, ExecutorId), ControllerExecutor]()

  def function2TimerTask(f: () => Unit): TimerTask = new TimerTask {
    def run() = f()
  }

  def start(): Unit = {
    def timerTask() = {
      activeExecutors.synchronized {
        if (activeExecutors.size > 0) {
          desiredCores = new mutable.HashMap[(ApplicationId, ExecutorId), Cores]()
          // obtain desired cores from all registered executors
          activeExecutors.foreach { case (id, controllerExecutor) =>
            desiredCores += ((id, controllerExecutor.computeDesiredCore()))
          }

          // correct desired cores
          val totalRequestedCores = desiredCores.values.sum
          if (totalRequestedCores > maximumCores) {
            correctedCores = correctCores(desiredCores)
          } else {
            correctedCores = desiredCores
          }

          logInfo("corrected cores: " + correctedCores)

          // apply desired cores
          activeExecutors.foreach { case (id, controllerExecutor) =>
            controllerExecutor.applyNextCore(correctedCores(id), desiredCores(id))
          }
        }
      }
    }

    timer.scheduleAtFixedRate(function2TimerTask(timerTask), Ts, Ts)
  }

  def stop(): Unit = {
    timer.cancel()
  }

  def registerExecutor(applicationId: ApplicationId, executorId: ExecutorId, controllerExecutor: ControllerExecutor) = {
    activeExecutors.synchronized {
      activeExecutors += (((applicationId, executorId), controllerExecutor))
      logInfo("Registering new executor " + applicationId + "/" + executorId + ", total executors " + activeExecutors.size)
    }
  }

  def unregisterExecutor(applicationId: ApplicationId, executorId: ExecutorId) = {
    activeExecutors.synchronized {
      activeExecutors -= ((applicationId, executorId))
      logInfo("Unregistering executor " + applicationId + "/" + executorId + ", total executors " + activeExecutors.size)
    }
  }

  def correctCores(desiredCores: mutable.HashMap[(ApplicationId, ExecutorId), Cores]): mutable.HashMap[(ApplicationId, ExecutorId), Cores]
}
