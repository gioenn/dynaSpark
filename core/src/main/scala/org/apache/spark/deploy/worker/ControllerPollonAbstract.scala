package org.apache.spark.deploy.worker

import java.util.{Timer, TimerTask}

import org.apache.spark.internal.Logging

import scala.collection.mutable

/**
  * Created by Simone Ripamonti on 07/06/2017.
  */
abstract class ControllerPollonAbstract(val maximumCores: Int, val Ts: Long, val alpha: Double) extends Logging {
  type ApplicationId = String
  type ExecutorId = String
  type Cores = Double
  logInfo("Max cores allocable: " + maximumCores + ", TSample: " + Ts)
  private val timer = new Timer()
  private var csForRate = new mutable.HashMap[(ApplicationId, ExecutorId), Cores]()
  private var csAllCores = new mutable.HashMap[(ApplicationId, ExecutorId), Cores]()
  private var cs = new mutable.HashMap[(ApplicationId, ExecutorId), Cores]()
  protected var activeExecutors = new mutable.HashMap[(ApplicationId, ExecutorId), ControllerExecutor]()

  def function2TimerTask(f: () => Unit): TimerTask = new TimerTask {
    def run() = f()
  }

  def start(): Unit = {
    def timerTask() = {
      activeExecutors.synchronized {
        if (activeExecutors.size > 0) {
          csForRate = new mutable.HashMap[(ApplicationId, ExecutorId), Cores]()
          cs = new mutable.HashMap[(ApplicationId, ExecutorId), Cores]()
          // obtain desired cores from all registered executors
          activeExecutors.foreach { case (id, controllerExecutor) =>
            var desiredCore = controllerExecutor.computeDesiredCore()
            // fix NaN
            if (desiredCore.isNaN) {
              desiredCore = 0
            }
            csForRate += ((id, desiredCore))
          }

          val sumCoresForRate = csForRate.values.sum

          if (sumCoresForRate == 0) {
            csAllCores = csForRate.map { case (id, _) => (id, 0d) }
          } else {
            var correctedCores = correctCores(csForRate).map { case (id, c) => if (c.isNaN) ((id, 0d))
            else ((id, c))
            }
            val correctedCoresSum = correctedCores.values.sum
            if ((0.99 * maximumCores) > correctedCoresSum && correctedCoresSum > 0) {
              correctedCores = correctedCores.map { case (id, core) => (id, (core / correctedCoresSum) * maximumCores) }
            }
            csAllCores = correctedCores
          }

          if (sumCoresForRate <= maximumCores) {
            // no contention
            cs = csForRate.map { case (id, thisCsForRate) => (id, alpha * csAllCores.get(id).get + (1 - alpha) * thisCsForRate) }
          } else {
            // contention
            cs = csAllCores
          }

          logInfo("corrected cores: " + cs)

          // apply desired cores
          activeExecutors.foreach { case (id, controllerExecutor) =>
            controllerExecutor.applyNextCore(cs(id), csForRate(id))
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
