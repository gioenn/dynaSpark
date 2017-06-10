package org.apache.spark.deploy.worker

import org.apache.spark.internal.Logging

import scala.collection.mutable

/**
  * Created by Simone Ripamonti on 07/06/2017.
  */
class ControllerPollon(var activeExecutors: Int, val maximumCores: Int) extends Logging{
  type ApplicationId = String
  type ExecutorId = String
  type Cores = Double
  logInfo("MAX CORES "+maximumCores)
  private var desiredCores = new mutable.HashMap[(ApplicationId,ExecutorId), Cores]()
  private var correctedCores = new mutable.HashMap[(ApplicationId,ExecutorId), Cores]()

  def fix_cores(applicationId: ApplicationId, executorId: ExecutorId, cores: Cores): Cores = {
    desiredCores.synchronized {
      logInfo("fix_cores("+executorId+","+cores+")")
      // add your desired number of cores
      desiredCores += ((applicationId, executorId) -> cores)
      logInfo("desiredCores size "+desiredCores.size+", activeExecutors "+activeExecutors)
      // check if all requests have been collected
      if (desiredCores.keySet.size == activeExecutors) {
        logInfo("computing correctedCores")
        computeCorrectedCores()
      } else {
        // wait for others to send core requests
        logInfo("waiting for others")
        desiredCores.wait()
      }
    }

    // obtain corrected cores
    correctedCores((applicationId, executorId))
  }

  def increaseActiveExecutors(): Unit = {
    desiredCores.synchronized {
      activeExecutors += 1
      logInfo("ACTIVE EXECUTORS INCREASED: "+activeExecutors)
      if (desiredCores.keySet.size == activeExecutors) {
        computeCorrectedCores()
      }
    }
  }

  def decreaseActiveExecutors(): Unit = {
    desiredCores.synchronized {
      logInfo("ACTIVE EXECUTORS DECREASED: "+activeExecutors)
      activeExecutors -= 1
      if (desiredCores.keySet.size == activeExecutors) {
        computeCorrectedCores()
      }
    }
  }

  private def computeCorrectedCores(): Unit = {
    val totalCoresRequested = desiredCores.values.sum

    // scale requested cores if needed
    if (totalCoresRequested > maximumCores) {
      logInfo("REQUESTED CORES "+totalCoresRequested+" > MAX CORES "+maximumCores)
      correctedCores = new mutable.HashMap[(ApplicationId,ExecutorId), Cores]()
      val tempCorrectedCores = desiredCores.mapValues(requestedCores => (maximumCores / totalCoresRequested) * requestedCores)
      tempCorrectedCores.foreach(cc => correctedCores+=cc)
    } else {
      correctedCores = desiredCores
    }
    logInfo("REQUESTED CORES: " + desiredCores.values.toList
      + " TOTAL: " + totalCoresRequested
      + " CORRECTED: " + correctedCores.values.toList)

    desiredCores.notifyAll()
    desiredCores = new mutable.HashMap[(ApplicationId,ExecutorId), Cores]()
  }
}
