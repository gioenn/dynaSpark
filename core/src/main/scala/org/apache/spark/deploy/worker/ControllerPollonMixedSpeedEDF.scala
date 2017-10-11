package org.apache.spark.deploy.worker

import scala.collection.mutable

/**
  * Allocate available cores to applications in a way that is proportional to the average speed of the application and
  * to normalized time distance to the deadline
  * Created by Simone Ripamonti on 02/10/2017.
  */
class ControllerPollonMixedSpeedEDF(override val maximumCores: Int, Ts: Long, alpha: Double, val avgNominalRate: Long) extends ControllerPollonAbstract(maximumCores, Ts, alpha) {
  override def correctCores(desiredCores: mutable.HashMap[(ApplicationId, ExecutorId), Cores]): mutable.HashMap[(ApplicationId, ExecutorId), Cores] = {

    var correctedCores = new mutable.HashMap[(ApplicationId, ExecutorId), Cores]()
    val currentTimestamp = System.currentTimeMillis()
    val remainingTimeToComplete: mutable.HashMap[(ApplicationId, ExecutorId), Double] = activeExecutors.map { case (id, controllerExecutor) =>
      (id, ((controllerExecutor.deadlineAppTimestamp - currentTimestamp) / (1000)).toDouble)
    }
    val minTimeToComplete: Double = remainingTimeToComplete.values.toList.min
    val trasledReverseTTC: mutable.HashMap[(ApplicationId, ExecutorId), Double] = remainingTimeToComplete.map { case (id, ttc) =>
      (id, (1/ttc)-(1/minTimeToComplete)+0.001)
    }
    val TRTTCSum = trasledReverseTTC.values.sum
    val deadlineWeight = trasledReverseTTC.map{ case(id, trttc) =>
      (id, 1 - (trttc/TRTTCSum))
    }
    val speed = activeExecutors.map { case (id, controllerExecutor) => (id, avgNominalRate / controllerExecutor.nominalRateApp) }
    val k = speed.map{case (id, speed) => (id, speed*deadlineWeight(id))}

    var desiredCoresLocal = desiredCores.clone()
    var remainingCores = maximumCores.toDouble

    while (!desiredCoresLocal.isEmpty && remainingCores > 0) {
      var completedExecutors: mutable.MutableList[(ApplicationId, ExecutorId)] = mutable.MutableList[(ApplicationId, ExecutorId)]()
//      val totalTimeToComplete: Double = normalizedTimeToComplete.values.toList.sum
//      val totalSpeed = speed.values.sum
      val totalK = k.values.sum
      var totalAssignedCores: Double = 0
      desiredCoresLocal.foreach { case (id, cores) => {
//        val assignableCores = (normalizedTimeToComplete(id) / totalTimeToComplete) * (speed(id) / totalSpeed) * remainingCores
        val assignableCores =(k(id) / totalK)*remainingCores
        val assignedCores = math.min(assignableCores, cores)
        // assign cores
        correctedCores(id) = correctedCores.getOrElse(id, 0.toDouble) + assignedCores
        // reduce asked cores
        desiredCoresLocal(id) = desiredCoresLocal(id) - assignedCores
        // remove executor if given all the requested cores
        if (assignableCores >= cores) {
          completedExecutors += id
        }
        // update accumulators
        totalAssignedCores += assignedCores
      }
      }
      // update available cores to assign
      remainingCores -= totalAssignedCores
      // update executors requesting other cores
      completedExecutors.foreach { id => {
        desiredCoresLocal -= id
//        normalizedTimeToComplete -= id
//        speed -= id
        k -= id
      }
      }


    }

    return correctedCores
  }
}
