package org.apache.spark.deploy.worker

import scala.collection.mutable

/**
  * Allocate available cores to applications in a way that is proportional to the average speed of the application
  * Created by Simone Ripamonti on 02/10/2017.
  */
class ControllerPollonSpeed(override val maximumCores: Int, Ts: Long, val avgNominalRate: Long) extends ControllerPollonAbstract(maximumCores, Ts) {
  override def correctCores(desiredCores: mutable.HashMap[(ApplicationId, ExecutorId), Cores]): mutable.HashMap[(ApplicationId, ExecutorId), Cores] = {
    var correctedCores = new mutable.HashMap[(ApplicationId, ExecutorId), Cores]()

    val speed = activeExecutors.map { case (id, controllerExecutor) => (id, avgNominalRate / controllerExecutor.nominalRateApp) }
    var desiredCoresLocal = desiredCores.clone()
    var remainingCores = maximumCores.toDouble

    while (!desiredCoresLocal.isEmpty && remainingCores > 0) {
      var completedExecutors: mutable.MutableList[(ApplicationId, ExecutorId)] = mutable.MutableList[(ApplicationId, ExecutorId)]()
      val totalSpeed = speed.values.sum
      var totalAssignedCores: Double = 0
      desiredCoresLocal.foreach { case (id, cores) => {
        val assignableCores = (speed(id) / totalSpeed) * remainingCores
        val assignedCores = math.min(assignableCores, cores)
        // assign cores
        correctedCores(id) = correctedCores.getOrElse(id, 0.toDouble) + assignedCores
        // reduce asked cores
        desiredCoresLocal(id) = desiredCoresLocal(id) - assignedCores
        // remove executor if given all the requested cores
        if (assignedCores >= cores) {
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
        desiredCoresLocal -= (id)
        speed -= (id)
      }
      }


    }

    return correctedCores
  }
}
