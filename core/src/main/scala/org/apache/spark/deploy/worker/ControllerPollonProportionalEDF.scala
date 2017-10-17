package org.apache.spark.deploy.worker

import scala.collection.mutable

/**
  * Allocate available cores to applications in a way that it is proportional to their normalized time distance to the
  * deadline
  * Created by Simone Ripamonti on 02/10/2017.
  */
class ControllerPollonProportionalEDF(override val maximumCores: Int, Ts: Long, alpha: Double) extends ControllerPollonAbstract(maximumCores, Ts, alpha) {
  override def correctCores(desiredCores: mutable.HashMap[(ApplicationId, ExecutorId), Cores]): mutable.HashMap[(ApplicationId, ExecutorId), Cores] = {
    //    var correctedCores = new mutable.HashMap[(ApplicationId, ExecutorId), Cores]()
    //    val currentTimestamp = System.currentTimeMillis()
    //    var remainingCores = maximumCores
    //    val remainingTimeToComplete: mutable.HashMap[(ApplicationId, ExecutorId), Long] = activeExecutors.map { case (id, controllerExecutor) =>
    //      (id, controllerExecutor.deadlineAppTimestamp - currentTimestamp)
    //    }
    //
    //    val maxTimeToComplete: Long = remainingTimeToComplete.values.toList.max
    //    val minTimeToComplete: Long = remainingTimeToComplete.values.toList.min
    //
    //    val normalizedTimeToComplete: mutable.HashMap[(ApplicationId, ExecutorId), Long] = remainingTimeToComplete.map{ case (id, ttc) =>
    //      if (maxTimeToComplete - minTimeToComplete == 0){
    //        (id, 1.toLong)
    //      } else {
    //        (id, ((0-minTimeToComplete-ttc)/(0-minTimeToComplete+maxTimeToComplete)))
    //      }
    //    }
    //
    //    val totalNormalizedTimeToComplete: Long = normalizedTimeToComplete.values.toList.sum
    //
    //    normalizedTimeToComplete.foreach{case (id, nttc) => {
    //      correctedCores += ((id, (nttc/totalNormalizedTimeToComplete)*maximumCores))
    //    }}
    //
    //    return correctedCores


    var correctedCores = new mutable.HashMap[(ApplicationId, ExecutorId), Cores]()
    val currentTimestamp = System.currentTimeMillis()
    val remainingTimeToComplete: mutable.HashMap[(ApplicationId, ExecutorId), Double] = activeExecutors.map { case (id, controllerExecutor) =>
      (id, ((controllerExecutor.deadlineAppTimestamp - currentTimestamp) / 1000).toDouble)
    }
    logInfo("Remaining times to complete: "+ remainingTimeToComplete)
    val minTimeToComplete: Double = remainingTimeToComplete.values.min

    //    val normalizedTimeToComplete: mutable.HashMap[(ApplicationId, ExecutorId), Double] = remainingTimeToComplete.map { case (id, ttc) =>
    //      if(minTimeToComplete <= 0){
    //        if(ttc+1-minTimeToComplete == 0){
    //          (id, 1.toDouble)
    //        } else {
    //          (id, 1.toDouble+1.toDouble/(ttc+1-minTimeToComplete))
    //        }
    //      } else {
    //        if (ttc==0) {
    //          (id, 1.toDouble)
    //        } else {
    //          (id, 1.toDouble+1.toDouble/ttc)
    //        }
    //      }
    //    }
    val trasled: mutable.HashMap[(ApplicationId, ExecutorId), Double] = remainingTimeToComplete.map { case (id, ttc) =>
      (id, ttc - minTimeToComplete + 1)
    }
    var trasledSum = trasled.values.sum
    if (trasledSum == 0){
      trasledSum = 1
    }
    val deadlineWeight = trasled.map{ case(id, trttc) =>
      (id, 1 - (trttc/trasledSum))
    }

    logInfo("Weights: "+deadlineWeight)
    var desiredCoresLocal = desiredCores.clone()
    var remainingCores = maximumCores.toDouble

    while (!desiredCoresLocal.isEmpty && remainingCores > 0) {
      var completedExecutors: mutable.MutableList[(ApplicationId, ExecutorId)] = mutable.MutableList[(ApplicationId, ExecutorId)]()
      val totalDeadlineWeight: Double = deadlineWeight.values.sum
      var totalAssignedCores: Double = 0
      desiredCoresLocal.foreach { case (id, cores) => {
        val assignableCores = (deadlineWeight(id) / totalDeadlineWeight) * remainingCores
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
        deadlineWeight -= id
      }
      }


    }

    return correctedCores


  }
}
