package org.apache.spark.deploy.worker

import scala.collection.mutable

/**
  * Allocate available cores to applications in a way that the application that is closer to the deadline has higher
  * priority in allocating its cores
  * Created by Simone Ripamonti on 02/10/2017.
  */
class ControllerPollonPureEDF(override val maximumCores: Int, Ts: Long) extends ControllerPollonAbstract(maximumCores, Ts) {
  override def correctCores(desiredCores: mutable.HashMap[(ApplicationId, ExecutorId), Cores]): mutable.HashMap[(ApplicationId, ExecutorId), Cores] = {
    var correctedCores = new mutable.HashMap[(ApplicationId, ExecutorId), Cores]()
    val currentTimestamp = System.currentTimeMillis()
    var remainingCores = maximumCores.toDouble
    val remainingTimeToComplete = activeExecutors.map { case (id, controllerExecutor) =>
      (id, controllerExecutor.deadlineAppTimestamp - currentTimestamp)
    }

    val sortedKeys = remainingTimeToComplete.toSeq.sortBy(_._2)

    sortedKeys.foreach({case (id, _) => {
      if (desiredCores(id) <= remainingCores){
        correctedCores += ((id, desiredCores(id)))
        remainingCores -= desiredCores(id)
      } else {
        correctedCores += ((id, remainingCores))
        remainingCores = 0
      }
    }})

    return correctedCores
  }
}
