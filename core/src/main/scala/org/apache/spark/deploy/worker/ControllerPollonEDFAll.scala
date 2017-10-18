package org.apache.spark.deploy.worker
import scala.collection.mutable

/**
  * Created by Simone Ripamonti on 18/10/2017.
  * Assigns all the cores (capped to "usable" ones) to the earliest deadline application
  */
class ControllerPollonEDFAll(override val maximumCores: Int, Ts: Long, alpha: Double) extends ControllerPollonAbstract(maximumCores, Ts, alpha) {
  override def correctCores(desiredCores: mutable.HashMap[(ApplicationId, ExecutorId), Cores]): mutable.HashMap[(ApplicationId, ExecutorId), Cores] = {
    var correctedCores = new mutable.HashMap[(ApplicationId, ExecutorId), Cores]()
    val currentTimestamp = System.currentTimeMillis()
    var remainingCores = maximumCores.toDouble
    val remainingTimeToComplete = activeExecutors.map { case (id, controllerExecutor) =>
      (id, controllerExecutor.deadlineAppTimestamp - currentTimestamp)
    }
    val maxUsableCores = activeExecutors.map { case (id, controllerExecutor) =>
      (id, math.min(controllerExecutor.tasks - controllerExecutor.completedTasks, maximumCores))
    }

    val sortedKeys = remainingTimeToComplete.toSeq.sortBy(_._2)

    sortedKeys.foreach({case (id, _) => {
      if (maxUsableCores(id) <= remainingCores){
        correctedCores += ((id, maxUsableCores(id)))
        remainingCores -= maxUsableCores(id)
      } else {
        correctedCores += ((id, remainingCores))
        remainingCores = 0
      }
    }})

    return correctedCores
  }
}
