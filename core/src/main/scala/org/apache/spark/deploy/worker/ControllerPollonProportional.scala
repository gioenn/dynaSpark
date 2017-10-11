package org.apache.spark.deploy.worker
import scala.collection.mutable

/**
  * Allocate available cores to applications in a way that is proportional to the requested number of cores
  * Created by Simone Ripamonti on 01/10/2017.
  */
class ControllerPollonProportional(override val maximumCores: Int, Ts: Long, alpha: Double) extends ControllerPollonAbstract(maximumCores, Ts, alpha) {
  override def correctCores(desiredCores: mutable.HashMap[(ApplicationId, ExecutorId), Cores]): mutable.HashMap[(ApplicationId, ExecutorId), Cores] = {

    var correctedCores = new mutable.HashMap[(ApplicationId, ExecutorId), Cores]()
    val totalRequestedCores = desiredCores.values.sum

    desiredCores.foreach { case (id, cores) =>
      correctedCores += ((id, (maximumCores / totalRequestedCores) * cores))
    }

    return correctedCores
  }
}
