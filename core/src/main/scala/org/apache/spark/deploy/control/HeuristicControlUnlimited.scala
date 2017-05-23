package org.apache.spark.deploy.control

import org.apache.spark.SparkConf

/**
  * Created by Simone Ripamonti on 23/05/2017.
  */
class HeuristicControlUnlimited(conf: SparkConf) extends HeuristicControl(conf) {

  override def computeCoreStage(deadlineStage: Long = 0L, numRecord: Long = 0L, stageId: Int = 0, firstStage : Boolean = false, lastStage: Boolean = false): Double = {
    // if requested cores are greater than the available ones (coreForVM * numMaxExecutor), we fix the value to the
    // available ones
    val requestedCores = super.computeCoreStage(deadlineStage, numRecord, stageId, firstStage, lastStage)
    if (requestedCores > coreForVM * numMaxExecutor){
      coreForVM * numMaxExecutor
    } else {
      requestedCores
    }

  }
}
