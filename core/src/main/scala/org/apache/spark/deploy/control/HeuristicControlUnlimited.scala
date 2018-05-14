package org.apache.spark.deploy.control

import org.apache.spark.SparkConf

/**
  * Created by Simone Ripamonti on 23/05/2017.
  */
class HeuristicControlUnlimited(conf: SparkConf) extends HeuristicControl(conf) {


  override def computeCores(coresToBeAllocated: Double,
                            executorIndex: Int,
                            stageId: Int,
                            last: Boolean): (Double, Double, Double) = {
    // compute core to start
    val coreForExecutors = computeCoreForExecutors(coresToBeAllocated, stageId, last)

    var coreToStart = coreForExecutors(executorIndex)

    // compute max core
    val coreMax = coreForVM

    // return result
    (coreMin, coreMax, coreToStart)
  }

  override def computeCoreForExecutors(coresToBeAllocated: Double, stageId: Int, last: Boolean): IndexedSeq[Double] = {

    numExecutor = math.ceil(coresToBeAllocated.toDouble / coreForVM.toDouble).toInt

    var coresToStart = 0
    if (coresToBeAllocated == coreForVM*numMaxExecutor) {
      coresToStart = math.ceil(coresToBeAllocated.toDouble).toInt
    }
    else {
      coresToStart = math.ceil(coresToBeAllocated.toDouble / OVERSCALE).toInt
    }
    numExecutor = numMaxExecutor
    (1 to numMaxExecutor).map { x =>
        math.ceil((coresToStart / numExecutor.toDouble) / CQ) * CQ
    }

  }


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
