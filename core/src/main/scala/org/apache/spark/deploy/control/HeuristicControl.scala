package org.apache.spark.deploy.control

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.StageInfo

import scala.collection.mutable.ListBuffer

/**
  * Created by Simone Ripamonti on 13/05/2017.
  */
class HeuristicControl(conf: SparkConf) extends HeuristicBase(conf) with Logging {
  val coreMin: Double = conf.getDouble("spark.control.coremin", 0.0)
  val coreForVM: Int = conf.getInt("spark.control.coreforvm", 8)
  val OVERSCALE: Int = conf.getInt("spark.control.overscale", 2)
  val CQ: Double = conf.getDouble("spark.control.corequantum", 0.05)
  val BETA: Double = conf.get("spark.control.beta").toDouble


  override def computeCores(coresToBeAllocated: Double,
                            executorIndex: Int,
                            stageId: Int,
                            last: Boolean): (Double, Double, Double) = {
    // compute core to start
    val coreForExecutors = computeCoreForExecutors(coresToBeAllocated, stageId, last)
    logInfo(coreForExecutors.toString())
    var coreToStart = coreForExecutors(executorIndex)

    // compute max core
    val coreMax = math.min(coreToStart * OVERSCALE, coreForVM)

    // return result
    (coreMin, coreMax, coreToStart)
  }

  override def computeCoreForExecutors(coresToBeAllocated: Double, stageId: Int, last: Boolean): IndexedSeq[Double] = {
    numExecutor = math.ceil(coresToBeAllocated.toDouble / coreForVM.toDouble).toInt
    if (numExecutor > numMaxExecutor) {
      logError("NUM EXECUTORS TOO HIGH: %d > NUM MAX EXECUTORS %d".format(
        numExecutor, numMaxExecutor
      ))
      IndexedSeq(-1)
    } else {
      var coresToStart = 0
      coresToStart = math.ceil(coresToBeAllocated.toDouble / OVERSCALE).toInt
      numExecutor = numMaxExecutor
      (1 to numMaxExecutor).map { x =>
        math.ceil((coresToStart / numExecutor.toDouble) / CQ) * CQ
      }
    }
  }

  override def computeTaskForExecutors(coresToBeAllocated: Double, totalTasksStage: Int,
                                       last: Boolean): List[Int] = {
    numExecutor = math.ceil(coresToBeAllocated.toDouble / coreForVM.toDouble).toInt
    if (numExecutor > numMaxExecutor) {
      logError("NUM EXECUTORS TOO HIGH: %d > NUM MAX EXECUTORS %d".format(
        numExecutor, numMaxExecutor
      ))
      List(-1)
    } else {
      super.computeTaskForExecutors(coresToBeAllocated, totalTasksStage, last)
    }
  }

  override def computeDeadlineStage(startTime: Long,
                                    appDeadlineJobMilliseconds: Long,
                                    totalStageRemaining: Long,
                                    totalDurationRemaining: Long,
                                    stageDuration: Long,
                                    stageId : Int,
                                    firstStage : Boolean = false): Long = {

    val weight = computeWeightStage(stageId, totalStageRemaining, totalDurationRemaining, stageDuration)

    var stageDeadline = ((appDeadlineJobMilliseconds - startTime) / (weight + 1)).toLong
    if (stageDeadline < 0) {
      if(firstStage){
        logError("DEADLINE NEGATIVE -> DEADLINE NOT SATISFIED")
      } else {
        logError("ALPHA DEADLINE NEGATIVE -> ALPHA DEADLINE NOT SATISFIED")
        stageDeadline = 1
      }
    }
    stageDeadline
  }



  override def computeCoreStage(deadlineStage: Long = 0L, numRecord: Long = 0L, stageId: Int = 0, firstStage : Boolean = false, lastStage: Boolean = false): Double = {
    logInfo("NumRecords: " + numRecord.toString +
      " DeadlineStage : " + deadlineStage.toString +
      " NominalRate: " + NOMINAL_RATE_RECORD_S.toString)

    if (firstStage){
      // old computeCoreFirstStage
      coreForVM * numMaxExecutor
    } else if (!lastStage){
      // old computeCoreStage
      if (deadlineStage > 1) {
        OVERSCALE * math.ceil((numRecord / (deadlineStage / 1000.0)) / NOMINAL_RATE_RECORD_S).toInt
      } else {
        coreForVM * numMaxExecutor
      }
    } else {
      // old fixCoreLastStage
      numExecutor = math.ceil(computeCoreStage(deadlineStage, numRecord).toDouble
        / coreForVM.toDouble).toInt
      NOMINAL_RATE_RECORD_S = NOMINAL_RATE_RECORD_S * (1 - ((numMaxExecutor - numExecutor).toDouble
        / numMaxExecutor.toDouble))
      logInfo("New Last Stage Nominal Rate: " + NOMINAL_RATE_RECORD_S.toString)
      computeCoreStage(deadlineStage, numRecord)
    }
  }


  private def computeWeightStage(stageId: Int, totalStageRemaining: Long, totalDurationRemaining: Long, stageDuration : Long): Double = synchronized {

    val w1: Double = totalStageRemaining
    val w2: Double = (totalDurationRemaining.toDouble / stageDuration) - 1.0
    val weight = (w1 * BETA) + (w2 * (1.0 - BETA))

    logInfo("STAGE ID " + stageId + " [WEIGHT] W1: " + w1 + " W2: " + w2 + " W: " + weight + " with BETA: " + BETA)

    return weight;

  }

}
