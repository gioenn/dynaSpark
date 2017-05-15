package org.apache.spark.deploy.control

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.StageInfo

/**
  * Created by Simone Ripamonti on 13/05/2017.
  */
abstract class HeuristicBase(conf: SparkConf) extends Logging{

  var NOMINAL_RATE_RECORD_S: Double = conf.getDouble("spark.control.nominalrate", 1000.0)
  var numMaxExecutor: Int = conf.getInt("spark.control.maxexecutor", 4)
  var numExecutor = 0


  def computeCores(coresToBeAllocated: Double,
                   executorIndex: Int,
                   stageId : Int,
                   last: Boolean) : (Double, Double, Double)

  def computeCoreForExecutors(coresToBeAllocated: Double, stageId: Int, last: Boolean): IndexedSeq[Double]

  def computeTaskForExecutors(coresToBeAllocated: Double,
                              totalTasksStage: Int,
                              last: Boolean): List[Int]

  def computeDeadlineStage(startTime: Long,
                           appDeadlineJobMilliseconds: Long,
                           totalStageRemaining: Long,
                           totalDurationRemaining: Long,
                           stageDuration: Long,
                           stageId : Int,
                           firstStage : Boolean = false): Long

  def computeCoreStage(deadlineStage: Long = 0L, numRecord: Long = 0L, stageId : Int, firstStage : Boolean = false, lastStage: Boolean = false): Double

  def computeNominalRecord(stage: StageInfo, duration: Long, recordsRead: Double): Unit = {
    // val duration = (stage.completionTime.get - stage.submissionTime.get) / 1000.0
    NOMINAL_RATE_RECORD_S = recordsRead / (duration / 1000.0)
    logInfo("DURATION STAGE ID " + stage.stageId + " : " + duration)
    logInfo("NOMINAL RECORD/S STAGE ID " + stage.stageId + " : " + NOMINAL_RATE_RECORD_S)
    conf.set("spark.control.nominalrate", NOMINAL_RATE_RECORD_S.toString)
  }
}
