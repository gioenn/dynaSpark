package org.apache.spark.deploy.control

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.StageInfo
import spray.json.JsValue

import scala.collection.mutable.ListBuffer

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
                              last: Boolean): List[Int] = {
    numExecutor = numMaxExecutor
    var remainingTasks = totalTasksStage.toInt
    var z = numExecutor
    var taskPerExecutor = new ListBuffer[Int]()
    while (remainingTasks > 0 && z > 0) {
      val a = math.floor(remainingTasks / z).toInt
      remainingTasks -= a
      z -= 1
      taskPerExecutor += a
    }
    val taskForExecutor = scala.collection.mutable.IndexedSeq(taskPerExecutor: _*)
    var j = taskForExecutor.size - 1
    while (remainingTasks > 0 && j >= 0) {
      taskForExecutor(j) += 1
      remainingTasks -= 1
      j -= 1
      if (j < 0) j = taskForExecutor.size - 1
    }
    taskForExecutor.toList
  }

  def computeDeadlineStage(startTime: Long,
                           appDeadlineJobMilliseconds: Long,
                           totalStageRemaining: Long,
                           totalDurationRemaining: Long,
                           stageDuration: Long,
                           stageId : Int,
                           firstStage : Boolean = false): Long

  def computeDeadlineStageWeightGiven(startTime: Long,
                                      appDeadlineJobMilliseconds: Long,
                                      weight: Double,
                                      stageId: Int,
                                      firstStage: Boolean = false
                                      ): Long

  def computeCoreStage(deadlineStage: Long = 0L, numRecord: Long = 0L, stageId : Int, firstStage : Boolean = false, lastStage: Boolean = false): Double

  def computeNominalRecord(stage: StageInfo, duration: Long, recordsRead: Double): Unit = {
    // val duration = (stage.completionTime.get - stage.submissionTime.get) / 1000.0
    NOMINAL_RATE_RECORD_S = recordsRead / (duration / 1000.0)
    logInfo("DURATION STAGE ID " + stage.stageId + " : " + duration)
    logInfo("NOMINAL RECORD/S STAGE ID " + stage.stageId + " : " + NOMINAL_RATE_RECORD_S)
    conf.set("spark.control.nominalrate", NOMINAL_RATE_RECORD_S.toString)
  }

  def checkDeadline(appJson: JsValue) : Boolean
}
