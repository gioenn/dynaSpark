package org.apache.spark.deploy.control

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

import scala.collection.mutable.ListBuffer

/**
  * Created by Simone Ripamonti on 13/05/2017.
  */
class HeuristicFixed(conf: SparkConf) extends HeuristicBase(conf) with Logging {

  val stageCores: List[Double] = conf.get("spark.control.stagecores").replace("[", "").replace("]", "").split(',').toList.map(_.trim).map(_.toDouble)
  val stageDeadlines: List[Long] = conf.get("spark.control.stagedeadlines").replace("[", "").replace("]", "").split(',').toList.map(_.trim).map(_.toLong)
  val stageToCoresConf = ((0 until stageCores.length) zip stageCores).toMap
  val stageToDeadlinesConf = ((0 until stageDeadlines.length) zip stageDeadlines).toMap


  override def computeCores(coresToBeAllocated: Double,
                            executorIndex: Int,
                            stageId: Int,
                            last: Boolean): (Double, Double, Double) = {
    val core = stageToCoresConf(stageId)
    (core, core, core)
  }

  override def computeCoreForExecutors(coresToBeAllocated: Double, stageId: Int, last: Boolean): IndexedSeq[Double] = {
    (1 to numMaxExecutor).map { x => stageToCoresConf(stageId) }
  }


  override def computeTaskForExecutors(coresToBeAllocated: Double, totalTasksStage: Int, last: Boolean): List[Int] = {
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

  override def computeDeadlineStage(startTime: Long,
                                    appDeadlineJobMilliseconds: Long,
                                    totalStageRemaining: Long,
                                    totalDurationRemaining: Long,
                                    stageDuration: Long,
                                    stageId: Int,
                                    firstStage: Boolean = false): Long = {
    stageToDeadlinesConf (stageId)
  }

  def computeCoreStage(deadlineStage: Long = 0L, numRecord: Long = 0L, stageId : Int, firstStage : Boolean = false, lastStage: Boolean = false): Double = {
    stageToCoresConf(stageId)
  }
}
