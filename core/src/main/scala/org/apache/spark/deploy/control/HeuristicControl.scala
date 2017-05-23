package org.apache.spark.deploy.control

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import spray.json._
import DefaultJsonProtocol._


import scala.collection.mutable.{HashMap}

/**
  * Created by Simone Ripamonti on 13/05/2017.
  */
class HeuristicControl(conf: SparkConf) extends HeuristicBase(conf) with Logging {

  logInfo("USING CONTROLLED CORE/DEADLINE ALLOCATION")

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
    computeDeadlineStageWeightGiven(startTime, appDeadlineJobMilliseconds, weight, stageId, firstStage)
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

  override def computeDeadlineStageWeightGiven(startTime: Long, appDeadlineJobMilliseconds: Long, weight: Double, stageId: Int, firstStage: Boolean): Long = {
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

  override def checkDeadline(appJson : JsValue): Boolean = {
    var feasibility = true
    val deadline = conf.get("spark.control.deadline").toInt
    val alpha = conf.get("spark.control.alpha").toDouble
    val numTaskApp = conf.get("spark.control.numtask").toLong
    val inputRecordApp = conf.getLong("spark.control.inputrecord", 0)
    val appDeadline = System.currentTimeMillis() + (alpha * deadline).toLong

    // APP STATE PROGRESS VAR
    var currentTime = System.currentTimeMillis()
    var totalDuration = appJson.asJsObject.fields("0").asJsObject.
      fields("totalduration").convertTo[Double]
    var stageJsonIds = appJson.asJsObject.fields.keys.toList.filter(id =>
      appJson.asJsObject.fields(id).asJsObject.fields("nominalrate").convertTo[Double] != 0.0)

    val inputRecordProfileApp = appJson.asJsObject.fields("0").asJsObject.
      fields("inputrecord").convertTo[Long]

    // MAX REQUESTED CORE FOR BETTER NUM MAX EXECUTOR
    var maxRequestedCore = 0D

    // INPUT / OUTPUT Normalized by numtask
    val inputMap: HashMap[String, Double] = new HashMap[String, Double]
    val outputMap: HashMap[String, Double] = new HashMap[String, Double]


    // FOR EACH STAGE CHECK CORE NEEDED AND UPDATE VALUES
    appJson.asJsObject.fields.keys.toList.
      sortWith((x, y) => x.toInt < y.toInt).foreach(id => {
      // STAGE JSON
      val stageJson = appJson.asJsObject.fields(id).asJsObject
      logInfo("SID " + id + " " + stageJson.prettyPrint)
      // IF GENSTAGE OUTPUT IS INPUTRECORD TO GENERATE
      if (stageJson.fields("genstage").convertTo[Boolean]) {
        outputMap(id) = inputRecordApp / numTaskApp
        val duration = stageJson.fields("duration").convertTo[Double]
        totalDuration -= duration

      } else if (!stageJson.fields("skipped").convertTo[Boolean]) {
        val numTaskProfile = stageJson.fields("numtask").convertTo[Long]
        val recordsReadProfile = stageJson.fields("recordsread").convertTo[Long] +
          stageJson.fields("shufflerecordsread").convertTo[Long]
        val recordsWriteProfile = stageJson.fields("recordswrite").convertTo[Long] +
          stageJson.fields("shufflerecordswrite").convertTo[Long]
        val parentsIds = stageJson.fields("parentsIds").convertTo[List[Int]]
        val stageId = id.toInt

        // FILTERING FACTOR
        val beta = recordsWriteProfile.toDouble / recordsReadProfile.toDouble
        logInfo("BETA " + beta.toString)
        var inputRecordProfile = parentsIds.foldLeft(0L) {
          (agg, x) => agg + appJson.asJsObject.fields(x.toString).asJsObject.fields("recordswrite").convertTo[Long] +
            appJson.asJsObject.fields(x.toString).asJsObject.fields("shufflerecordswrite").convertTo[Long]
        }
        if (inputRecordProfile == 0L) {
          inputRecordProfile = parentsIds.foldLeft(0L) {
            (agg, x) => agg + appJson.asJsObject.fields(x.toString).asJsObject.fields("recordsread").convertTo[Long] +
              appJson.asJsObject.fields(x.toString).asJsObject.fields("shufflerecordsread").convertTo[Long]
          }
        }
        if (inputRecordProfile == 0) inputRecordProfile = inputRecordProfileApp
        logInfo("INPUT RECORD PROFILE: " + inputRecordProfile.toString)
        val gamma = inputRecordProfile / recordsReadProfile.toDouble
        logInfo("GAMMA " + gamma.toString)
        var inputRecord = parentsIds.foldLeft(0.0) {
          (agg, x) => agg + outputMap(x.toString)
        }
        if (inputRecord == 0.0) {
          inputRecord = parentsIds.foldLeft(0.0) {
            (agg, x) => agg + inputMap(x.toString)
          }
        }
        if (inputRecord == 0.0) inputRecord = inputRecordApp / numTaskApp
        logInfo("INPUT RECORD: " + inputRecord.toString)
        NOMINAL_RATE_RECORD_S = stageJson.fields("nominalrate").convertTo[Double]

        // COMPUTE DEADLINE
        val duration = stageJson.fields("duration").convertTo[Double]
        val weight = (totalDuration / duration) - 1
        val deadlineStage = computeDeadlineStageWeightGiven(currentTime, deadline, weight, stageId)

        // UPDATE RECORD AND APP STATE
        if (recordsReadProfile == numTaskApp) {
          inputMap(id) = numTaskApp
        } else {
          inputMap(id) = inputRecord / gamma
        }
        if (recordsWriteProfile == 0L) {
          outputMap(id) = 0
        } else {
          outputMap(id) = (inputRecord / gamma) * beta
        }
        logInfo(inputMap.toString())
        logInfo(outputMap.toString())
        currentTime += deadlineStage
        totalDuration -= duration
        stageJsonIds = stageJsonIds.filter(x => x != id)

        if (inputRecord == (inputRecordApp / numTaskApp)) {
          inputRecord = (inputRecord / gamma) * numTaskApp
        } else {
          inputRecord = inputRecord * numTaskApp
        }
        // COMPUTE CORE AND CHECK FEASIBILITY
        val coreStage = computeCoreStage(deadlineStage, inputRecord.toLong, stageId)
        maxRequestedCore = math.max(coreStage, maxRequestedCore)
        val coreForExecutor = computeCoreForExecutors(coreStage, stageId, false)
        if (coreForExecutor == IndexedSeq(-1)) {
          numMaxExecutor = math.ceil(coreStage.toDouble /
            coreForVM.toDouble).toInt
          feasibility = false
        }
      }
    })
    // SUGGEST MAX EXECUTOR
    if (maxRequestedCore < (0.8 * coreForVM * numMaxExecutor)) {
      logInfo("TOTAL CORE >> CORE NEEDED: REDUCE MAX EXECUTOR TO " +
        math.ceil(maxRequestedCore / coreForVM))
    }
    feasibility
  }


}
