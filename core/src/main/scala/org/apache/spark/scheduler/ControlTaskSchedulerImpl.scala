package org.apache.spark.scheduler

import org.apache.spark.{SparkContext, TaskNotSerializableException}
import org.apache.spark.scheduler.TaskLocality.TaskLocality

import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
  * Created by Simone Ripamonti on 18/05/2017.
  */
private[spark] class ControlTaskSchedulerImpl(
                                        override val sc: SparkContext,
                                        override val maxTaskFailures: Int,
                                        isLocal: Boolean = false) extends TaskSchedulerImpl(sc, maxTaskFailures, isLocal){

  def this(sc: SparkContext) = this(sc, sc.conf.getInt("spark.task.maxFailures", 4))

  val execIdToTaskSet = new HashMap[String, Long].withDefaultValue(-1)
  val taskSetToExecId = new HashMap[Long, Set[String]].withDefaultValue(Set())

  // Number of tasks running on each executor
  private val executorIdToTaskCount = new HashMap[String, Int]

  private def resourceOfferSingleTaskSet(
                                          taskSet: TaskSetManager,
                                          maxLocality: TaskLocality,
                                          shuffledOffers: Seq[WorkerOffer],
                                          availableCpus: Array[Int],
                                          tasks: Seq[ArrayBuffer[TaskDescription]]) : Boolean = {
    var launchedTask = false
    for (i <- 0 until shuffledOffers.size) {
      val execId = shuffledOffers(i).executorId
      val host = shuffledOffers(i).host
      val stageId = taskSet.stageId
      logInfo("CPU FREE: %d, EID %s, SID, %d, ASSIGNED SID %d".format(availableCpus(i),
        execId, stageId, execIdToTaskSet(execId)))
      if (availableCpus(i) >= CPUS_PER_TASK && execIdToTaskSet(execId) == stageId) {
        try {
          for (task <- taskSet.resourceOffer(execId, host, maxLocality)) {
            tasks(i) += task
            val tid = task.taskId
            taskIdToTaskSetManager(tid) = taskSet
            taskIdToExecutorId(tid) = execId
            executorIdToTaskCount(execId) += 1
            executorsByHost(host) += execId
            availableCpus(i) -= CPUS_PER_TASK
            assert(availableCpus(i) >= 0)
            launchedTask = true
          }
        } catch {
          case e: TaskNotSerializableException =>
            logError(s"Resource offer failed, task set ${taskSet.name} was not serializable")
            // Do not offer resources for this task, but don't throw an error to allow other
            // task sets to be submitted.
            return launchedTask
        }
      }
    }
    return launchedTask
  }

  def bind(executorId: String, stageId: Int): Unit = {
    if (execIdToTaskSet(executorId) == -1) {
      logInfo("BINDING EXECUTOR ID: %s TO STAGEID %d".format(executorId, stageId))
      execIdToTaskSet(executorId) = stageId
    }
  }

  def unbind(executorId: String, stageId: Int): Unit = {
    if (execIdToTaskSet(executorId) == stageId) {
      logInfo("UNBINDING EXECUTOR ID: %s FROM SID: %d".format(executorId, stageId))
      execIdToTaskSet(executorId) = -1
    }
  }

}
