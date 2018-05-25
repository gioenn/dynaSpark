/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.worker

import java.util.{Timer, TimerTask}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
  * Created by Matteo on 21/07/2016.
  */
class ControllerExecutor
(conf: SparkConf, executorId: String, deadline: Long,
 coreMin: Double, coreMax: Double, _tasks: Int, core: Double) extends Logging {

  val K: Double = conf.get("spark.control.k").toDouble
  val Ts: Long = conf.get("spark.control.tsample").toLong
  val Ti: Double = conf.get("spark.control.ti").toDouble
  val CQ: Double = conf.get("spark.control.corequantum").toDouble

  val tasks: Double = _tasks.toDouble
  var worker: Worker = _

  var csiOld: Double = core.toDouble
  var SP: Double = 0.0
  var completedTasks: Double = 0.0
  var cs: Double = 0.0

  val timer = new Timer()
  var oldCore = core

  def function2TimerTask(f: () => Unit): TimerTask = new TimerTask {
    def run() = f()
  }

  def start(): Unit = {
    def timerTask() = {
      if (SP < 1.0) SP += Ts.toDouble / deadline.toDouble
      var nextCore: Double = coreMin
      if (SP >= 1.0) {
        SP = 1.0
        nextCore = math.min(coreMax, (tasks-completedTasks))
      } else {
        nextCore = nextAllocation()
      }
      logInfo("SP Updated: " + SP.toString)
      logInfo("Real: " + (completedTasks / tasks).toString)
      logInfo("CoreToAllocate: " + nextCore.toString)

      if (nextCore != oldCore) {
        oldCore = nextCore
        worker.onScaleExecutor("", executorId, nextCore)
      }

    }
    timer.scheduleAtFixedRate(function2TimerTask(timerTask), Ts, Ts)
  }

  def stop(): Unit = {
    timer.cancel()
  }

  def nextAllocation(statx: Int = 3): Double = {
    val csp = K * (SP - (completedTasks / tasks))
    if (statx != 3) {
      cs = coreMin
    }
    else {
      val csi = csiOld + K * (Ts.toDouble / Ti) * (SP - (completedTasks / tasks))
      cs = math.min(math.max(coreMin.toDouble, csp + csi), math.min((tasks-completedTasks), coreMax.toDouble))
    }
    cs = math.ceil(cs / CQ) * CQ
    csiOld = cs - csp
    cs
  }

}