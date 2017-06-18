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
(conf: SparkConf, applicationId: String, executorId: String, deadline: Long,
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
  var csp: Double = 0

  val timer = new Timer()
  var oldCore = core

  def nextAllocation(): Double = {
    csp = K * (SP - (completedTasks / tasks))
    val csi = csiOld + K * (Ts.toDouble / Ti) * (SP - (completedTasks / tasks))
    cs = math.max(coreMin.toDouble, csp + csi)
    cs
  }

  def computeDesiredCore(): Double = {
    if (SP < 1.0) SP += Ts.toDouble / deadline.toDouble
    var nextCore: Double = coreMin
    if (SP >= 1.0) {
      SP = 1.0
      nextCore = coreMax
    } else {
      nextCore = nextAllocation()
    }
    nextCore
  }

  def applyNextCore(nextCore: Double) = {
    // log updates
    logInfo("SP Updated: " + SP.toString+ " ApplicationId: "+applicationId)
    logInfo("Real: " + (completedTasks / tasks).toString+ " ApplicationId: "+applicationId)
    logInfo("CoreToAllocate: " + nextCore.toString + " ApplicationId: "+applicationId)
    // match core quantum
    cs = math.ceil(nextCore / CQ) * CQ
    // store old value
    csiOld = cs - csp
    // scale executor
    if (cs != oldCore) {
      oldCore = cs
      worker.onScaleExecutor(applicationId, executorId, cs)
    }
  }
}