package org.apache.spark.deploy.worker

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.scalatest.Matchers

import scala.collection.mutable

/**
  * Created by Simone Ripamonti on 03/10/2017.
  */
class ControllerPollonTest extends SparkFunSuite with Matchers {

  val conf = new SparkConf(loadDefaults = false)
  conf.set("spark.control.k", "5")
  conf.set("spark.control.tsample", "1000")
  conf.set("spark.control.ti", "1200")
  conf.set("spark.control.corequantum", "0.05")

  test("test ControllerPollonProportional"){
    val pollon: ControllerPollonAbstract = new ControllerPollonProportional(16, 1000,1)
    val ce1: ControllerExecutor = new ControllerExecutor(conf, "1", "1", 3000, 0, 16, 1000, 5, 250000, 1000)
    val ce2: ControllerExecutor = new ControllerExecutor(conf, "2", "2", 4000, 0, 16, 1000, 15, 500000, 2000)
    val nc1 = ce1.nextAllocation()
    val nc2 = ce2.nextAllocation()
    val desiredCores: mutable.HashMap[(String, String), Double] = mutable.HashMap(("1","1")->nc1, ("2","2")->nc2)
    pollon.registerExecutor("1", "1", ce1)
    pollon.registerExecutor("2", "2", ce2)
    val correctedCores = pollon.correctCores(desiredCores)
    println(correctedCores)
    correctedCores("1","1") shouldBe 4
    correctedCores("2","2") shouldBe 12
  }

  test("test ControllerPollonPureEDF"){
    val pollon: ControllerPollonAbstract = new ControllerPollonPureEDF(16, 1000, 1)
    val ce1: ControllerExecutor = new ControllerExecutor(conf, "1", "1", 3000, 0, 16, 1000, 10, 250000, 1000)
    val ce2: ControllerExecutor = new ControllerExecutor(conf, "2", "2", 4000, 0, 16, 1000, 20, 500000, 2000)
    val nc1 = ce1.nextAllocation()
    val nc2 = ce2.nextAllocation()
    val desiredCores: mutable.HashMap[(String, String), Double] = mutable.HashMap(("1","1")->nc1, ("2","2")->nc2)
    pollon.registerExecutor("1", "1", ce1)
    pollon.registerExecutor("2", "2", ce2)
    val correctedCores = pollon.correctCores(desiredCores)
    println(correctedCores)
    correctedCores("1","1") shouldBe 10
    correctedCores("2","2") shouldBe 6
  }

  test("test ControllerPollonProportionalEDF"){
    val time = System.currentTimeMillis()
    val pollon: ControllerPollonAbstract = new ControllerPollonProportionalEDF(16, 1000, 1)
    val ce1: ControllerExecutor = new ControllerExecutor(conf, "1", "1", 3000, 0, 16, 1000, 16, time+250000, 1000)
    val ce2: ControllerExecutor = new ControllerExecutor(conf, "2", "2", 4000, 0, 16, 1000, 16, time-250000, 2000)
    val nc1 = ce1.nextAllocation()
    val nc2 = ce2.nextAllocation()
    val desiredCores: mutable.HashMap[(String, String), Double] = mutable.HashMap(("1","1")->nc1, ("2","2")->nc2)
    pollon.registerExecutor("1", "1", ce1)
    pollon.registerExecutor("2", "2", ce2)
    val correctedCores = pollon.correctCores(desiredCores)
    println(correctedCores)
    assert(correctedCores("1","1") <= correctedCores ("2","2"))
  }

  test("test ControllerPollonSpeed"){
    val pollon: ControllerPollonAbstract = new ControllerPollonSpeed(16, 1000, 1000, 1)
    val ce1: ControllerExecutor = new ControllerExecutor(conf, "1", "1", 3000, 0, 16, 1000, 5, 250000, 1000)
    val ce2: ControllerExecutor = new ControllerExecutor(conf, "2", "2", 4000, 0, 16, 1000, 15, 500000, 3000)
    val nc1 = ce1.nextAllocation()
    val nc2 = ce2.nextAllocation()
    val desiredCores: mutable.HashMap[(String, String), Double] = mutable.HashMap(("1","1")->nc1, ("2","2")->nc2)
    pollon.registerExecutor("1", "1", ce1)
    pollon.registerExecutor("2", "2", ce2)
    val correctedCores = pollon.correctCores(desiredCores)
    println(correctedCores)
    correctedCores("1","1") shouldBe 5
    correctedCores("2","2") shouldBe 11
  }

  test("test ControllerPollonMixedSpeedEDF speed"){
    val pollon: ControllerPollonAbstract = new ControllerPollonMixedSpeedEDF(16, 1000, 1000, 1)
    val time = System.currentTimeMillis()
    val ce1: ControllerExecutor = new ControllerExecutor(conf, "1", "1", 3000, 0, 16, 1000, 5, time+250000, 1000)
    val ce2: ControllerExecutor = new ControllerExecutor(conf, "2", "2", 4000, 0, 16, 1000, 15, time+250000, 3000)
    val nc1 = ce1.nextAllocation()
    val nc2 = ce2.nextAllocation()
    val desiredCores: mutable.HashMap[(String, String), Double] = mutable.HashMap(("1","1")->nc1, ("2","2")->nc2)
    pollon.registerExecutor("1", "1", ce1)
    pollon.registerExecutor("2", "2", ce2)
    val correctedCores = pollon.correctCores(desiredCores)
    println(correctedCores)
    // cores should be the same as previous ControllerPollonSpeed test, since "EDF" is not relevant
    correctedCores("1","1") shouldBe 5
    correctedCores("2","2") shouldBe 11
  }

  test("test ControllerPollonMixedSpeedEDF edf"){
    val pollon: ControllerPollonAbstract = new ControllerPollonMixedSpeedEDF(16, 1000, 1000, 1)
    val time = System.currentTimeMillis()
    val ce1: ControllerExecutor = new ControllerExecutor(conf, "1", "1", 3000, 0, 16, 1000, 16, time+250000, 1000)
    val ce2: ControllerExecutor = new ControllerExecutor(conf, "2", "2", 4000, 0, 16, 1000, 16, time-250000, 1000)
    val nc1 = ce1.nextAllocation()
    val nc2 = ce2.nextAllocation()
    val desiredCores: mutable.HashMap[(String, String), Double] = mutable.HashMap(("1","1")->nc1, ("2","2")->nc2)
    pollon.registerExecutor("1", "1", ce1)
    pollon.registerExecutor("2", "2", ce2)
    val correctedCores = pollon.correctCores(desiredCores)
    println(correctedCores)
    // cores should be the same as previous ControllerPollonProportionalEDF test, since "speed" is not relevant
    assert(correctedCores("1","1")<=correctedCores("2","2"))
  }

  test("test ControllerPollonMixedSpeedEDF"){
    val pollon: ControllerPollonAbstract = new ControllerPollonMixedSpeedEDF(16, 1000, 1000, 1)
    val time = System.currentTimeMillis()
    val ce1: ControllerExecutor = new ControllerExecutor(conf, "1", "1", 3000, 0, 16, 1000, 15, time+250000, 1500)
    val ce2: ControllerExecutor = new ControllerExecutor(conf, "2", "2", 4000, 0, 16, 1000, 15, time+260000, 1000)
    val nc1 = ce1.nextAllocation()
    val nc2 = ce2.nextAllocation()
    val desiredCores: mutable.HashMap[(String, String), Double] = mutable.HashMap(("1","1")->nc1, ("2","2")->nc2)
    pollon.registerExecutor("1", "1", ce1)
    pollon.registerExecutor("2", "2", ce2)
    val correctedCores = pollon.correctCores(desiredCores)
    println(correctedCores)
    // cores should be the same as previous ControllerPollonProportionalEDF test, since "speed" is not relevant
    assert(correctedCores("1","1")+correctedCores("2","2")<=16.01)
    assert(correctedCores("1","1")+correctedCores("2","2")>=15.5)
  }

}
