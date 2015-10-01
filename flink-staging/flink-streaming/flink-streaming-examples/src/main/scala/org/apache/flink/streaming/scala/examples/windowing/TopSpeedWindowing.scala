/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.scala.examples.windowing


import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.windowing.{Delta, Time}

import scala.Stream._
import scala.math._
import scala.language.postfixOps
import scala.util.Random

/**
 * An example of grouped stream windowing where different eviction and 
 * trigger policies can be used. A source fetches events from cars 
 * every 1 sec containing their id, their current speed (kmh),
 * overall elapsed distance (m) and a timestamp. The streaming
 * example triggers the top speed of each car every x meters elapsed 
 * for the last y seconds.
 */
object TopSpeedWindowing {

  // *************************************************************************
  // PROGRAM
  // *************************************************************************

  case class CarEvent(carId: Int, speed: Int, distance: Double, time: Long)

  val numOfCars = 2
  val evictionSec = 10
  val triggerMeters = 50d

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val cars = setCarsInput(env)

    val topSeed = cars.keyBy("carId")
      .window(Time.of(evictionSec * 1000, (car : CarEvent) => car.time))
      .every(Delta.of[CarEvent](triggerMeters,
          (oldSp,newSp) => newSp.distance-oldSp.distance, CarEvent(0,0,0,0)))
      .local    
      .maxBy("speed")
      .flatten()

    if (fileOutput) {
      topSeed.writeAsText(outputPath)
    } else {
      topSeed.print
    }

    env.execute("TopSpeedWindowing")

  }

  // *************************************************************************
  // USER FUNCTIONS
  // *************************************************************************

  def genCarStream(): Stream[CarEvent] = {

    def nextSpeed(carEvent : CarEvent) : CarEvent =
    {
      val next =
        if (Random.nextBoolean) min(100, carEvent.speed + 5) else max(0, carEvent.speed - 5)
      CarEvent(carEvent.carId, next, carEvent.distance + next/3.6d,System.currentTimeMillis)
    }
    def carStream(speeds : Stream[CarEvent]) : Stream[CarEvent] =
    {
      Thread.sleep(1000)
      speeds.append(carStream(speeds.map(nextSpeed)))
    }
    carStream(range(0, numOfCars).map(CarEvent(_,50,0,System.currentTimeMillis())))
  }

  def parseMap(line : String): (Int, Int, Double, Long) = {
    val record = line.substring(1, line.length - 1).split(",")
    (record(0).toInt, record(1).toInt, record(2).toDouble, record(3).toLong)
  }

  // *************************************************************************
  // UTIL METHODS
  // *************************************************************************

  var fileInput = false
  var fileOutput = false
  var inputPath : String = null
  var outputPath : String = null

  def parseParameters(args: Array[String]): Boolean = {
    if (args.length > 0) {
      if (args.length == 2) {
        fileInput = true
        fileOutput = true
        inputPath = args(0)
        outputPath = args(1)
        true
      } else {
        System.err.println("Usage: TopSpeedWindowing <input path> <output path>")
        false
      }
    } else {
      true
    }
  }

  private def setCarsInput(env: StreamExecutionEnvironment) : DataStream[CarEvent] = {
    if (fileInput) {
      env.readTextFile(inputPath).map(parseMap(_)).map(x => CarEvent(x._1, x._2, x._3, x._4))
    } else {
      env.fromCollection(genCarStream())
    }
  }

}
