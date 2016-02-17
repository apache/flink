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


import java.util.concurrent.TimeUnit

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger

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

    val params = ParameterTool.fromArgs(args)
    println("Usage: TopSpeedWindowing --input <path> --output <path>")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val cars =
      if (params.has("input")) {
        env.readTextFile(params.get("input"))
          .map(parseMap(_))
          .map(x => CarEvent(x._1, x._2, x._3, x._4))
      } else {
        println("Executing TopSpeedWindowing example with default inputs data set.")
        println("Use --input to specify file input.")
        env.fromCollection(genCarStream())
      }

    val topSeed = cars
      .assignAscendingTimestamps( _.time )
      .keyBy("carId")
      .window(GlobalWindows.create)
      .evictor(TimeEvictor.of(Time.of(evictionSec * 1000, TimeUnit.MILLISECONDS)))
      .trigger(DeltaTrigger.of(triggerMeters, new DeltaFunction[CarEvent] {
        def getDelta(oldSp: CarEvent, newSp: CarEvent): Double = newSp.distance - oldSp.distance
      }, cars.getType().createSerializer(env.getConfig)))
//      .window(Time.of(evictionSec * 1000, (car : CarEvent) => car.time))
//      .every(Delta.of[CarEvent](triggerMeters,
//          (oldSp,newSp) => newSp.distance-oldSp.distance, CarEvent(0,0,0,0)))
      .maxBy("speed")

    if (params.has("output")) {
      topSeed.writeAsText(params.get("output"))
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      topSeed.print()
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

}
