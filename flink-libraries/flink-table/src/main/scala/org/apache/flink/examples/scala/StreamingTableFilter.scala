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

package org.apache.flink.examples.scala

import org.apache.flink.streaming.api.scala._

import org.apache.flink.api.scala.table._

import scala.Stream._
import scala.math._
import scala.language.postfixOps
import scala.util.Random

/**
 * Simple example for demonstrating the use of the Table API with Flink Streaming.
 */
object StreamingTableFilter {

  case class CarEvent(carId: Int, speed: Int, distance: Double, time: Long) extends Serializable

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val cars = genCarStream().toTable
      .filter('carId === 0)
      .select('carId, 'speed, 'distance + 1000 as 'distance, 'time % 5 as 'time)
      .toDataStream[CarEvent]

    cars.print()

    StreamExecutionEnvironment.getExecutionEnvironment.execute("TopSpeedWindowing")

  }

  def genCarStream(): DataStream[CarEvent] = {

    def nextSpeed(carEvent : CarEvent) : CarEvent =
    {
      val next =
        if (Random.nextBoolean()) min(100, carEvent.speed + 5) else max(0, carEvent.speed - 5)
      CarEvent(carEvent.carId, next, carEvent.distance + next/3.6d,System.currentTimeMillis)
    }
    def carStream(speeds : Stream[CarEvent]) : Stream[CarEvent] =
    {
      Thread.sleep(1000)
      speeds.append(carStream(speeds.map(nextSpeed)))
    }
    carStream(range(0, numOfCars).map(CarEvent(_,50,0,System.currentTimeMillis())))
  }

  def parseParameters(args: Array[String]): Boolean = {
    if (args.length > 0) {
      if (args.length == 3) {
        numOfCars = args(0).toInt
        evictionSec = args(1).toInt
        triggerMeters = args(2).toDouble
        true
      }
      else {
        System.err.println("Usage: TopSpeedWindowing <numCars> <evictSec> <triggerMeters>")
        false
      }
    }else{
      true
    }
  }

  var numOfCars = 2
  var evictionSec = 10
  var triggerMeters = 50d

}
