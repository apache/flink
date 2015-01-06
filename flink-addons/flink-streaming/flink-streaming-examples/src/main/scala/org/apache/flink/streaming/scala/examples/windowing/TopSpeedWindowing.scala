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


import java.util.concurrent.TimeUnit._

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.windowing.{Delta, Time}
import org.apache.flink.api.scala._
import scala.Stream._
import scala.math._
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

  case class CarEvent(carId: Int, speed: Int, distance: Double, time: Long) extends Serializable

  def main(args: Array[String]) {
    if (!parseParameters(args)) {
      return
    }

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val cars = env.fromCollection(genCarStream())
      .groupBy("carId")
      .window(Time.of(evictionSec, SECONDS))
      .every(Delta.of[CarEvent](triggerMeters,
          (oldSp,newSp) => newSp.distance-oldSp.distance, CarEvent(0,0,0,0)))
      .reduce((x, y) => if (x.speed > y.speed) x else y)

    cars print

    env.execute("TopSpeedWindowing")

  }

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

  def parseParameters(args: Array[String]): Boolean = {
    if (args.length > 0) {
      if (args.length == 3) {
        numOfCars = args(0).toInt
        evictionSec = args(1).toInt
        triggerMeters = args(2).toDouble
      }
      else {
        System.err.println("Usage: TopSpeedWindowing <numCars> <evictSec> <triggerMeters>")
        false
      }
    }
    true
  }

  var numOfCars = 2
  var evictionSec = 10
  var triggerMeters = 50d

}
