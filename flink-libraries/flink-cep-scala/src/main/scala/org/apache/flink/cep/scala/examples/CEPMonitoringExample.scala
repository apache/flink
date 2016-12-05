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

package org.apache.flink.cep.scala.examples

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.examples.events.{MonitoringEvent, TemperatureAlert, TemperatureEvent, TemperatureWarning}
import org.apache.flink.cep.scala.examples.sources.MonitoringEventSource
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

object CEPMonitoringExample {

  private val TEMPERATURE_THRESHOLD = 100
  private val MAX_RACK_ID = 10
  private val PAUSE = 100
  private val TEMPERATURE_RATIO = 0.5
  private val POWER_STD = 10
  private val POWER_MEAN = 100
  private val TEMP_STD = 20
  private val TEMP_MEAN = 80

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Use ingestion time => TimeCharacteristic == EventTime + IngestionTimeExtractor
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Input stream of monitoring events
    val inputEventStream = env.addSource(new MonitoringEventSource(
      MAX_RACK_ID,
      PAUSE,
      TEMPERATURE_RATIO,
      POWER_STD,
      POWER_MEAN,
      TEMP_STD,
      TEMP_MEAN)
    ).assignTimestampsAndWatermarks(new IngestionTimeExtractor[MonitoringEvent])

    // Warning pattern: Two consecutive temperature events whose temperature is higher than the given threshold
    // appearing within a time interval of 10 seconds
    val warningPattern = Pattern
      .begin[MonitoringEvent]("first")
      .subtype(classOf[TemperatureEvent])
      .where(_.getTemperature > TEMPERATURE_THRESHOLD)
      .next("second")
      .subtype(classOf[TemperatureEvent])
      .where(_.getTemperature > TEMPERATURE_THRESHOLD)
      .within(Time.seconds(10))

    // Create a pattern stream from our warning pattern
    val tempPatternStream = CEP.pattern(inputEventStream.keyBy(_.getRackID), warningPattern)

    // Generate temperature warnings for each matched warning pattern
    val warnings = tempPatternStream.select(
      pattern => {
        val first = pattern("first").asInstanceOf[TemperatureEvent]
        val second = pattern("second").asInstanceOf[TemperatureEvent]
        new TemperatureWarning(first.getRackID, (first.getTemperature + second.getTemperature) / 2)
      }
    )

    // Alert pattern: Two consecutive temperature warnings appearing within a time interval of 20 seconds
    val alertPattern = Pattern
      .begin[TemperatureWarning]("first")
      .next("second")
      .within(Time.seconds(20))

    // Create a pattern stream from our alert pattern
    val alertPatternStream = CEP.pattern(warnings.keyBy(_.getRackID), alertPattern)

    // Generate a temperature alert only iff the second temperature warning's average temperature is higher than
    // first warning's temperature
    val alerts: DataStream[TemperatureAlert] = alertPatternStream.flatSelect((pattern, out) => {
      val first = pattern("first")
      val second = pattern("second")
      if (first.getAverageTemperature < second.getAverageTemperature) {
        out.collect(new TemperatureAlert(first.getRackID))
      }
    })

    // Print the warning and alert events to stdout
    warnings.print()
    alerts.print()
    env.execute("CEP monitoring job")
  }
}
