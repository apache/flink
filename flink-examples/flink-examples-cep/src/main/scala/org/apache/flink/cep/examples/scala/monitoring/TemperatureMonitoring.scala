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

package org.apache.flink.cep.examples.scala.monitoring

import org.apache.flink.cep.examples.scala.monitoring.events.{MonitoringEvent, TemperatureAlert, TemperatureEvent, TemperatureWarning}
import org.apache.flink.cep.examples.scala.monitoring.sources.MonitoringEventSource
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.IngestionTimeExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * CEP example monitoring program.
  * This example program generates a stream of monitoring events which are analyzed using
  * Flink's CEP library. The input event stream consists of temperature and power events
  * from a set of racks. The goal is to detect when a rack is about to overheat.
  * In order to do that, we create a CEP pattern which generates a TemperatureWarning
  * whenever it sees two consecutive temperature events in a given time interval whose temperatures
  * are higher than a given threshold value. A warning itself is not critical but if we see
  * two warning for the same rack whose temperatures are rising, we want to generate an alert.
  * This is achieved by defining another CEP pattern which analyzes the stream of generated
  * temperature warnings.
  */
object TemperatureMonitoring {

  private val TEMPERATURE_THRESHOLD = 100

  def main(args: Array[String]) {
    println("Executing temperature monitoring Scala example.")
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Use ingestion time => TimeCharacteristic == EventTime + IngestionTimeExtractor
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // Input stream of monitoring events
    val inputEventStream = env.addSource(new MonitoringEventSource())
      .assignTimestampsAndWatermarks(new IngestionTimeExtractor[MonitoringEvent])

    // Warning pattern: Two consecutive temperature events whose temperature is higher
    // than the given threshold appearing within a time interval of 10 seconds
    val warningPattern = Pattern
      .begin[MonitoringEvent]("first")
        .subtype(classOf[TemperatureEvent])
        .where(_.temperature > TEMPERATURE_THRESHOLD)
      .next("second")
        .subtype(classOf[TemperatureEvent])
        .where(_.temperature > TEMPERATURE_THRESHOLD)
      .within(Time.seconds(10))

    // Create a pattern stream from our warning pattern
    val tempPatternStream = CEP.pattern(inputEventStream.keyBy(_.rackID), warningPattern)

    // Generate temperature warnings for each matched warning pattern
    val warnings: DataStream[TemperatureWarning] = tempPatternStream.select( pattern => {
        val first = pattern("first").head.asInstanceOf[TemperatureEvent]
        val second = pattern("second").head.asInstanceOf[TemperatureEvent]
        new TemperatureWarning(first.rackID, (first.temperature + second.temperature) / 2)
      }
    )

    // Alert pattern: Two consecutive temperature warnings
    // appearing within a time interval of 20 seconds
    val alertPattern = Pattern
      .begin[TemperatureWarning]("first")
      .next("second")
      .within(Time.seconds(20))

    // Create a pattern stream from our alert pattern
    val alertPatternStream = CEP.pattern(warnings.keyBy(_.rackID), alertPattern)

    // Generate a temperature alert iff the second temperature warning's average temperature
    // is higher than first warning's temperature
    val alerts: DataStream[TemperatureAlert] = alertPatternStream.flatSelect((pattern, out) => {
      val first = pattern("first").head
      val second = pattern("second").head
      if (first.averageTemperature < second.averageTemperature) {
        out.collect(new TemperatureAlert(first.rackID, second.datetime))
      }
    })

    // Print the warning and alert events to the stdout
    println("Printing result to the stdout of task executors.")
    warnings.print()
    alerts.print()

    env.execute("CEP Temperature Monitoring Scala Example")
  }

}
