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

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.MemorySize
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.TextLineInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger
import org.apache.flink.streaming.examples.wordcount.util.WordCountData
import org.apache.flink.streaming.scala.examples.windowing.util.CarSource
import org.apache.flink.streaming.scala.examples.wordcount.util.CLI

import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * An example of grouped stream windowing where different eviction and trigger policies can be used.
 * A source fetches events from cars every 100 msec containing their id, their current speed (kmh),
 * overall elapsed distance (m) and a timestamp. The streaming example triggers the top speed of
 * each car every x meters elapsed for the last y seconds.
 */
object TopSpeedWindowing {

  // *************************************************************************
  // PROGRAM
  // *************************************************************************

  case class CarEvent(carId: Int, speed: Int, distance: Double, time: Long)

  val numOfCars = 2
  val evictionSec = 10
  val triggerMeters = 50d

  def main(args: Array[String]): Unit = {
    val params = CLI.fromArgs(args)

    // Create the execution environment. This is the main entrypoint
    // to building a Flink application.
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // Apache Flink’s unified approach to stream and batch processing means that a DataStream
    // application executed over bounded input will produce the same final results regardless
    // of the configured execution mode. It is important to note what final means here: a job
    // executing in STREAMING mode might produce incremental updates (think upserts in
    // a database) while in BATCH mode, it would only produce one final result at the end. The
    // final result will be the same if interpreted correctly, but getting there can be
    // different.
    //
    // The “classic” execution behavior of the DataStream API is called STREAMING execution
    // mode. Applications should use streaming execution for unbounded jobs that require
    // continuous incremental processing and are expected to stay online indefinitely.
    //
    // By enabling BATCH execution, we allow Flink to apply additional optimizations that we
    // can only do when we know that our input is bounded. For example, different
    // join/aggregation strategies can be used, in addition to a different shuffle
    // implementation that allows more efficient task scheduling and failure recovery behavior.
    //
    // By setting the runtime mode to AUTOMATIC, Flink will choose BATCH if all sources
    // are bounded and otherwise STREAMING.
    env.setRuntimeMode(params.executionMode)

    // This optional step makes the input parameters
    // available in the Flink UI.
    env.getConfig.setGlobalJobParameters(params)

    val cars = params.input match {
      case Some(input) =>
        // Create a new file source that will read files from a given set of directories.
        // Each file will be processed as plain text and split based on newlines.
        val builder = FileSource.forRecordStreamFormat(new TextLineInputFormat, input: _*)
        params.discoveryInterval.foreach {
          duration =>
            // If a discovery interval is provided, the source will
            // continuously watch the given directories for new files.
            builder.monitorContinuously(duration)
        }
        env
          .fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "file-input")
          .map(line => parseMap(line))
          .name("parse-input")
      case None =>
        env.addSource(CarSource(2)).name("in-memory-input")
    }

    val topSpeeds = cars
      .assignAscendingTimestamps(_.time)
      .keyBy(_.carId)
      .window(GlobalWindows.create)
      .evictor(TimeEvictor.of(Time.of(evictionSec * 1000, TimeUnit.MILLISECONDS)))
      .trigger(DeltaTrigger.of(
        triggerMeters,
        new DeltaFunction[CarEvent] {
          def getDelta(oldSp: CarEvent, newSp: CarEvent): Double = newSp.distance - oldSp.distance
        },
        cars.dataType.createSerializer(env.getConfig)
      ))
//      .window(Time.of(evictionSec * 1000, (car : CarEvent) => car.time))
//      .every(Delta.of[CarEvent](triggerMeters,
//          (oldSp,newSp) => newSp.distance-oldSp.distance, CarEvent(0,0,0,0)))
      .maxBy("speed")

    params.output match {
      case Some(output) =>
        // Given an output directory, Flink will write the results to a file
        // using a simple string encoding. In a production environment, this might
        // be something more structured like CSV, Avro, JSON, or Parquet.
        topSpeeds
          .sinkTo(
            FileSink
              .forRowFormat[CarEvent](output, new SimpleStringEncoder())
              .withRollingPolicy(
                DefaultRollingPolicy
                  .builder()
                  .withMaxPartSize(MemorySize.ofMebiBytes(1))
                  .withRolloverInterval(Duration.ofSeconds(10))
                  .build())
              .build())
          .name("file-sink")

      case None => topSpeeds.print().name("print-sink")
    }

    env.execute("TopSpeedWindowing")

  }

  // *************************************************************************
  // USER FUNCTIONS
  // *************************************************************************

  def parseMap(line: String): CarEvent = {
    val record = line.substring(1, line.length - 1).split(",")
    CarEvent(record(0).toInt, record(1).toInt, record(2).toDouble, record(3).toLong)
  }
}
