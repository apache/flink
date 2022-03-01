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
import org.apache.flink.api.scala._
import org.apache.flink.configuration.MemorySize
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.TextLineInputFormat
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.examples.wordcount.util.WordCountData
import org.apache.flink.streaming.scala.examples.wordcount.WordCount.Tokenizer
import org.apache.flink.streaming.scala.examples.wordcount.util.CLI

import java.time.Duration

/**
 * Implements a windowed version of the streaming "WordCount" program.
 *
 * The input is a plain text file with lines separated by newline characters.
 *
 * Usage:
 * {{{
 * WordCount
 * --input <path>
 * --output <path>
 * --window <n>
 * --slide <n>
 * }}}
 *
 * If no parameters are provided, the program is run with default data from
 * [[WordCountData]].
 *
 * This example shows how to:
 *
 *  - write a simple Flink Streaming program,
 *  - use tuple data types,
 *  - use basic windowing abstractions.
 *
 */
object WindowWordCount {

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

    val text = params.input match {
      case Some(input) =>
        // Create a new file source that will read files from a given set of directories.
        // Each file will be processed as plain text and split based on newlines.
        val builder = FileSource.forRecordStreamFormat(new TextLineInputFormat, input:_*)
        params.discoveryInterval.foreach { duration =>
          // If a discovery interval is provided, the source will
          // continuously watch the given directories for new files.
          builder.monitorContinuously(duration)
        }
        env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "file-input")
      case None =>
        env.fromElements(WordCountData.WORDS:_*).name("in-memory-input")
    }

    val windowSize = params.getInt("window").getOrElse(250)
    val slideSize = params.getInt("slide").getOrElse(150)

    val counts =
      // The text lines read from the source are split into words
      // using a user-defined function. The tokenizer, implemented below,
      // will output each words as a (2-tuple) containing (word, 1)
      text.flatMap(new Tokenizer)
        .name("tokenizer")
        // keyBy groups tuples based on the "_1" field, the word.
        // Using a keyBy allows performing aggregations and other
        // stateful transformations over data on a per-key basis.
        // This is similar to a GROUP BY clause in a SQL query.
        .keyBy(_._1)
        // create windows of windowSize records slided every slideSize records
        .countWindow(windowSize, slideSize)
        // For each key, we perform a simple sum of the "1" field, the count.
        // If the input data set is bounded, sum will output a final count for
        // each word. If it is unbounded, it will continuously output updates
        // each time it sees a new instance of each word in the stream.
        .sum(1)
        .name("counter")

    params.output match {
      case Some(output) =>
        // Given an output directory, Flink will write the results to a file
        // using a simple string encoding. In a production environment, this might
        // be something more structured like CSV, Avro, JSON, or Parquet.
        counts.sinkTo(FileSink.forRowFormat[(String, Int)](output, new SimpleStringEncoder())
          .withRollingPolicy(DefaultRollingPolicy.builder()
            .withMaxPartSize(MemorySize.ofMebiBytes(1))
            .withRolloverInterval(Duration.ofSeconds(10))
            .build())
          .build())
          .name("file-sink")

      case None => counts.print().name("print-sink")
    }

    // Apache Flink applications are composed lazily. Calling execute
    // submits the Job and begins processing.
    env.execute("WindowWordCount")
  }
}
