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

package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.GuavaRateLimiter;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.examples.windowing.util.CarGeneratorFunction;
import org.apache.flink.streaming.examples.wordcount.util.CLI;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * An example of grouped stream windowing where different eviction and trigger policies can be used.
 * A source fetches events from cars containing their id, their current speed (kmh), overall elapsed
 * distance (m) and a timestamp. The streaming example triggers the top speed of each car every x
 * meters elapsed for the last y seconds.
 */
public class TopSpeedWindowing {

    // *************************************************************************
    // PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {
        final CLI params = CLI.fromArgs(args);

        // Create the execution environment. This is the main entrypoint
        // to building a Flink application.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Apache Flink’s unified approach to stream and batch processing means that a DataStream
        // application executed over bounded input will produce the same final results regardless
        // of the configured execution mode. It is important to note what final means here: a job
        // executing in STREAMING mode might produce incremental updates (think upserts in
        // a database) while a BATCH job would only produce one final result at the end. The final
        // result will be the same if interpreted correctly, but getting there can be different.
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
        // By setting the runtime mode to AUTOMATIC, Flink will choose BATCH  if all sources
        // are bounded and otherwise STREAMING.
        env.setRuntimeMode(params.getExecutionMode());

        // This optional step makes the input parameters
        // available in the Flink UI.
        env.getConfig().setGlobalJobParameters(params);

        SingleOutputStreamOperator<Tuple4<Integer, Integer, Double, Long>> carData;
        if (params.getInputs().isPresent()) {
            // Create a new file source that will read files from a given set of directories.
            // Each file will be processed as plain text and split based on newlines.
            FileSource.FileSourceBuilder<String> builder =
                    FileSource.forRecordStreamFormat(
                            new TextLineInputFormat(), params.getInputs().get());

            // If a discovery interval is provided, the source will
            // continuously watch the given directories for new files.
            params.getDiscoveryInterval().ifPresent(builder::monitorContinuously);

            carData =
                    env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "file-input")
                            .map(new ParseCarData())
                            .name("parse-input");
        } else {
            CarGeneratorFunction carGenerator = new CarGeneratorFunction(2);
            DataGeneratorSource<Tuple4<Integer, Integer, Double, Long>> carGeneratorSource =
                    new DataGeneratorSource<>(
                            carGenerator,
                            Long.MAX_VALUE,
                            parallelismIgnored -> new GuavaRateLimiter(10),
                            TypeInformation.of(
                                    new TypeHint<Tuple4<Integer, Integer, Double, Long>>() {}));
            carData =
                    env.fromSource(
                            carGeneratorSource,
                            WatermarkStrategy.noWatermarks(),
                            "Car data generator source");
            carData.setParallelism(1);
        }

        int evictionSec = 10;
        double triggerMeters = 50;
        DataStream<Tuple4<Integer, Integer, Double, Long>> topSpeeds =
                carData.assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        .<Tuple4<Integer, Integer, Double, Long>>
                                                forMonotonousTimestamps()
                                        .withTimestampAssigner((car, ts) -> car.f3))
                        .keyBy(value -> value.f0)
                        .window(GlobalWindows.create())
                        .evictor(TimeEvictor.of(Time.of(evictionSec, TimeUnit.SECONDS)))
                        .trigger(
                                DeltaTrigger.of(
                                        triggerMeters,
                                        new DeltaFunction<
                                                Tuple4<Integer, Integer, Double, Long>>() {
                                            private static final long serialVersionUID = 1L;

                                            @Override
                                            public double getDelta(
                                                    Tuple4<Integer, Integer, Double, Long>
                                                            oldDataPoint,
                                                    Tuple4<Integer, Integer, Double, Long>
                                                            newDataPoint) {
                                                return newDataPoint.f2 - oldDataPoint.f2;
                                            }
                                        },
                                        carData.getType().createSerializer(env.getConfig())))
                        .maxBy(1);

        if (params.getOutput().isPresent()) {
            // Given an output directory, Flink will write the results to a file
            // using a simple string encoding. In a production environment, this might
            // be something more structured like CSV, Avro, JSON, or Parquet.
            topSpeeds
                    .sinkTo(
                            FileSink.<Tuple4<Integer, Integer, Double, Long>>forRowFormat(
                                            params.getOutput().get(), new SimpleStringEncoder<>())
                                    .withRollingPolicy(
                                            DefaultRollingPolicy.builder()
                                                    .withMaxPartSize(MemorySize.ofMebiBytes(1))
                                                    .withRolloverInterval(Duration.ofSeconds(10))
                                                    .build())
                                    .build())
                    .name("file-sink");
        } else {
            topSpeeds.print();
        }

        env.execute("CarTopSpeedWindowingExample");
    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    private static class ParseCarData
            extends RichMapFunction<String, Tuple4<Integer, Integer, Double, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple4<Integer, Integer, Double, Long> map(String record) {
            String rawData = record.substring(1, record.length() - 1);
            String[] data = rawData.split(",");
            return new Tuple4<>(
                    Integer.valueOf(data[0]),
                    Integer.valueOf(data[1]),
                    Double.valueOf(data[2]),
                    Long.valueOf(data[3]));
        }
    }
}
