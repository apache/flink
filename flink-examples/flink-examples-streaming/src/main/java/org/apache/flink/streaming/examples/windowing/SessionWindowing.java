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
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * An example of session windowing that keys events by ID and groups and counts them in session with
 * gaps of 3 milliseconds.
 */
public class SessionWindowing {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);
        env.setParallelism(1);

        final boolean fileOutput = params.has("output");

        final List<Tuple3<String, Long, Integer>> input = new ArrayList<>();

        input.add(new Tuple3<>("a", 1L, 1));
        input.add(new Tuple3<>("b", 1L, 1));
        input.add(new Tuple3<>("b", 3L, 1));
        input.add(new Tuple3<>("b", 5L, 1));
        input.add(new Tuple3<>("c", 6L, 1));
        // We expect to detect the session "a" earlier than this point (the old
        // functionality can only detect here when the next starts)
        input.add(new Tuple3<>("a", 10L, 1));
        // We expect to detect session "b" and "c" at this point as well
        input.add(new Tuple3<>("c", 11L, 1));

        GeneratorFunction<Long, Tuple3<String, Long, Integer>> dataGenerator =
                index -> input.get(index.intValue());
        DataGeneratorSource<Tuple3<String, Long, Integer>> generatorSource =
                new DataGeneratorSource<>(
                        dataGenerator,
                        input.size(),
                        TypeInformation.of(new TypeHint<Tuple3<String, Long, Integer>>() {}));

        DataStream<Tuple3<String, Long, Integer>> source =
                env.fromSource(
                        generatorSource,
                        WatermarkStrategy.<Tuple3<String, Long, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.f1),
                        "Generated data source");

        // We create sessions for each id with max timeout of 3 time units
        DataStream<Tuple3<String, Long, Integer>> aggregated =
                source.keyBy(value -> value.f0)
                        .window(EventTimeSessionWindows.withGap(Time.milliseconds(3L)))
                        .sum(2);

        if (fileOutput) {
            aggregated
                    .sinkTo(
                            FileSink.<Tuple3<String, Long, Integer>>forRowFormat(
                                            new Path(params.get("output")),
                                            new SimpleStringEncoder<>())
                                    .withRollingPolicy(
                                            DefaultRollingPolicy.builder()
                                                    .withMaxPartSize(MemorySize.ofMebiBytes(1))
                                                    .withRolloverInterval(Duration.ofSeconds(10))
                                                    .build())
                                    .build())
                    .name("output");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            aggregated.print();
        }

        env.execute();
    }
}
