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

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.WatermarkCombiner;
import org.apache.flink.api.common.WatermarkDeclaration;
import org.apache.flink.api.common.WatermarkOutput;
import org.apache.flink.api.common.WatermarkPolicy;
import org.apache.flink.api.common.eventtime.GenericWatermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.connector.source.util.ratelimit.GuavaRateLimiter;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;
import org.apache.flink.streaming.examples.windowing.util.CarGeneratorFunction;

import org.apache.flink.util.FlinkRuntimeException;

import sun.net.www.content.text.Generic;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

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
        ss();
//        orig();
    }

    public static void orig() throws Exception {

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
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);


        SingleOutputStreamOperator<Tuple4<Integer, Integer, Double, Long>> carData;

        CarGeneratorFunction carGenerator = new CarGeneratorFunction(2);
        DataGeneratorSource<Tuple4<Integer, Integer, Double, Long>> carGeneratorSource =
                new DataGeneratorSource<>(
                        carGenerator,
                        Long.MAX_VALUE,
                        parallelismIgnored -> new GuavaRateLimiter(10),
                        TypeInformation.of(
                                new TypeHint<Tuple4<Integer, Integer, Double, Long>>() {
                                }));
        carData =
                env.fromSource(
                        carGeneratorSource,
                        WatermarkStrategy.noWatermarks(),
                        "Car data generator source");
        carData.setParallelism(1);

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
                        .evictor(TimeEvictor.of(Duration.ofSeconds(evictionSec)))
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
                                        carData.getType()
                                                .createSerializer(
                                                        env.getConfig().getSerializerConfig())))
                        .maxBy(1);

        topSpeeds.print();

        env.execute("CarTopSpeedWindowingExample");
    }


    public static void ss() throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getInstance();
        NonKeyedPartitionStream<Integer> source =
                env.fromSource(
                        DataStreamV2SourceUtils.fromData(Arrays.asList(1, 2, 3)), "testsource");
        source.process(new OneInputStreamProcessFunction<Integer, Integer>() {
                    @Override
                    public void processRecord(
                            Integer record,
                            Collector<Integer> output,
                            PartitionedContext ctx) throws Exception {
                        output.collect(record + 10);
                    }

                    @Override
                    public void onWatermark(
                            GenericWatermark watermark,
                            Collector<Integer> output,
                            NonPartitionedContext<Integer> ctx) {
                        // this will be called since watermarkPolicy is defined to POP
                        // If it watermarkPolicy would be defined with PEEK, this method will not be called
                        ctx.getWatermarkManager().emitWatermark(new CustomWatermark("Override time: " + System.currentTimeMillis()));
                    }

                    @Override
                    public WatermarkPolicy watermarkPolicy() {
                        return new WatermarkPolicy() {
                            @Override
                            public WatermarkResult useWatermark(GenericWatermark watermark) {
                                return WatermarkResult.POP;
                            }
                        };
                    }

                    @Override
                    public Set<Class<? extends WatermarkDeclaration>> watermarkDeclarations() {
                        return Collections.singleton(CustomWatermarkDeclaration.class);
                    }

                })
                .keyBy(new KeySelector<Integer, Integer>() {
                             @Override
                             public Integer getKey(Integer value) throws Exception {
                                 return value;
                             }
                         }
                ).
                process(new OneInputStreamProcessFunction<Integer, Integer>() {
                    @Override
                    public void processRecord(
                            Integer record,
                            Collector<Integer> output,
                            PartitionedContext ctx) throws Exception {
                        output.collect(record);
                    }

                    @Override
                    public WatermarkPolicy watermarkPolicy() {
                        return new WatermarkPolicy() {
                            @Override
                            public WatermarkResult useWatermark(GenericWatermark watermark) {
                                // We want to handle Watermarks explicitly, so, onWatermark will be called back
                                return WatermarkResult.POP;
                            }
                        };
                    }

                    @Override
                    public void onWatermark(
                            GenericWatermark watermark,
                            Collector<Integer> output,
                            NonPartitionedContext<Integer> ctx) {
                        // this will be called since watermarkPolicy is defined to POP
                        // If it watermarkPolicy would be defined with PEEK, this method will not be called
                        // Note that here the watermark will contain also CustomWatermark instances,
                        // since the upstream operator explicitly handles Watermarks and sends CustomWatermarks
                        ctx.getWatermarkManager().emitWatermark(watermark);
                    }

                });

        env.execute("testjob");
    }

    public static class CustomWatermark implements GenericWatermark {
        String strPayload;

        public CustomWatermark(String strPayload) {
            this.strPayload = strPayload;
        }

        public String getStrPayload() {
            return strPayload;
        }
    }

    public static class CustomWatermarkDeclaration implements WatermarkDeclaration {

        @Override
        public WatermarkSerde declaredWatermark() {
            return new WatermarkSerde() {
                @Override
                public Class<? extends GenericWatermark> watermarkClass() {
                    return CustomWatermark.class;
                }

                @Override
                public void serialize(
                        GenericWatermark genericWatermark,
                        DataOutputView target) throws IOException {
                    target.writeUTF(((CustomWatermark) genericWatermark).getStrPayload());
                }

                @Override
                public GenericWatermark deserialize(DataInputView inputView) throws IOException {
                    return new CustomWatermark(inputView.readUTF());
                }
            };
        }

        @Override
        public WatermarkCombiner watermarkCombiner() {
            return new WatermarkCombiner() {
                @Override
                public void combineWatermark(
                        GenericWatermark watermark,
                        Context context,
                        WatermarkOutput output) throws Exception {
                    if (!(watermark instanceof CustomWatermark)) {
                        throw new FlinkRuntimeException("Expected CustomWatermark, got " + watermark.getClass());
                    }
                    // custom watermark alignment logic
                    if (context.getIndexOfCurrentChannel() == 0) {
                        output.emitWatermark(watermark);
                    }
                }
            };
        }
    }
}

