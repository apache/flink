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

import org.apache.flink.api.common.GenericWatermarkPolicy;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.WatermarkCombiner;
import org.apache.flink.api.common.eventtime.GenericWatermark;
import org.apache.flink.api.common.eventtime.IngestionTimeAssigner;
import org.apache.flink.api.common.eventtime.NoWatermarksGenerator;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public static void ss() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(DeploymentOptions.TARGET, "local");
//        configuration.set(DeploymentOptions.ATTACHED, true);

        ExecutionEnvironment env = ExecutionEnvironment.getInstance();
        NonKeyedPartitionStream<Integer> source =
                env.fromSource(
                        DataStreamV2SourceUtils.fromData(Arrays.asList(1, 2, 3)), "test-source");
        source.process(new OneInputStreamProcessFunction<Integer, Integer>() {
            @Override
            public void processRecord(
                    Integer record,
                    Collector<Integer> output,
                    PartitionedContext ctx) throws Exception {
                System.out.println(record + " AAA");
                output.collect(record + 10);
            }

            @Override
            public GenericWatermarkPolicy watermarkPolicy() {
                return new GenericWatermarkPolicy() {
                    @Override
                    public WatermarkResult useWatermark(GenericWatermark watermark) {
                        return WatermarkResult.POP;
                    }

                    @Override
                    public WatermarkCombiner watermarkCombiner() {
                        return new WatermarkCombiner() {
                            @Override
                            public GenericWatermark combineWatermark(
                                    GenericWatermark watermark,
                                    Context context) throws Exception {
                                return watermark;
                            }
                        };
                    }
                };
            }

            @Override
            public void onWatermark(
                    GenericWatermark watermark,
                    Collector<Integer> output,
                    NonPartitionedContext<Integer> ctx) {
                System.out.println(watermark);
                ctx.getWatermarkManager().emitWatermark(watermark);
            }
        }).keyBy(new KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer value) throws Exception {
                System.out.println(value + " KEYY");
                return value;
            }
        }).process(new OneInputStreamProcessFunction<Integer, Integer>() {
            @Override
            public void processRecord(
                    Integer record,
                    Collector<Integer> output,
                    PartitionedContext ctx) throws Exception {
                System.out.println(record + " BBB");
                output.collect(record);

            }

            @Override
            public GenericWatermarkPolicy watermarkPolicy() {
                return new GenericWatermarkPolicy() {
                    @Override
                    public WatermarkResult useWatermark(GenericWatermark watermark) {
                        return WatermarkResult.POP;
                    }

                    @Override
                    public WatermarkCombiner watermarkCombiner() {
                        return new WatermarkCombiner() {
                            @Override
                            public GenericWatermark combineWatermark(
                                    GenericWatermark watermark,
                                    Context context) throws Exception {
                                return watermark;
                            }
                        };
                    }
                };
            }

            @Override
            public void onWatermark(
                    GenericWatermark watermark,
                    Collector<Integer> output,
                    NonPartitionedContext<Integer> ctx) {
                System.out.println(watermark);
                ctx.getWatermarkManager().emitWatermark(watermark);
            }

        });
        env.execute("asda");
    }

    public static void dd() {

    }

    public static void main(String[] args) throws Exception {
//            ss();

//        final CLI params = CLI.fromArgs(args);
//
//        // Create the execution environment. This is the main entrypoint
//        // to building a Flink application.
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // Apache Flink’s unified approach to stream and batch processing means that a DataStream
//        // application executed over bounded input will produce the same final results regardless
//        // of the configured execution mode. It is important to note what final means here: a job
//        // executing in STREAMING mode might produce incremental updates (think upserts in
//        // a database) while a BATCH job would only produce one final result at the end. The final
//        // result will be the same if interpreted correctly, but getting there can be different.
//        //
//        // The “classic” execution behavior of the DataStream API is called STREAMING execution
//        // mode. Applications should use streaming execution for unbounded jobs that require
//        // continuous incremental processing and are expected to stay online indefinitely.
//        //
//        // By enabling BATCH execution, we allow Flink to apply additional optimizations that we
//        // can only do when we know that our input is bounded. For example, different
//        // join/aggregation strategies can be used, in addition to a different shuffle
//        // implementation that allows more efficient task scheduling and failure recovery behavior.
//        //
//        // By setting the runtime mode to AUTOMATIC, Flink will choose BATCH  if all sources
//        // are bounded and otherwise STREAMING.
//        env.setRuntimeMode(params.getExecutionMode());
//
//        // This optional step makes the input parameters
//        // available in the Flink UI.
//        env.getConfig().setGlobalJobParameters(params);
//
//        SingleOutputStreamOperator<Tuple4<Integer, Integer, Double, Long>> carData;
//        if (params.getInputs().isPresent()) {
//            // Create a new file source that will read files from a given set of directories.
//            // Each file will be processed as plain text and split based on newlines.
//            FileSource.FileSourceBuilder<String> builder =
//                    FileSource.forRecordStreamFormat(
//                            new TextLineInputFormat(), params.getInputs().get());
//
//            // If a discovery interval is provided, the source will
//            // continuously watch the given directories for new files.
//            params.getDiscoveryInterval().ifPresent(builder::monitorContinuously);
//
//            carData =
//                    env.fromSource(builder.build(), WatermarkStrategy.noWatermarks(), "file-input")
//                            .map(new ParseCarData())
//                            .name("parse-input");
//        } else {
//            CarGeneratorFunction carGenerator = new CarGeneratorFunction(2);
//            DataGeneratorSource<Tuple4<Integer, Integer, Double, Long>> carGeneratorSource =
//                    new DataGeneratorSource<>(
//                            carGenerator,
//                            Long.MAX_VALUE,
//                            parallelismIgnored -> new GuavaRateLimiter(10),
//                            TypeInformation.of(
//                                    new TypeHint<Tuple4<Integer, Integer, Double, Long>>() {}));
//            carData =
//                    env.fromSource(
//                            carGeneratorSource,
//                            WatermarkStrategy
//                                    .<Tuple4<Integer, Integer, Double, Long>>
//                                            forMonotonousTimestamps()
//                                    .withTimestampAssigner((car, ts) -> car.f3),
//                            "Car data generator source");
//            carData.setParallelism(1);
//        }
//
//        int evictionSec = 10;
//        double triggerMeters = 50;
//        DataStream<Tuple4<Integer, Integer, Double, Long>> topSpeeds =
//                carData.keyBy(value -> value.f0)
//                        .window(GlobalWindows.create())
//                        .evictor(TimeEvictor.of(Duration.ofSeconds(evictionSec)))
//                        .trigger(
//                                DeltaTrigger.of(
//                                        triggerMeters,
//                                        new DeltaFunction<
//                                                Tuple4<Integer, Integer, Double, Long>>() {
//                                            private static final long serialVersionUID = 1L;
//
//                                            @Override
//                                            public double getDelta(
//                                                    Tuple4<Integer, Integer, Double, Long>
//                                                            oldDataPoint,
//                                                    Tuple4<Integer, Integer, Double, Long>
//                                                            newDataPoint) {
//                                                return newDataPoint.f2 - oldDataPoint.f2;
//                                            }
//                                        },
//                                        carData.getType()
//                                                .createSerializer(
//                                                        env.getConfig().getSerializerConfig())))
//                        .maxBy(1);
//
//        if (params.getOutput().isPresent()) {
//            // Given an output directory, Flink will write the results to a file
//            // using a simple string encoding. In a production environment, this might
//            // be something more structured like CSV, Avro, JSON, or Parquet.
//            topSpeeds
//                    .sinkTo(
//                            FileSink.<Tuple4<Integer, Integer, Double, Long>>forRowFormat(
//                                            params.getOutput().get(), new SimpleStringEncoder<>())
//                                    .withRollingPolicy(
//                                            DefaultRollingPolicy.builder()
//                                                    .withMaxPartSize(MemorySize.ofMebiBytes(1))
//                                                    .withRolloverInterval(Duration.ofSeconds(10))
//                                                    .build())
//                                    .build())
//                    .name("file-sink");
//        } else {
//            topSpeeds.print();
//        }
//
//        env.execute("CarTopSpeedWindowingExample");
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

        private static ParameterTool setupParams() {
            Map<String, String> properties = new HashMap<>();
            properties.put("security.delegation.token.provider.hadoopfs.enabled", "false");
            properties.put("security.delegation.token.provider.hbase.enabled", "false");
            return ParameterTool.fromMap(properties);
        }

        public static void sss() throws Exception {
            ParameterTool paramUtils = setupParams();
            Configuration config = new Configuration(paramUtils.getConfiguration());
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                    config);
            env.setRuntimeMode(RuntimeExecutionMode.BATCH);
            DataStream<Domain> positionData = domainStream(env);
            positionData.keyBy(Domain::getA1)
                    .process(new KeyedProcessFunction<String, Domain, Domain>() {
                        private transient MapState<String, Domain> processedInputs;

                        @Override
                        public void open(Configuration configuration) {
                            MapStateDescriptor<String, Domain> mapStateDescriptor = new MapStateDescriptor<>("domain2-input",
                                    TypeInformation.of(String.class),
                                    TypeInformation.of(Domain.class));
                            processedInputs = getRuntimeContext().getMapState(mapStateDescriptor);
                        }

                        @Override
                        public void processElement(
                                Domain value,
                                KeyedProcessFunction<String, Domain, Domain>.Context context,
                                org.apache.flink.util.Collector<Domain> out) throws Exception {
                            processedInputs.put(value.getUniqueId(), value);
                            context.timerService().registerEventTimeTimer(Long.MAX_VALUE);
                        }

                        @Override
                        public void onTimer(
                                long timestamp,
                                OnTimerContext ctx,
                                org.apache.flink.util.Collector<Domain> collector) throws Exception {
                            processedInputs
                                    .iterator()
                                    .forEachRemaining(entry -> collector.collect(entry.getValue()));
                            processedInputs.clear();
                        }
                    }).process(new ProcessFunction<Domain, Void>() {
                        @Override
                        public void processElement(
                                Domain value,
                                ProcessFunction<Domain, Void>.Context ctx,
                                org.apache.flink.util.Collector<Void> out) throws Exception {

                        }

                    });
            env.execute("FileReadJob");

        }

        public static DataStream<Domain> domainStream(StreamExecutionEnvironment env) {        /* Not assigning watermarks as program is being run in batch mode and watermarks are irrelevant to batch mode */
            return env.fromCollection(getDataCollection())
                    .assignTimestampsAndWatermarks(getNoWatermarkStrategy())
                    .returns(TypeInformation.of(Domain.class))
                    .name("test-domain-source")
                    .uid("test-domain-source");
        }

        private static List<Domain> getDataCollection() {
            List<Domain> data = new ArrayList<>();
            data.add(new Domain("A11", "123-Z-1"));
            data.add(new Domain("A11", "456-A-2"));
            data.add(new Domain("A11", "456-B-2"));
            data.add(new Domain("A21", "673-9Z-09"));
            data.add(new Domain("A21", "843-09-21"));
            return data;
        }

        private static WatermarkStrategy<Domain> getNoWatermarkStrategy() {
            return WatermarkStrategy.<Domain>forGenerator((ctx) -> new NoWatermarksGenerator<>())
                    .withTimestampAssigner((ctx) -> new IngestionTimeAssigner<>());
        }

        private static WatermarkStrategy<Domain> getMonotonous() {
            return WatermarkStrategy.<Domain>forMonotonousTimestamps()
                    .withTimestampAssigner((ctx) -> new IngestionTimeAssigner<>());
        }

        private static class Domain {
            private String a1;
            private String uniqueId;

            public Domain() {
            }

            public Domain(String a1, String uniqueId) {
                this.a1 = a1;
                this.uniqueId = uniqueId;
            }

            public String getA1() {
                return a1;
            }

            public void setA1(String a1) {
                this.a1 = a1;
            }

            public String getUniqueId() {
                return uniqueId;
            }
        }

}
