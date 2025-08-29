/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.common.eventtime.NoWatermarksGenerator;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.SerializableFunction;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.apache.flink.test.checkpointing.SavepointITCase.waitUntilAllTasksAreRunning;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for timestamps, watermarks, and event-time sources. */
@SuppressWarnings("serial")
public class TimestampITCase extends TestLogger {

    @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

    private static final int NUM_TASK_MANAGERS = 2;
    private static final int NUM_TASK_SLOTS = 3;
    private static final int PARALLELISM = NUM_TASK_MANAGERS * NUM_TASK_SLOTS;

    // this is used in some tests to synchronize
    static MultiShotLatch latch;

    @ClassRule
    public static final MiniClusterWithClientResource CLUSTER =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setConfiguration(getConfiguration())
                            .setNumberTaskManagers(NUM_TASK_MANAGERS)
                            .setNumberSlotsPerTaskManager(NUM_TASK_SLOTS)
                            .build());

    private static Configuration getConfiguration() {
        Configuration config = new Configuration();
        config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("12m"));
        return config;
    }

    private static final SimpleVersionedSerializer<Void> VOID_SERIALIZER =
            new SimpleVersionedSerializer<Void>() {
                @Override
                public int getVersion() {
                    return 1;
                }

                @Override
                public byte[] serialize(Void obj) {
                    return new byte[0];
                }

                @Override
                public Void deserialize(int version, byte[] serialized) {
                    return null;
                }
            };

    public static final TestSplit SPLIT = new TestSplit();

    public static final SimpleVersionedSerializer<TestSplit> SPLIT_SERIALIZER =
            new SimpleVersionedSerializer<TestSplit>() {
                @Override
                public int getVersion() {
                    return 1;
                }

                @Override
                public byte[] serialize(TestSplit split) {
                    return new byte[0];
                }

                @Override
                public TestSplit deserialize(int version, byte[] serialized) {
                    return SPLIT;
                }
            };

    @Before
    public void setupLatch() {
        // ensure that we get a fresh latch for each test
        latch = new MultiShotLatch();
    }

    /**
     * These check whether custom timestamp emission works at sources and also whether timestamps
     * arrive at operators throughout a topology.
     *
     * <p>This also checks whether watermarks keep propagating if a source closes early.
     *
     * <p>This only uses map to test the workings of watermarks in a complete, running topology. All
     * tasks and stream operators have dedicated tests that test the watermark propagation
     * behaviour.
     */
    @Test
    public void testWatermarkPropagation() throws Exception {
        final int numWatermarks = 10;

        long initialTime = 0L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(PARALLELISM);

        DataStream<Integer> source1 =
                env.fromSource(
                        timestampSource(numWatermarks, initialTime, true),
                        WatermarkStrategy.noWatermarks(),
                        "TimestampSource1");
        DataStream<Integer> source2 =
                env.fromSource(
                        timestampSource(numWatermarks / 2, initialTime, true),
                        WatermarkStrategy.noWatermarks(),
                        "TimestampSource2");

        source1.union(source2)
                .map(new IdentityMap())
                .connect(source2)
                .map(new IdentityCoMap())
                .transform("Custom Operator", BasicTypeInfo.INT_TYPE_INFO, new CustomOperator(true))
                .sinkTo(new DiscardingSink<>());

        env.execute();

        // verify that all the watermarks arrived at the final custom operator
        for (int i = 0; i < PARALLELISM; i++) {
            // we are only guaranteed to see NUM_WATERMARKS / 2 watermarks because the
            // other source stops emitting after that
            for (int j = 0; j < numWatermarks / 2; j++) {
                if (!CustomOperator.finalWatermarks[i]
                        .get(j)
                        .equals(new Watermark(initialTime + j))) {
                    System.err.println("All Watermarks: ");
                    for (int k = 0; k <= numWatermarks / 2; k++) {
                        System.err.println(CustomOperator.finalWatermarks[i].get(k));
                    }

                    fail("Wrong watermark.");
                }
            }

            assertEquals(
                    Watermark.MAX_WATERMARK,
                    CustomOperator.finalWatermarks[i].get(
                            CustomOperator.finalWatermarks[i].size() - 1));
        }
    }

    @Test
    public void testSelfUnionWatermarkPropagation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<Integer> dataStream1 = env.fromData(1, 2, 3);

        dataStream1
                .union(dataStream1)
                .transform(
                        "Custom Operator", BasicTypeInfo.INT_TYPE_INFO, new CustomOperator(false))
                .sinkTo(new DiscardingSink<>());
        env.execute();

        assertEquals(
                Watermark.MAX_WATERMARK,
                CustomOperator.finalWatermarks[0].get(
                        CustomOperator.finalWatermarks[0].size() - 1));
    }

    @Test
    public void testWatermarkPropagationNoFinalWatermarkOnStop() throws Exception {

        // for this test to work, we need to be sure that no other jobs are being executed
        final ClusterClient<?> clusterClient = CLUSTER.getClusterClient();
        while (!getRunningJobs(clusterClient).isEmpty()) {
            Thread.sleep(100);
        }

        final int numWatermarks = 10;

        long initialTime = 0L;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(PARALLELISM);

        DataStream<Integer> source1 =
                env.fromSource(
                        timestampSource(numWatermarks, initialTime, false),
                        WatermarkStrategy.noWatermarks(),
                        "InfiniteTimestampSource1");
        DataStream<Integer> source2 =
                env.fromSource(
                        timestampSource(numWatermarks / 2, initialTime, false),
                        WatermarkStrategy.noWatermarks(),
                        "InfiniteTimestampSource2");

        source1.union(source2)
                .map(new IdentityMap())
                .connect(source2)
                .map(new IdentityCoMap())
                .transform("Custom Operator", BasicTypeInfo.INT_TYPE_INFO, new CustomOperator(true))
                .sinkTo(new DiscardingSink<Integer>());

        Thread t =
                new Thread("stopper") {
                    @Override
                    public void run() {
                        try {
                            // try until we get the running jobs
                            List<JobID> running = getRunningJobs(clusterClient);
                            while (running.isEmpty()) {
                                Thread.sleep(10);
                                running = getRunningJobs(clusterClient);
                            }

                            JobID id = running.get(0);

                            waitUntilAllTasksAreRunning(CLUSTER.getRestClusterClient(), id);

                            // send stop until the job is stopped
                            final String savepointDirName = tmpFolder.newFolder().getAbsolutePath();
                            do {
                                try {
                                    clusterClient
                                            .stopWithSavepoint(
                                                    id,
                                                    false,
                                                    savepointDirName,
                                                    SavepointFormatType.CANONICAL)
                                            .get();
                                } catch (Exception e) {
                                    boolean ignoreException =
                                            ExceptionUtils.findThrowable(
                                                            e, CheckpointException.class)
                                                    .map(
                                                            CheckpointException
                                                                    ::getCheckpointFailureReason)
                                                    .map(
                                                            reason ->
                                                                    reason
                                                                            == CheckpointFailureReason
                                                                                    .NOT_ALL_REQUIRED_TASKS_RUNNING)
                                                    .orElse(false);
                                    if (!ignoreException) {
                                        throw e;
                                    }
                                }
                                Thread.sleep(10);
                            } while (!getRunningJobs(clusterClient).isEmpty());
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }
                    }
                };
        t.start();

        env.execute();

        // verify that all the watermarks arrived at the final custom operator
        for (List<Watermark> subtaskWatermarks : CustomOperator.finalWatermarks) {

            // we are only guaranteed to see NUM_WATERMARKS / 2 watermarks because the
            // other source stops emitting after that
            for (int j = 0; j < subtaskWatermarks.size(); j++) {
                if (subtaskWatermarks.get(j).getTimestamp() != initialTime + j) {
                    System.err.println("All Watermarks: ");
                    for (int k = 0; k <= numWatermarks / 2; k++) {
                        System.err.println(subtaskWatermarks.get(k));
                    }

                    fail("Wrong watermark.");
                }
            }

            // if there are watermarks, the final one must not be the MAX watermark
            if (subtaskWatermarks.size() > 0) {
                assertNotEquals(
                        Watermark.MAX_WATERMARK,
                        subtaskWatermarks.get(subtaskWatermarks.size() - 1));
            }
        }
        t.join();
    }

    /**
     * These check whether timestamps are properly assigned at the sources and handled in network
     * transmission and between chained operators when timestamps are enabled.
     */
    @Test
    public void testTimestampHandling() throws Exception {
        final int numElements = 10;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(PARALLELISM);

        DataGeneratorSource<Integer> generator =
                new DataGeneratorSource<>(
                        (GeneratorFunction<Long, Integer>) Long::intValue, numElements, Types.INT);

        WatermarkStrategy<Integer> timestamped =
                WatermarkStrategy.<Integer>forMonotonousTimestamps()
                        .withTimestampAssigner((ctx) -> (element, ts) -> element);

        DataStream<Integer> source1 = env.fromSource(generator, timestamped, "GenSource1");
        DataStream<Integer> source2 = env.fromSource(generator, timestamped, "GenSource2");

        source1.map(new IdentityMap())
                .connect(source2)
                .map(new IdentityCoMap())
                .transform(
                        "Custom Operator",
                        BasicTypeInfo.INT_TYPE_INFO,
                        new TimestampCheckingOperator())
                .sinkTo(new DiscardingSink<Integer>());

        env.execute();
    }

    /**
     * Verifies that we don't have timestamps when the source doesn't emit them with the records.
     */
    @Test
    public void testDisabledTimestamps() throws Exception {
        final int numElements = 10;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(PARALLELISM);

        DataGeneratorSource<Integer> generator =
                new DataGeneratorSource<>(
                        (GeneratorFunction<Long, Integer>) Long::intValue, numElements, Types.INT);

        DataStream<Integer> source1 =
                env.fromSource(generator, WatermarkStrategy.noWatermarks(), "GenSource1")
                        .transform(
                                "Erase Timestamps",
                                BasicTypeInfo.INT_TYPE_INFO,
                                new TimestampErasingOperator());

        DataStream<Integer> source2 =
                env.fromSource(generator, WatermarkStrategy.noWatermarks(), "GenSource2")
                        .transform(
                                "Erase Timestamps",
                                BasicTypeInfo.INT_TYPE_INFO,
                                new TimestampErasingOperator());

        source1.map(new IdentityMap())
                .connect(source2)
                .map(new IdentityCoMap())
                .transform(
                        "Custom Operator",
                        BasicTypeInfo.INT_TYPE_INFO,
                        new DisabledTimestampCheckingOperator())
                .sinkTo(new DiscardingSink<Integer>());

        env.execute();
    }

    /**
     * This tests whether timestamps are properly extracted in the timestamp extractor and whether
     * watermarks are also correctly forwarded from this with the auto watermark interval.
     */
    @Test
    public void testTimestampExtractorWithAutoInterval() throws Exception {
        final int numElements = 10;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(10);
        env.setParallelism(1);

        DataGeneratorSource<Integer> generator =
                new DataGeneratorSource<>(
                        (GeneratorFunction<Long, Integer>) idx -> (int) (idx + 1),
                        numElements,
                        RateLimiterStrategy.perSecond(5),
                        Types.INT);
        DataStream<Integer> source1 =
                env.fromSource(generator, WatermarkStrategy.noWatermarks(), "AutoIntervalSource");

        DataStream<Integer> extractOp =
                source1.assignTimestampsAndWatermarks(
                        AscendingRecordTimestampsWatermarkStrategy.create(Long::valueOf));

        extractOp
                .transform("Watermark Check", BasicTypeInfo.INT_TYPE_INFO, new CustomOperator(true))
                .transform(
                        "Timestamp Check",
                        BasicTypeInfo.INT_TYPE_INFO,
                        new TimestampCheckingOperator());

        // verify that extractor picks up source parallelism
        Assert.assertEquals(
                extractOp.getTransformation().getParallelism(),
                source1.getTransformation().getParallelism());

        env.execute();

        // verify that we get NUM_ELEMENTS watermarks
        for (int j = 0; j < numElements; j++) {
            if (!CustomOperator.finalWatermarks[0].get(j).equals(new Watermark(j))) {
                long wm = CustomOperator.finalWatermarks[0].get(j).getTimestamp();
                Assert.fail(
                        "Wrong watermark. Expected: "
                                + j
                                + " Found: "
                                + wm
                                + " All: "
                                + CustomOperator.finalWatermarks[0]);
            }
        }

        // the input is finite, so it should have a MAX Watermark
        assertEquals(
                Watermark.MAX_WATERMARK,
                CustomOperator.finalWatermarks[0].get(
                        CustomOperator.finalWatermarks[0].size() - 1));
    }

    /**
     * This tests whether timestamps are properly extracted in the timestamp extractor and whether
     * watermark are correctly forwarded from the custom watermark emit function.
     */
    @Test
    public void testTimestampExtractorWithCustomWatermarkEmit() throws Exception {
        final int numElements = 10;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(10);
        env.setParallelism(1);

        DataGeneratorSource<Integer> source =
                new DataGeneratorSource<>(
                        (GeneratorFunction<Long, Integer>) idx -> (int) (idx + 1),
                        numElements,
                        Types.INT);

        DataStream<Integer> source1 =
                env.fromSource(
                        source, WatermarkStrategy.noWatermarks(), "CustomWatermarkEmitSource");

        source1.assignTimestampsAndWatermarks(
                        new WatermarkStrategy<Integer>() {
                            @Override
                            public TimestampAssigner<Integer> createTimestampAssigner(
                                    TimestampAssignerSupplier.Context context) {
                                return (element, recordTimestamp) -> element;
                            }

                            @Override
                            public WatermarkGenerator<Integer> createWatermarkGenerator(
                                    WatermarkGeneratorSupplier.Context context) {
                                return new WatermarkGenerator<Integer>() {
                                    @Override
                                    public void onEvent(
                                            Integer event,
                                            long eventTimestamp,
                                            WatermarkOutput output) {
                                        output.emitWatermark(
                                                new org.apache.flink.api.common.eventtime.Watermark(
                                                        eventTimestamp - 1));
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {}
                                };
                            }
                        })
                .transform("Watermark Check", BasicTypeInfo.INT_TYPE_INFO, new CustomOperator(true))
                .transform(
                        "Timestamp Check",
                        BasicTypeInfo.INT_TYPE_INFO,
                        new TimestampCheckingOperator());

        env.execute();

        // verify that we get NUM_ELEMENTS watermarks
        for (int j = 0; j < numElements; j++) {
            if (!CustomOperator.finalWatermarks[0].get(j).equals(new Watermark(j))) {
                Assert.fail("Wrong watermark.");
            }
        }

        // the input is finite, so it should have a MAX Watermark
        assertEquals(
                Watermark.MAX_WATERMARK,
                CustomOperator.finalWatermarks[0].get(
                        CustomOperator.finalWatermarks[0].size() - 1));
    }

    /** This test verifies that the timestamp extractor does not emit decreasing watermarks. */
    @Test
    public void testTimestampExtractorWithDecreasingCustomWatermarkEmit() throws Exception {
        final int numElements = 10;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(1);
        env.setParallelism(1);

        // Creates a source that emits a sequence of interleaved values: i, i - 1 for i in
        // [1..numElements].
        // For example: 1, 0, 2, 1, 3, 2, ...
        // This simulates out-of-order timestamps to test whether the watermark generator
        // correctly suppresses decreasing watermarks.
        // The source emits 2 × numElements records, with a rate limiter to allow watermark
        // propagation.
        DataStream<Integer> source1 =
                env.fromSource(
                        new DataGeneratorSource<>(
                                (GeneratorFunction<Long, Integer>)
                                        idx ->
                                                (idx % 2 == 0)
                                                        ? (int) (idx / 2 + 1)
                                                        : (int) (idx / 2),
                                numElements * 2,
                                RateLimiterStrategy.perSecond(5),
                                Types.INT),
                        WatermarkStrategy.noWatermarks(),
                        "DecreasingWatermarkSource");

        source1.assignTimestampsAndWatermarks(
                        new WatermarkStrategy<Integer>() {
                            @Override
                            public TimestampAssigner<Integer> createTimestampAssigner(
                                    TimestampAssignerSupplier.Context context) {
                                return (element, recordTimestamp) -> element;
                            }

                            @Override
                            public WatermarkGenerator<Integer> createWatermarkGenerator(
                                    WatermarkGeneratorSupplier.Context context) {
                                return new WatermarkGenerator<Integer>() {
                                    @Override
                                    public void onEvent(
                                            Integer event,
                                            long eventTimestamp,
                                            WatermarkOutput output) {
                                        output.emitWatermark(
                                                new org.apache.flink.api.common.eventtime.Watermark(
                                                        eventTimestamp - 1));
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {}
                                };
                            }
                        })
                .transform("Watermark Check", BasicTypeInfo.INT_TYPE_INFO, new CustomOperator(true))
                .transform(
                        "Timestamp Check",
                        BasicTypeInfo.INT_TYPE_INFO,
                        new TimestampCheckingOperator());

        env.execute();

        // verify that we get NUM_ELEMENTS watermarks
        for (int j = 0; j < numElements; j++) {
            if (!CustomOperator.finalWatermarks[0].get(j).equals(new Watermark(j))) {
                Assert.fail("Wrong watermark.");
            }
        }
        // the input is finite, so it should have a MAX Watermark
        assertEquals(
                Watermark.MAX_WATERMARK,
                CustomOperator.finalWatermarks[0].get(
                        CustomOperator.finalWatermarks[0].size() - 1));
    }

    /** This test verifies that the timestamp extractor forwards Long.MAX_VALUE watermarks. */
    @Test
    public void testTimestampExtractorWithLongMaxWatermarkFromSource() throws Exception {
        final int numElements = 10;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(1);
        env.setParallelism(2);

        DataStream<Integer> source1 =
                env.fromSource(
                        new LongMaxWatermarkSource(numElements),
                        WatermarkStrategy.noWatermarks(),
                        "LongMaxWatermarkSource");

        source1.assignTimestampsAndWatermarks(
                        (WatermarkStrategy<Integer>) context -> new NoWatermarksGenerator<>())
                .transform(
                        "Watermark Check", BasicTypeInfo.INT_TYPE_INFO, new CustomOperator(true));

        env.execute();

        Assert.assertTrue(CustomOperator.finalWatermarks[0].size() == 1);
        Assert.assertTrue(
                CustomOperator.finalWatermarks[0].get(0).getTimestamp() == Long.MAX_VALUE);
    }

    /**
     * This test verifies that the timestamp extractor forwards Long.MAX_VALUE watermarks.
     *
     * <p>Same test as before, but using a different timestamp extractor.
     */
    @Test
    public void testTimestampExtractorWithLongMaxWatermarkFromSource2() throws Exception {
        final int numElements = 10;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(10);
        env.setParallelism(2);

        DataStream<Integer> source1 =
                env.fromSource(
                        new LongMaxWatermarkSource(numElements),
                        WatermarkStrategy.noWatermarks(),
                        "LongMaxWatermarkSource2");

        source1.assignTimestampsAndWatermarks(
                        (WatermarkStrategy<Integer>) context -> new NoWatermarksGenerator<>())
                .transform(
                        "Watermark Check", BasicTypeInfo.INT_TYPE_INFO, new CustomOperator(true));

        env.execute();

        Assert.assertTrue(CustomOperator.finalWatermarks[0].size() == 1);
        Assert.assertTrue(
                CustomOperator.finalWatermarks[0].get(0).getTimestamp() == Long.MAX_VALUE);
    }

    @Test
    public void testErrorOnEventTimeOverProcessingTime() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStream<Tuple2<String, Integer>> source1 =
                env.fromData(new Tuple2<>("a", 1), new Tuple2<>("b", 2));

        source1.keyBy(x -> x.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .reduce(
                        new ReduceFunction<Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> reduce(
                                    Tuple2<String, Integer> value1,
                                    Tuple2<String, Integer> value2) {
                                return value1;
                            }
                        })
                .print();

        try {
            env.execute();
            fail("this should fail with an exception");
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testErrorOnEventTimeWithoutTimestamps() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStream<Tuple2<String, Integer>> source1 =
                env.fromData(new Tuple2<>("a", 1), new Tuple2<>("b", 2));

        source1.keyBy(x -> x.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .reduce(
                        new ReduceFunction<Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> reduce(
                                    Tuple2<String, Integer> value1,
                                    Tuple2<String, Integer> value2) {
                                return value1;
                            }
                        })
                .print();

        try {
            env.execute();
            fail("this should fail with an exception");
        } catch (Exception e) {
            // expected
        }
    }

    // ------------------------------------------------------------------------
    //  Custom Operators and Functions
    // ------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private static class CustomOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        List<Watermark> watermarks;
        public static List<Watermark>[] finalWatermarks = new List[PARALLELISM];
        private final boolean timestampsEnabled;

        public CustomOperator(boolean timestampsEnabled) {
            this.timestampsEnabled = timestampsEnabled;
        }

        @Override
        public void processElement(StreamRecord<Integer> element) throws Exception {
            if (timestampsEnabled) {
                if (element.getTimestamp() != element.getValue()) {
                    Assert.fail("Timestamps are not properly handled.");
                }
            }
            output.collect(element);
        }

        @Override
        public void processWatermark(Watermark mark) throws Exception {
            super.processWatermark(mark);

            for (Watermark previousMark : watermarks) {
                assertTrue(previousMark.getTimestamp() < mark.getTimestamp());
            }
            watermarks.add(mark);
            latch.trigger();
            output.emitWatermark(mark);
        }

        @Override
        public void open() throws Exception {
            super.open();
            watermarks = new ArrayList<>();
        }

        @Override
        public void close() throws Exception {
            super.close();
            finalWatermarks[getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()] = watermarks;
        }
    }

    private static class TimestampCheckingOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        public TimestampCheckingOperator() {}

        @Override
        public void processElement(StreamRecord<Integer> element) throws Exception {
            if (element.getTimestamp() != element.getValue()) {
                Assert.fail("Timestamps are not properly handled.");
            }
            output.collect(element);
        }
    }

    private static class DisabledTimestampCheckingOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        @Override
        public void processElement(StreamRecord<Integer> element) throws Exception {
            if (element.hasTimestamp()) {
                Assert.fail("Timestamps are not properly handled.");
            }
            output.collect(element);
        }
    }

    /** A test operator that explicitly erases the timestamp of incoming records. */
    private static class TimestampErasingOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        @Override
        public void processElement(StreamRecord<Integer> element) throws Exception {
            element.eraseTimestamp();
            output.collect(element);
        }
    }

    private static class IdentityCoMap implements CoMapFunction<Integer, Integer, Integer> {
        @Override
        public Integer map1(Integer value) throws Exception {
            return value;
        }

        @Override
        public Integer map2(Integer value) throws Exception {
            return value;
        }
    }

    private static class IdentityMap implements MapFunction<Integer, Integer> {
        @Override
        public Integer map(Integer value) throws Exception {
            return value;
        }
    }

    private static List<JobID> getRunningJobs(ClusterClient<?> client) throws Exception {
        Collection<JobStatusMessage> statusMessages = client.listJobs().get();
        return statusMessages.stream()
                .filter(status -> status.getJobState() == JobStatus.RUNNING)
                .map(JobStatusMessage::getJobId)
                .collect(Collectors.toList());
    }

    public static class AscendingRecordTimestampsWatermarkStrategy<T>
            implements WatermarkStrategy<T> {
        private final SerializableFunction<T, Long> timestampExtractor;

        public AscendingRecordTimestampsWatermarkStrategy(
                SerializableFunction<T, Long> timestampExtractor) {
            this.timestampExtractor = timestampExtractor;
        }

        public static <T> AscendingRecordTimestampsWatermarkStrategy<T> create(
                SerializableFunction<T, Long> timestampExtractor) {
            return new AscendingRecordTimestampsWatermarkStrategy<>(timestampExtractor);
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(
                WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(
                TimestampAssignerSupplier.Context context) {
            return (event, ignore) -> timestampExtractor.apply(event);
        }
    }

    private static class LongMaxWatermarkSource implements Source<Integer, TestSplit, Void> {

        private final int numElements;

        public LongMaxWatermarkSource(int numElements) {
            this.numElements = numElements;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public SourceReader<Integer, TestSplit> createReader(SourceReaderContext context) {
            return new LongMaxWatermarkSourceReader(numElements);
        }

        @Override
        public SplitEnumerator<TestSplit, Void> createEnumerator(
                SplitEnumeratorContext<TestSplit> context) {
            return new TestSplitEnumerator(context);
        }

        @Override
        public SplitEnumerator<TestSplit, Void> restoreEnumerator(
                SplitEnumeratorContext<TestSplit> context, Void checkpoint) {
            return createEnumerator(context);
        }

        @Override
        public SimpleVersionedSerializer<TestSplit> getSplitSerializer() {
            return SPLIT_SERIALIZER;
        }

        @Override
        public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
            return VOID_SERIALIZER;
        }
    }

    public static Source<Integer, TestSplit, Void> timestampSource(
            int numElements, long initialTs, boolean bounded) {
        return new Source<>() {
            @Override
            public Boundedness getBoundedness() {
                return bounded ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
            }

            @Override
            public SourceReader<Integer, TestSplit> createReader(SourceReaderContext ctx) {
                return new TimestampSourceReader(initialTs, numElements, bounded);
            }

            @Override
            public SplitEnumerator<TestSplit, Void> createEnumerator(
                    SplitEnumeratorContext<TestSplit> ctx) {
                return new TestSplitEnumerator(ctx);
            }

            @Override
            public SplitEnumerator<TestSplit, Void> restoreEnumerator(
                    SplitEnumeratorContext<TestSplit> ctx, Void checkpoint) {
                return createEnumerator(ctx);
            }

            @Override
            public SimpleVersionedSerializer<TestSplit> getSplitSerializer() {
                return SPLIT_SERIALIZER;
            }

            @Override
            public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
                return VOID_SERIALIZER;
            }
        };
    }

    public static class TimestampSourceReader implements SourceReader<Integer, TestSplit> {
        private final long initialTime;
        private final int numWatermarks;
        private final boolean bounded;
        private int currentIndex = 0;
        private volatile boolean running = true;

        public TimestampSourceReader(long initialTime, int numWatermarks, boolean bounded) {
            this.initialTime = initialTime;
            this.numWatermarks = numWatermarks;
            this.bounded = bounded;
        }

        @Override
        public void start() {}

        @Override
        public InputStatus pollNext(ReaderOutput<Integer> output) throws Exception {
            if (bounded && currentIndex >= numWatermarks) {
                return InputStatus.END_OF_INPUT;
            }
            if (!bounded && currentIndex >= numWatermarks) {
                Thread.sleep(20);
                return InputStatus.NOTHING_AVAILABLE;
            }

            long ts = initialTime + currentIndex;
            output.collect(currentIndex, ts);
            output.emitWatermark(new org.apache.flink.api.common.eventtime.Watermark(ts));
            currentIndex++;
            return InputStatus.MORE_AVAILABLE;
        }

        @Override
        public List<TestSplit> snapshotState(long checkpointId) {
            return Collections.emptyList();
        }

        @Override
        public void addSplits(List<TestSplit> splits) {}

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public CompletableFuture<Void> isAvailable() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() throws Exception {
            running = false;
        }
    }

    private static class LongMaxWatermarkSourceReader implements SourceReader<Integer, TestSplit> {

        private final int numElements;
        private int currentIndex = 1;
        private boolean finished = false;
        private boolean emittedSecond = false;
        private boolean emittedFinalWatermarks = false;

        public LongMaxWatermarkSourceReader(int numElements) {
            this.numElements = numElements;
        }

        @Override
        public void start() {}

        @Override
        public InputStatus pollNext(ReaderOutput<Integer> output) throws Exception {
            if (finished) {
                return InputStatus.END_OF_INPUT;
            }

            if (currentIndex <= numElements) {
                if (!emittedSecond) {
                    // First emit index with timestamp index
                    output.collect(currentIndex, currentIndex);
                    emittedSecond = true;
                    return InputStatus.MORE_AVAILABLE;
                } else {
                    // Then emit index-1 with timestamp index-1
                    output.collect(currentIndex - 1, currentIndex - 1);

                    // Emit watermark with timestamp index-2
                    if (currentIndex >= 2) {
                        output.emitWatermark(
                                new org.apache.flink.api.common.eventtime.Watermark(
                                        currentIndex - 2));
                    }

                    currentIndex++;
                    emittedSecond = false;
                    return InputStatus.MORE_AVAILABLE;
                }
            } else if (!emittedFinalWatermarks) {
                // Emit final Long.MAX_VALUE watermarks twice
                output.emitWatermark(
                        new org.apache.flink.api.common.eventtime.Watermark(Long.MAX_VALUE));
                output.emitWatermark(
                        new org.apache.flink.api.common.eventtime.Watermark(Long.MAX_VALUE));
                emittedFinalWatermarks = true;
                finished = true;
                return InputStatus.END_OF_INPUT;
            } else {
                finished = true;
                return InputStatus.END_OF_INPUT;
            }
        }

        @Override
        public List<TestSplit> snapshotState(long checkpointId) {
            return Collections.emptyList();
        }

        @Override
        public void addSplits(List<TestSplit> splits) {}

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public CompletableFuture<Void> isAvailable() {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() throws Exception {}
    }

    private static class TestSplit implements SourceSplit {
        @Override
        public String splitId() {
            return "single-split";
        }
    }

    private static class TestSplitEnumerator implements SplitEnumerator<TestSplit, Void> {
        private final SplitEnumeratorContext<TestSplit> context;
        private boolean splitAssigned = false;

        public TestSplitEnumerator(SplitEnumeratorContext<TestSplit> context) {
            this.context = context;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, String requesterHostname) {
            if (!splitAssigned) {
                context.assignSplit(SPLIT, subtaskId);
                splitAssigned = true;
            }
        }

        @Override
        public void addSplitsBack(List<TestSplit> splits, int subtaskId) {}

        @Override
        public void addReader(int subtaskId) {}

        @Override
        public Void snapshotState(long checkpointId) {
            return null;
        }

        @Override
        public void close() {}
    }
}
