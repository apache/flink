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

package org.apache.flink.streaming.connectors.kinesis;

import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.model.SentinelSequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardMetadata;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisShardIdGenerator;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestRuntimeContext;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestSourceContext;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestUtils;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OperatorSnapshotUtil;

import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for checking whether {@link FlinkKinesisConsumer} can restore from snapshots that were done
 * using an older {@code FlinkKinesisConsumer}.
 *
 * <p>For regenerating the binary snapshot files run {@link #writeSnapshot()} on the corresponding
 * Flink release-* branch.
 */
public class FlinkKinesisConsumerMigrationTest {

    /**
     * TODO change this to the corresponding savepoint version to be written (e.g. {@link
     * FlinkVersion#v1_3} for 1.3) TODO and remove all @Ignore annotations on the writeSnapshot()
     * method to generate savepoints TODO Note: You should generate the savepoint based on the
     * release branch instead of the master.
     */
    private final FlinkVersion flinkGenerateSavepointVersion = null;

    private static final String TEST_STREAM_NAME = "fakeStream1";
    private static final SequenceNumber TEST_SEQUENCE_NUMBER = new SequenceNumber("987654321");
    private static final String TEST_SHARD_ID = KinesisShardIdGenerator.generateFromShardOrder(0);

    private static final HashMap<StreamShardMetadata, SequenceNumber> TEST_STATE = new HashMap<>();

    static {
        StreamShardMetadata shardMetadata = new StreamShardMetadata();
        shardMetadata.setStreamName(TEST_STREAM_NAME);
        shardMetadata.setShardId(TEST_SHARD_ID);

        TEST_STATE.put(shardMetadata, TEST_SEQUENCE_NUMBER);
    }

    private static Stream<FlinkVersion> flinkVersionProvider() {
        return Stream.of(
                FlinkVersion.v1_3,
                FlinkVersion.v1_4,
                FlinkVersion.v1_7,
                FlinkVersion.v1_8,
                FlinkVersion.v1_9,
                FlinkVersion.v1_10,
                FlinkVersion.v1_11,
                FlinkVersion.v1_12,
                FlinkVersion.v1_13,
                FlinkVersion.v1_14,
                FlinkVersion.v1_15);
    }

    /** Manually run this to write binary snapshot data. */
    @Ignore
    @Test
    public void writeSnapshot() throws Exception {
        writeSnapshot(
                "src/test/resources/kinesis-consumer-migration-test-flink"
                        + flinkGenerateSavepointVersion
                        + "-snapshot",
                TEST_STATE);

        // write empty state snapshot
        writeSnapshot(
                "src/test/resources/kinesis-consumer-migration-test-flink"
                        + flinkGenerateSavepointVersion
                        + "-empty-snapshot",
                new HashMap<>());
    }

    @ParameterizedTest
    @MethodSource("flinkVersionProvider")
    public void testRestoreWithEmptyState(FlinkVersion flinkVersion) throws Exception {
        final List<StreamShardHandle> initialDiscoveryShards = new ArrayList<>(TEST_STATE.size());
        for (StreamShardMetadata shardMetadata : TEST_STATE.keySet()) {
            Shard shard = new Shard();
            shard.setShardId(shardMetadata.getShardId());

            SequenceNumberRange sequenceNumberRange = new SequenceNumberRange();
            sequenceNumberRange.withStartingSequenceNumber("1");
            shard.setSequenceNumberRange(sequenceNumberRange);

            initialDiscoveryShards.add(new StreamShardHandle(shardMetadata.getStreamName(), shard));
        }

        final TestFetcher<String> fetcher =
                new TestFetcher<>(
                        Collections.singletonList(TEST_STREAM_NAME),
                        new TestSourceContext<>(),
                        new TestRuntimeContext(true, 1, 0),
                        TestUtils.getStandardProperties(),
                        new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
                        null,
                        initialDiscoveryShards);

        final DummyFlinkKinesisConsumer<String> consumerFunction =
                new DummyFlinkKinesisConsumer<>(
                        fetcher,
                        new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()));

        StreamSource<String, DummyFlinkKinesisConsumer<String>> consumerOperator =
                new StreamSource<>(consumerFunction);

        final AbstractStreamOperatorTestHarness<String> testHarness =
                new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

        testHarness.setup();
        testHarness.initializeState(
                OperatorSnapshotUtil.getResourceFilename(
                        "kinesis-consumer-migration-test-flink"
                                + flinkVersion
                                + "-empty-snapshot"));
        testHarness.open();

        consumerFunction.run(new TestSourceContext<>());

        // assert that no state was restored
        assertThat(consumerFunction.getRestoredState()).isEmpty();

        // although the restore state is empty, the fetcher should still have been registered the
        // initial discovered shard;
        // furthermore, the discovered shard should be considered a newly created shard while the
        // job wasn't running,
        // and therefore should be consumed from the earliest sequence number
        KinesisStreamShardState restoredShardState = fetcher.getSubscribedShardsState().get(0);
        assertThat(restoredShardState.getStreamShardHandle().getStreamName())
                .isEqualTo(TEST_STREAM_NAME);
        assertThat(restoredShardState.getStreamShardHandle().getShard().getShardId())
                .isEqualTo(TEST_SHARD_ID);
        assertThat(restoredShardState.getStreamShardHandle().isClosed()).isFalse();
        assertThat(restoredShardState.getLastProcessedSequenceNum())
                .isEqualTo(SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.get());

        consumerOperator.close();
        consumerOperator.cancel();
    }

    @ParameterizedTest
    @MethodSource("flinkVersionProvider")
    public void testRestore(FlinkVersion flinkVersion) throws Exception {
        final List<StreamShardHandle> initialDiscoveryShards = new ArrayList<>(TEST_STATE.size());
        for (StreamShardMetadata shardMetadata : TEST_STATE.keySet()) {
            Shard shard = new Shard();
            shard.setShardId(shardMetadata.getShardId());

            SequenceNumberRange sequenceNumberRange = new SequenceNumberRange();
            sequenceNumberRange.withStartingSequenceNumber("1");
            shard.setSequenceNumberRange(sequenceNumberRange);

            initialDiscoveryShards.add(new StreamShardHandle(shardMetadata.getStreamName(), shard));
        }

        final TestFetcher<String> fetcher =
                new TestFetcher<>(
                        Collections.singletonList(TEST_STREAM_NAME),
                        new TestSourceContext<>(),
                        new TestRuntimeContext(true, 1, 0),
                        TestUtils.getStandardProperties(),
                        new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
                        null,
                        initialDiscoveryShards);

        final DummyFlinkKinesisConsumer<String> consumerFunction =
                new DummyFlinkKinesisConsumer<>(
                        fetcher,
                        new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()));

        StreamSource<String, DummyFlinkKinesisConsumer<String>> consumerOperator =
                new StreamSource<>(consumerFunction);

        final AbstractStreamOperatorTestHarness<String> testHarness =
                new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

        testHarness.setup();
        testHarness.initializeState(
                OperatorSnapshotUtil.getResourceFilename(
                        "kinesis-consumer-migration-test-flink" + flinkVersion + "-snapshot"));
        testHarness.open();

        consumerFunction.run(new TestSourceContext<>());

        // assert that state is correctly restored
        assertThat(consumerFunction.getRestoredState()).isNotNull();
        assertThat(consumerFunction.getRestoredState()).hasSize(1);
        assertThat(removeEquivalenceWrappers(consumerFunction.getRestoredState()))
                .isEqualTo(TEST_STATE);
        assertThat(fetcher.getSubscribedShardsState()).hasSize(1);
        assertThat(fetcher.getSubscribedShardsState().get(0).getLastProcessedSequenceNum())
                .isEqualTo(TEST_SEQUENCE_NUMBER);

        KinesisStreamShardState restoredShardState = fetcher.getSubscribedShardsState().get(0);
        assertThat(restoredShardState.getStreamShardHandle().getStreamName())
                .isEqualTo(TEST_STREAM_NAME);
        assertThat(restoredShardState.getStreamShardHandle().getShard().getShardId())
                .isEqualTo(TEST_SHARD_ID);
        assertThat(restoredShardState.getStreamShardHandle().isClosed()).isFalse();
        assertThat(restoredShardState.getLastProcessedSequenceNum())
                .isEqualTo(TEST_SEQUENCE_NUMBER);

        consumerOperator.close();
        consumerOperator.cancel();
    }

    @ParameterizedTest
    @MethodSource("flinkVersionProvider")
    public void testRestoreWithReshardedStream(FlinkVersion flinkVersion) throws Exception {
        final List<StreamShardHandle> initialDiscoveryShards = new ArrayList<>(TEST_STATE.size());
        for (StreamShardMetadata shardMetadata : TEST_STATE.keySet()) {
            // setup the closed shard
            Shard closedShard = new Shard();
            closedShard.setShardId(shardMetadata.getShardId());

            SequenceNumberRange closedSequenceNumberRange = new SequenceNumberRange();
            closedSequenceNumberRange.withStartingSequenceNumber("1");
            closedSequenceNumberRange.withEndingSequenceNumber(
                    "1087654321"); // this represents a closed shard
            closedShard.setSequenceNumberRange(closedSequenceNumberRange);

            initialDiscoveryShards.add(
                    new StreamShardHandle(shardMetadata.getStreamName(), closedShard));

            // setup the new shards
            Shard newSplitShard1 = new Shard();
            newSplitShard1.setShardId(KinesisShardIdGenerator.generateFromShardOrder(1));

            SequenceNumberRange newSequenceNumberRange1 = new SequenceNumberRange();
            newSequenceNumberRange1.withStartingSequenceNumber("1087654322");
            newSplitShard1.setSequenceNumberRange(newSequenceNumberRange1);

            newSplitShard1.setParentShardId(TEST_SHARD_ID);

            Shard newSplitShard2 = new Shard();
            newSplitShard2.setShardId(KinesisShardIdGenerator.generateFromShardOrder(2));

            SequenceNumberRange newSequenceNumberRange2 = new SequenceNumberRange();
            newSequenceNumberRange2.withStartingSequenceNumber("2087654322");
            newSplitShard2.setSequenceNumberRange(newSequenceNumberRange2);

            newSplitShard2.setParentShardId(TEST_SHARD_ID);

            initialDiscoveryShards.add(
                    new StreamShardHandle(shardMetadata.getStreamName(), newSplitShard1));
            initialDiscoveryShards.add(
                    new StreamShardHandle(shardMetadata.getStreamName(), newSplitShard2));
        }

        final TestFetcher<String> fetcher =
                new TestFetcher<>(
                        Collections.singletonList(TEST_STREAM_NAME),
                        new TestSourceContext<>(),
                        new TestRuntimeContext(true, 1, 0),
                        TestUtils.getStandardProperties(),
                        new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
                        null,
                        initialDiscoveryShards);

        final DummyFlinkKinesisConsumer<String> consumerFunction =
                new DummyFlinkKinesisConsumer<>(
                        fetcher,
                        new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()));

        StreamSource<String, DummyFlinkKinesisConsumer<String>> consumerOperator =
                new StreamSource<>(consumerFunction);

        final AbstractStreamOperatorTestHarness<String> testHarness =
                new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

        testHarness.setup();
        testHarness.initializeState(
                OperatorSnapshotUtil.getResourceFilename(
                        "kinesis-consumer-migration-test-flink" + flinkVersion + "-snapshot"));
        testHarness.open();

        consumerFunction.run(new TestSourceContext<>());

        // assert that state is correctly restored
        assertThat(consumerFunction.getRestoredState()).isNotNull();
        assertThat(consumerFunction.getRestoredState()).hasSize(1);
        assertThat(removeEquivalenceWrappers(consumerFunction.getRestoredState()))
                .isEqualTo(TEST_STATE);

        // assert that the fetcher is registered with all shards, including new shards
        assertThat(fetcher.getSubscribedShardsState()).hasSize(3);

        KinesisStreamShardState restoredClosedShardState =
                fetcher.getSubscribedShardsState().get(0);
        assertThat(restoredClosedShardState.getStreamShardHandle().getStreamName())
                .isEqualTo(TEST_STREAM_NAME);
        assertThat(restoredClosedShardState.getStreamShardHandle().getShard().getShardId())
                .isEqualTo(TEST_SHARD_ID);
        assertThat(restoredClosedShardState.getStreamShardHandle().isClosed()).isTrue();
        assertThat(restoredClosedShardState.getLastProcessedSequenceNum())
                .isEqualTo(TEST_SEQUENCE_NUMBER);

        KinesisStreamShardState restoredNewSplitShard1 = fetcher.getSubscribedShardsState().get(1);
        assertThat(restoredNewSplitShard1.getStreamShardHandle().getStreamName())
                .isEqualTo(TEST_STREAM_NAME);
        assertThat(restoredNewSplitShard1.getStreamShardHandle().getShard().getShardId())
                .isEqualTo(KinesisShardIdGenerator.generateFromShardOrder(1));
        assertThat(restoredNewSplitShard1.getStreamShardHandle().isClosed()).isFalse();
        // new shards should be consumed from the beginning
        assertThat(restoredNewSplitShard1.getLastProcessedSequenceNum())
                .isEqualTo(SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.get());

        KinesisStreamShardState restoredNewSplitShard2 = fetcher.getSubscribedShardsState().get(2);
        assertThat(restoredNewSplitShard2.getStreamShardHandle().getStreamName())
                .isEqualTo(TEST_STREAM_NAME);
        assertThat(restoredNewSplitShard2.getStreamShardHandle().getShard().getShardId())
                .isEqualTo(KinesisShardIdGenerator.generateFromShardOrder(2));
        assertThat(restoredNewSplitShard2.getStreamShardHandle().isClosed()).isFalse();
        // new shards should be consumed from the beginning
        assertThat(restoredNewSplitShard2.getLastProcessedSequenceNum())
                .isEqualTo(SentinelSequenceNumber.SENTINEL_EARLIEST_SEQUENCE_NUM.get());

        consumerOperator.close();
        consumerOperator.cancel();
    }

    // ------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private void writeSnapshot(String path, HashMap<StreamShardMetadata, SequenceNumber> state)
            throws Exception {
        final List<StreamShardHandle> initialDiscoveryShards = new ArrayList<>(state.size());
        for (StreamShardMetadata shardMetadata : state.keySet()) {
            Shard shard = new Shard();
            shard.setShardId(shardMetadata.getShardId());

            SequenceNumberRange sequenceNumberRange = new SequenceNumberRange();
            sequenceNumberRange.withStartingSequenceNumber("1");
            shard.setSequenceNumberRange(sequenceNumberRange);

            initialDiscoveryShards.add(new StreamShardHandle(shardMetadata.getStreamName(), shard));
        }

        final TestFetcher<String> fetcher =
                new TestFetcher<>(
                        Collections.singletonList(TEST_STREAM_NAME),
                        new TestSourceContext<>(),
                        new TestRuntimeContext(true, 1, 0),
                        TestUtils.getStandardProperties(),
                        new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
                        state,
                        initialDiscoveryShards);

        final DummyFlinkKinesisConsumer<String> consumer =
                new DummyFlinkKinesisConsumer<>(
                        fetcher,
                        new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()));

        StreamSource<String, DummyFlinkKinesisConsumer<String>> consumerOperator =
                new StreamSource<>(consumer);

        final AbstractStreamOperatorTestHarness<String> testHarness =
                new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

        testHarness.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        testHarness.setup();
        testHarness.open();

        final AtomicReference<Throwable> error = new AtomicReference<>();

        // run the source asynchronously
        Thread runner =
                new Thread() {
                    @Override
                    public void run() {
                        try {
                            consumer.run(new TestSourceContext<>());
                        } catch (Throwable t) {
                            t.printStackTrace();
                            error.set(t);
                        }
                    }
                };
        runner.start();

        fetcher.waitUntilRun();

        final OperatorSubtaskState snapshot;
        synchronized (testHarness.getCheckpointLock()) {
            snapshot = testHarness.snapshot(0L, 0L);
        }

        OperatorSnapshotUtil.writeStateHandle(snapshot, path);

        consumerOperator.close();
        runner.join();
    }

    private static class DummyFlinkKinesisConsumer<T> extends FlinkKinesisConsumer<T> {

        private static final long serialVersionUID = -1573896262106029446L;

        private KinesisDataFetcher<T> mockFetcher;

        private static Properties dummyConfig = TestUtils.getStandardProperties();

        DummyFlinkKinesisConsumer(
                KinesisDataFetcher<T> mockFetcher, KinesisDeserializationSchema<T> schema) {
            super(TEST_STREAM_NAME, schema, dummyConfig);
            this.mockFetcher = mockFetcher;
        }

        @Override
        protected KinesisDataFetcher<T> createFetcher(
                List<String> streams,
                SourceContext<T> sourceContext,
                RuntimeContext runtimeContext,
                Properties configProps,
                KinesisDeserializationSchema<T> deserializer) {
            return mockFetcher;
        }
    }

    private static class TestFetcher<T> extends KinesisDataFetcher<T> {

        final OneShotLatch runLatch = new OneShotLatch();

        final HashMap<StreamShardMetadata, SequenceNumber> testStateSnapshot;
        final List<StreamShardHandle> testInitialDiscoveryShards;

        public TestFetcher(
                List<String> streams,
                SourceFunction.SourceContext<T> sourceContext,
                RuntimeContext runtimeContext,
                Properties configProps,
                KinesisDeserializationSchema<T> deserializationSchema,
                HashMap<StreamShardMetadata, SequenceNumber> testStateSnapshot,
                List<StreamShardHandle> testInitialDiscoveryShards) {

            super(
                    streams,
                    sourceContext,
                    runtimeContext,
                    configProps,
                    deserializationSchema,
                    DEFAULT_SHARD_ASSIGNER,
                    null,
                    null);

            this.testStateSnapshot = testStateSnapshot;
            this.testInitialDiscoveryShards = testInitialDiscoveryShards;
        }

        @Override
        public void runFetcher() throws Exception {
            runLatch.trigger();
        }

        @Override
        public HashMap<StreamShardMetadata, SequenceNumber> snapshotState() {
            return testStateSnapshot;
        }

        public void waitUntilRun() throws InterruptedException {
            runLatch.await();
        }

        @Override
        public List<StreamShardHandle> discoverNewShardsToSubscribe() throws InterruptedException {
            return testInitialDiscoveryShards;
        }

        @Override
        public void awaitTermination() throws InterruptedException {
            // do nothing
        }
    }

    private static Map<StreamShardMetadata, SequenceNumber> removeEquivalenceWrappers(
            Map<StreamShardMetadata.EquivalenceWrapper, SequenceNumber> equivalenceWrappedMap) {

        Map<StreamShardMetadata, SequenceNumber> unwrapped = new HashMap<>();
        for (Map.Entry<StreamShardMetadata.EquivalenceWrapper, SequenceNumber> wrapped :
                equivalenceWrappedMap.entrySet()) {
            unwrapped.put(wrapped.getKey().getShardMetadata(), wrapped.getValue());
        }

        return unwrapped;
    }
}
