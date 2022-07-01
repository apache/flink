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

package org.apache.flink.streaming.connectors.kinesis.internals;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.KinesisShardAssigner;
import org.apache.flink.streaming.connectors.kinesis.internals.publisher.RecordPublisher;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardMetadata;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyInterface;
import org.apache.flink.streaming.connectors.kinesis.proxy.KinesisProxyV2Interface;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kinesis.testutils.AlwaysThrowsDeserializationSchema;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisBehavioursFactory;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisShardIdGenerator;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestSourceContext;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestUtils;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestableKinesisDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestableKinesisDataFetcherForShardConsumerException;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.TestLogger;

import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.SHARD_DISCOVERY_INTERVAL_MILLIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for the {@link KinesisDataFetcher}. */
public class KinesisDataFetcherTest extends TestLogger {

    @Test
    public void testIsRunning() {
        KinesisDataFetcher<String> fetcher =
                createTestDataFetcherWithNoShards(10, 2, "test-stream");

        assertThat(fetcher.isRunning()).isTrue();
    }

    @Test
    @Timeout(10)
    public void testIsRunningFalseAfterShutDown() throws InterruptedException {
        KinesisDataFetcher<String> fetcher =
                createTestDataFetcherWithNoShards(10, 2, "test-stream");

        fetcher.shutdownFetcher();
        fetcher.awaitTermination();

        assertThat(fetcher.isRunning()).isFalse();
    }

    @Test
    public void testCancelDuringDiscovery() throws Exception {
        final String stream = "fakeStream";
        final int numShards = 3;

        Properties standardProperties = TestUtils.getStandardProperties();
        standardProperties.setProperty(SHARD_DISCOVERY_INTERVAL_MILLIS, "10000000");
        final LinkedList<KinesisStreamShardState> testShardStates = new LinkedList<>();
        final TestSourceContext<String> sourceContext = new TestSourceContext<>();
        TestableKinesisDataFetcher<String> fetcher =
                new TestableKinesisDataFetcher<String>(
                        singletonList(stream),
                        sourceContext,
                        standardProperties,
                        new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
                        1,
                        0,
                        new AtomicReference<>(),
                        testShardStates,
                        new HashMap<>(),
                        FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(
                                Collections.singletonMap(stream, numShards)));

        // FlinkKinesisConsumer is responsible for setting up the fetcher before it can be run;
        // run the consumer until it reaches the point where the fetcher starts to run
        final DummyFlinkKinesisConsumer<String> consumer =
                new DummyFlinkKinesisConsumer<>(TestUtils.getStandardProperties(), fetcher, 1, 0);

        CheckedThread consumerThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        consumer.run(new TestSourceContext<>());
                    }
                };
        consumerThread.start();

        // wait for the second discovery to be triggered, that has a high probability to be inside
        // discovery sleep (10k s)
        fetcher.waitUntilDiscovery(2);

        Thread.sleep(1000);

        consumer.cancel();
        consumerThread.sync();
    }

    @Test
    public void testIfNoShardsAreFoundShouldThrowException() {
        assertThatThrownBy(
                        () -> {
                            KinesisDataFetcher<String> fetcher =
                                    createTestDataFetcherWithNoShards(
                                            10, 2, "fakeStream1", "fakeStream2");

                            fetcher.runFetcher(); // this should throw RuntimeException
                        })
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testSkipCorruptedRecord() throws Exception {
        final String stream = "fakeStream";
        final int numShards = 3;

        final LinkedList<KinesisStreamShardState> testShardStates = new LinkedList<>();
        final TestSourceContext<String> sourceContext = new TestSourceContext<>();

        final TestableKinesisDataFetcher<String> fetcher =
                new TestableKinesisDataFetcher<>(
                        singletonList(stream),
                        sourceContext,
                        TestUtils.getStandardProperties(),
                        new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
                        1,
                        0,
                        new AtomicReference<>(),
                        testShardStates,
                        new HashMap<>(),
                        FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(
                                Collections.singletonMap(stream, numShards)));

        // FlinkKinesisConsumer is responsible for setting up the fetcher before it can be run;
        // run the consumer until it reaches the point where the fetcher starts to run
        final DummyFlinkKinesisConsumer<String> consumer =
                new DummyFlinkKinesisConsumer<>(TestUtils.getStandardProperties(), fetcher, 1, 0);

        CheckedThread consumerThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        consumer.run(new TestSourceContext<>());
                    }
                };
        consumerThread.start();

        fetcher.waitUntilRun();
        consumer.cancel();
        consumerThread.sync();

        assertThat(testShardStates).hasSize(numShards);

        for (int i = 0; i < numShards; i++) {
            fetcher.emitRecordAndUpdateState(
                    "record-" + i, 10L, i, new SequenceNumber("seq-num-1"));
            assertThat(testShardStates.get(i).getLastProcessedSequenceNum())
                    .isEqualTo(new SequenceNumber("seq-num-1"));
            assertThat(sourceContext.removeLatestOutput())
                    .isEqualTo(new StreamRecord<>("record-" + i, 10L));
        }

        // emitting a null (i.e., a corrupt record) should not produce any output, but still have
        // the shard state updated
        fetcher.emitRecordAndUpdateState(null, 10L, 1, new SequenceNumber("seq-num-2"));
        assertThat(testShardStates.get(1).getLastProcessedSequenceNum())
                .isEqualTo(new SequenceNumber("seq-num-2"));
        assertThat(sourceContext.removeLatestOutput())
                .isNull(); // no output should have been collected
    }

    @Test
    public void testStreamToLastSeenShardStateIsCorrectlySetWhenNotRestoringFromFailure()
            throws Exception {
        List<String> fakeStreams = new LinkedList<>();
        fakeStreams.add("fakeStream1");
        fakeStreams.add("fakeStream2");
        fakeStreams.add("fakeStream3");
        fakeStreams.add("fakeStream4");

        HashMap<String, String> subscribedStreamsToLastSeenShardIdsUnderTest =
                KinesisDataFetcher.createInitialSubscribedStreamsToLastDiscoveredShardsState(
                        fakeStreams);

        Map<String, Integer> streamToShardCount = new HashMap<>();
        Random rand = new Random();
        for (String fakeStream : fakeStreams) {
            streamToShardCount.put(fakeStream, rand.nextInt(5) + 1);
        }

        final TestableKinesisDataFetcher<String> fetcher =
                new TestableKinesisDataFetcher<>(
                        fakeStreams,
                        new TestSourceContext<>(),
                        TestUtils.getStandardProperties(),
                        new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
                        10,
                        2,
                        new AtomicReference<>(),
                        new LinkedList<>(),
                        subscribedStreamsToLastSeenShardIdsUnderTest,
                        FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(
                                streamToShardCount));

        final DummyFlinkKinesisConsumer<String> consumer =
                new DummyFlinkKinesisConsumer<>(TestUtils.getStandardProperties(), fetcher, 1, 0);

        CheckedThread consumerThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        consumer.run(new TestSourceContext<>());
                    }
                };
        consumerThread.start();

        fetcher.waitUntilRun();
        consumer.cancel();
        consumerThread.sync();

        // assert that the streams tracked in the state are identical to the subscribed streams
        Set<String> streamsInState = subscribedStreamsToLastSeenShardIdsUnderTest.keySet();
        assertThat(streamsInState).hasSize(fakeStreams.size());
        assertThat(streamsInState.containsAll(fakeStreams)).isTrue();

        // assert that the last seen shards in state is correctly set
        for (Map.Entry<String, String> streamToLastSeenShard :
                subscribedStreamsToLastSeenShardIdsUnderTest.entrySet()) {
            assertThat(streamToLastSeenShard.getValue())
                    .isEqualTo(
                            KinesisShardIdGenerator.generateFromShardOrder(
                                    streamToShardCount.get(streamToLastSeenShard.getKey()) - 1));
        }
    }

    @Test
    public void testStreamToLastSeenShardStateIsCorrectlySetWhenNoNewShardsSinceRestoredCheckpoint()
            throws Exception {
        List<String> fakeStreams = new LinkedList<>();
        fakeStreams.add("fakeStream1");
        fakeStreams.add("fakeStream2");

        Map<StreamShardHandle, String> restoredStateUnderTest = new HashMap<>();

        // fakeStream1 has 3 shards before restore
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream1",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
                UUID.randomUUID().toString());
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream1",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
                UUID.randomUUID().toString());
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream1",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))),
                UUID.randomUUID().toString());

        // fakeStream2 has 2 shards before restore
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream2",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
                UUID.randomUUID().toString());
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream2",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
                UUID.randomUUID().toString());

        Map<String, Integer> streamToShardCount = new HashMap<>();
        streamToShardCount.put(
                "fakeStream1", 3); // fakeStream1 will still have 3 shards after restore
        streamToShardCount.put(
                "fakeStream2", 2); // fakeStream2 will still have 2 shards after restore

        HashMap<String, String> subscribedStreamsToLastSeenShardIdsUnderTest =
                KinesisDataFetcher.createInitialSubscribedStreamsToLastDiscoveredShardsState(
                        fakeStreams);

        final TestableKinesisDataFetcher<String> fetcher =
                new TestableKinesisDataFetcher<>(
                        fakeStreams,
                        new TestSourceContext<>(),
                        TestUtils.getStandardProperties(),
                        new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
                        10,
                        2,
                        new AtomicReference<>(),
                        new LinkedList<>(),
                        subscribedStreamsToLastSeenShardIdsUnderTest,
                        FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(
                                streamToShardCount));

        for (Map.Entry<StreamShardHandle, String> restoredState :
                restoredStateUnderTest.entrySet()) {
            fetcher.advanceLastDiscoveredShardOfStream(
                    restoredState.getKey().getStreamName(),
                    restoredState.getKey().getShard().getShardId());
            fetcher.registerNewSubscribedShardState(
                    new KinesisStreamShardState(
                            KinesisDataFetcher.convertToStreamShardMetadata(restoredState.getKey()),
                            restoredState.getKey(),
                            new SequenceNumber(restoredState.getValue())));
        }

        CheckedThread runFetcherThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        fetcher.runFetcher();
                    }
                };
        runFetcherThread.start();

        fetcher.waitUntilInitialDiscovery();
        fetcher.shutdownFetcher();
        runFetcherThread.sync();

        // assert that the streams tracked in the state are identical to the subscribed streams
        Set<String> streamsInState = subscribedStreamsToLastSeenShardIdsUnderTest.keySet();
        assertThat(streamsInState).hasSize(fakeStreams.size());
        assertThat(streamsInState.containsAll(fakeStreams)).isTrue();

        // assert that the last seen shards in state is correctly set
        for (Map.Entry<String, String> streamToLastSeenShard :
                subscribedStreamsToLastSeenShardIdsUnderTest.entrySet()) {
            assertThat(streamToLastSeenShard.getValue())
                    .isEqualTo(
                            KinesisShardIdGenerator.generateFromShardOrder(
                                    streamToShardCount.get(streamToLastSeenShard.getKey()) - 1));
        }
    }

    @Test
    public void
            testStreamToLastSeenShardStateIsCorrectlySetWhenNewShardsFoundSinceRestoredCheckpoint()
                    throws Exception {
        List<String> fakeStreams = new LinkedList<>();
        fakeStreams.add("fakeStream1");
        fakeStreams.add("fakeStream2");

        Map<StreamShardHandle, String> restoredStateUnderTest = new HashMap<>();

        // fakeStream1 has 3 shards before restore
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream1",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
                UUID.randomUUID().toString());
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream1",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
                UUID.randomUUID().toString());
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream1",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))),
                UUID.randomUUID().toString());

        // fakeStream2 has 2 shards before restore
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream2",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
                UUID.randomUUID().toString());
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream2",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
                UUID.randomUUID().toString());

        Map<String, Integer> streamToShardCount = new HashMap<>();
        streamToShardCount.put(
                "fakeStream1",
                3 + 1); // fakeStream1 had 3 shards before & 1 new shard after restore
        streamToShardCount.put(
                "fakeStream2",
                2 + 3); // fakeStream2 had 2 shards before & 3 new shard after restore

        HashMap<String, String> subscribedStreamsToLastSeenShardIdsUnderTest =
                KinesisDataFetcher.createInitialSubscribedStreamsToLastDiscoveredShardsState(
                        fakeStreams);

        // using a non-resharded streams kinesis behaviour to represent that Kinesis is not
        // resharded AFTER the restore
        final TestableKinesisDataFetcher<String> fetcher =
                new TestableKinesisDataFetcher<>(
                        fakeStreams,
                        new TestSourceContext<>(),
                        TestUtils.getStandardProperties(),
                        new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
                        10,
                        2,
                        new AtomicReference<>(),
                        new LinkedList<>(),
                        subscribedStreamsToLastSeenShardIdsUnderTest,
                        FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(
                                streamToShardCount));

        for (Map.Entry<StreamShardHandle, String> restoredState :
                restoredStateUnderTest.entrySet()) {
            fetcher.advanceLastDiscoveredShardOfStream(
                    restoredState.getKey().getStreamName(),
                    restoredState.getKey().getShard().getShardId());
            fetcher.registerNewSubscribedShardState(
                    new KinesisStreamShardState(
                            KinesisDataFetcher.convertToStreamShardMetadata(restoredState.getKey()),
                            restoredState.getKey(),
                            new SequenceNumber(restoredState.getValue())));
        }

        CheckedThread runFetcherThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        fetcher.runFetcher();
                    }
                };
        runFetcherThread.start();

        fetcher.waitUntilInitialDiscovery();
        fetcher.shutdownFetcher();
        runFetcherThread.sync();

        // assert that the streams tracked in the state are identical to the subscribed streams
        Set<String> streamsInState = subscribedStreamsToLastSeenShardIdsUnderTest.keySet();
        assertThat(streamsInState).hasSize(fakeStreams.size());
        assertThat(streamsInState.containsAll(fakeStreams)).isTrue();

        // assert that the last seen shards in state is correctly set
        for (Map.Entry<String, String> streamToLastSeenShard :
                subscribedStreamsToLastSeenShardIdsUnderTest.entrySet()) {
            assertThat(streamToLastSeenShard.getValue())
                    .isEqualTo(
                            KinesisShardIdGenerator.generateFromShardOrder(
                                    streamToShardCount.get(streamToLastSeenShard.getKey()) - 1));
        }
    }

    @Test
    public void
            testStreamToLastSeenShardStateIsCorrectlySetWhenNoNewShardsSinceRestoredCheckpointAndSomeStreamsDoNotExist()
                    throws Exception {
        List<String> fakeStreams = new LinkedList<>();
        fakeStreams.add("fakeStream1");
        fakeStreams.add("fakeStream2");
        fakeStreams.add("fakeStream3"); // fakeStream3 will not have any shards
        fakeStreams.add("fakeStream4"); // fakeStream4 will not have any shards

        Map<StreamShardHandle, String> restoredStateUnderTest = new HashMap<>();

        // fakeStream1 has 3 shards before restore
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream1",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
                UUID.randomUUID().toString());
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream1",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
                UUID.randomUUID().toString());
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream1",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))),
                UUID.randomUUID().toString());

        // fakeStream2 has 2 shards before restore
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream2",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
                UUID.randomUUID().toString());
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream2",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
                UUID.randomUUID().toString());

        Map<String, Integer> streamToShardCount = new HashMap<>();
        streamToShardCount.put("fakeStream1", 3); // fakeStream1 has fixed 3 shards
        streamToShardCount.put("fakeStream2", 2); // fakeStream2 has fixed 2 shards
        streamToShardCount.put("fakeStream3", 0); // no shards can be found for fakeStream3
        streamToShardCount.put("fakeStream4", 0); // no shards can be found for fakeStream4

        HashMap<String, String> subscribedStreamsToLastSeenShardIdsUnderTest =
                KinesisDataFetcher.createInitialSubscribedStreamsToLastDiscoveredShardsState(
                        fakeStreams);

        // using a non-resharded streams kinesis behaviour to represent that Kinesis is not
        // resharded AFTER the restore
        final TestableKinesisDataFetcher<String> fetcher =
                new TestableKinesisDataFetcher<>(
                        fakeStreams,
                        new TestSourceContext<>(),
                        TestUtils.getStandardProperties(),
                        new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
                        10,
                        2,
                        new AtomicReference<>(),
                        new LinkedList<>(),
                        subscribedStreamsToLastSeenShardIdsUnderTest,
                        FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(
                                streamToShardCount));

        for (Map.Entry<StreamShardHandle, String> restoredState :
                restoredStateUnderTest.entrySet()) {
            fetcher.advanceLastDiscoveredShardOfStream(
                    restoredState.getKey().getStreamName(),
                    restoredState.getKey().getShard().getShardId());
            fetcher.registerNewSubscribedShardState(
                    new KinesisStreamShardState(
                            KinesisDataFetcher.convertToStreamShardMetadata(restoredState.getKey()),
                            restoredState.getKey(),
                            new SequenceNumber(restoredState.getValue())));
        }

        CheckedThread runFetcherThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        fetcher.runFetcher();
                    }
                };
        runFetcherThread.start();

        fetcher.waitUntilInitialDiscovery();
        fetcher.shutdownFetcher();
        runFetcherThread.sync();

        // assert that the streams tracked in the state are identical to the subscribed streams
        Set<String> streamsInState = subscribedStreamsToLastSeenShardIdsUnderTest.keySet();
        assertThat(streamsInState).hasSize(fakeStreams.size());
        assertThat(streamsInState.containsAll(fakeStreams)).isTrue();

        // assert that the last seen shards in state is correctly set
        assertThat(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream1"))
                .isEqualTo(KinesisShardIdGenerator.generateFromShardOrder(2));
        assertThat(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream2"))
                .isEqualTo(KinesisShardIdGenerator.generateFromShardOrder(1));
        assertThat(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream3")).isNull();
        assertThat(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream4")).isNull();
    }

    @Test
    public void
            testStreamToLastSeenShardStateIsCorrectlySetWhenNewShardsFoundSinceRestoredCheckpointAndSomeStreamsDoNotExist()
                    throws Exception {
        List<String> fakeStreams = new LinkedList<>();
        fakeStreams.add("fakeStream1");
        fakeStreams.add("fakeStream2");
        fakeStreams.add("fakeStream3"); // fakeStream3 will not have any shards
        fakeStreams.add("fakeStream4"); // fakeStream4 will not have any shards

        Map<StreamShardHandle, String> restoredStateUnderTest = new HashMap<>();

        // fakeStream1 has 3 shards before restore
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream1",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
                UUID.randomUUID().toString());
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream1",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
                UUID.randomUUID().toString());
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream1",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))),
                UUID.randomUUID().toString());

        // fakeStream2 has 2 shards before restore
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream2",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
                UUID.randomUUID().toString());
        restoredStateUnderTest.put(
                new StreamShardHandle(
                        "fakeStream2",
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
                UUID.randomUUID().toString());

        Map<String, Integer> streamToShardCount = new HashMap<>();
        streamToShardCount.put(
                "fakeStream1",
                3 + 1); // fakeStream1 had 3 shards before & 1 new shard after restore
        streamToShardCount.put(
                "fakeStream2",
                2 + 3); // fakeStream2 had 2 shards before & 2 new shard after restore
        streamToShardCount.put("fakeStream3", 0); // no shards can be found for fakeStream3
        streamToShardCount.put("fakeStream4", 0); // no shards can be found for fakeStream4

        HashMap<String, String> subscribedStreamsToLastSeenShardIdsUnderTest =
                KinesisDataFetcher.createInitialSubscribedStreamsToLastDiscoveredShardsState(
                        fakeStreams);

        // using a non-resharded streams kinesis behaviour to represent that Kinesis is not
        // resharded AFTER the restore
        final TestableKinesisDataFetcher<String> fetcher =
                new TestableKinesisDataFetcher<>(
                        fakeStreams,
                        new TestSourceContext<>(),
                        TestUtils.getStandardProperties(),
                        new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
                        10,
                        2,
                        new AtomicReference<>(),
                        new LinkedList<>(),
                        subscribedStreamsToLastSeenShardIdsUnderTest,
                        FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(
                                streamToShardCount));

        for (Map.Entry<StreamShardHandle, String> restoredState :
                restoredStateUnderTest.entrySet()) {
            fetcher.advanceLastDiscoveredShardOfStream(
                    restoredState.getKey().getStreamName(),
                    restoredState.getKey().getShard().getShardId());
            fetcher.registerNewSubscribedShardState(
                    new KinesisStreamShardState(
                            KinesisDataFetcher.convertToStreamShardMetadata(restoredState.getKey()),
                            restoredState.getKey(),
                            new SequenceNumber(restoredState.getValue())));
        }

        CheckedThread runFetcherThread =
                new CheckedThread() {
                    @Override
                    public void go() throws Exception {
                        fetcher.runFetcher();
                    }
                };
        runFetcherThread.start();

        fetcher.waitUntilInitialDiscovery();
        fetcher.shutdownFetcher();
        runFetcherThread.sync();

        // assert that the streams tracked in the state are identical to the subscribed streams
        Set<String> streamsInState = subscribedStreamsToLastSeenShardIdsUnderTest.keySet();
        assertThat(streamsInState).hasSize(fakeStreams.size());
        assertThat(streamsInState.containsAll(fakeStreams)).isTrue();

        // assert that the last seen shards in state is correctly set
        assertThat(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream1"))
                .isEqualTo(KinesisShardIdGenerator.generateFromShardOrder(3));
        assertThat(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream2"))
                .isEqualTo(KinesisShardIdGenerator.generateFromShardOrder(4));
        assertThat(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream3")).isNull();
        assertThat(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream4")).isNull();
    }

    @Test
    public void testStreamShardMetadataAndHandleConversion() {
        String streamName = "fakeStream1";
        String shardId = "shard-000001";
        String parentShardId = "shard-000002";
        String adjacentParentShardId = "shard-000003";
        String startingHashKey = "key-000001";
        String endingHashKey = "key-000010";
        String startingSequenceNumber = "seq-0000021";
        String endingSequenceNumber = "seq-00000031";

        StreamShardMetadata kinesisStreamShard = new StreamShardMetadata();
        kinesisStreamShard.setStreamName(streamName);
        kinesisStreamShard.setShardId(shardId);
        kinesisStreamShard.setParentShardId(parentShardId);
        kinesisStreamShard.setAdjacentParentShardId(adjacentParentShardId);
        kinesisStreamShard.setStartingHashKey(startingHashKey);
        kinesisStreamShard.setEndingHashKey(endingHashKey);
        kinesisStreamShard.setStartingSequenceNumber(startingSequenceNumber);
        kinesisStreamShard.setEndingSequenceNumber(endingSequenceNumber);

        Shard shard =
                new Shard()
                        .withShardId(shardId)
                        .withParentShardId(parentShardId)
                        .withAdjacentParentShardId(adjacentParentShardId)
                        .withHashKeyRange(
                                new HashKeyRange()
                                        .withStartingHashKey(startingHashKey)
                                        .withEndingHashKey(endingHashKey))
                        .withSequenceNumberRange(
                                new SequenceNumberRange()
                                        .withStartingSequenceNumber(startingSequenceNumber)
                                        .withEndingSequenceNumber(endingSequenceNumber));
        StreamShardHandle streamShardHandle = new StreamShardHandle(streamName, shard);

        assertThat(KinesisDataFetcher.convertToStreamShardMetadata(streamShardHandle))
                .isEqualTo(kinesisStreamShard);
        assertThat(KinesisDataFetcher.convertToStreamShardHandle(kinesisStreamShard))
                .isEqualTo(streamShardHandle);
    }

    private static class DummyFlinkKinesisConsumer<T> extends FlinkKinesisConsumer<T> {
        private static final long serialVersionUID = 1L;

        private final KinesisDataFetcher<T> fetcher;

        private final int numParallelSubtasks;
        private final int subtaskIndex;

        @SuppressWarnings("unchecked")
        DummyFlinkKinesisConsumer(
                Properties properties,
                KinesisDataFetcher<T> fetcher,
                int numParallelSubtasks,
                int subtaskIndex) {
            super("test", mock(KinesisDeserializationSchema.class), properties);
            this.fetcher = fetcher;
            this.numParallelSubtasks = numParallelSubtasks;
            this.subtaskIndex = subtaskIndex;
        }

        @Override
        protected KinesisDataFetcher<T> createFetcher(
                List<String> streams,
                SourceFunction.SourceContext<T> sourceContext,
                RuntimeContext runtimeContext,
                Properties configProps,
                KinesisDeserializationSchema<T> deserializationSchema) {
            return fetcher;
        }

        @Override
        public RuntimeContext getRuntimeContext() {
            RuntimeContext context = mock(RuntimeContext.class);
            when(context.getIndexOfThisSubtask()).thenReturn(subtaskIndex);
            when(context.getNumberOfParallelSubtasks()).thenReturn(numParallelSubtasks);
            return context;
        }
    }

    // ----------------------------------------------------------------------
    // Tests shard distribution with custom hash function
    // ----------------------------------------------------------------------

    @Test
    public void testShardToSubtaskMappingWithCustomHashFunction() throws Exception {

        int totalCountOfSubtasks = 10;
        int shardCount = 3;

        for (int i = 0; i < 2; i++) {

            final int hash = i;
            final KinesisShardAssigner allShardsSingleSubtaskFn = (shard, subtasks) -> hash;
            Map<String, Integer> streamToShardCount = new HashMap<>();
            List<String> fakeStreams = new LinkedList<>();
            fakeStreams.add("fakeStream");
            streamToShardCount.put("fakeStream", shardCount);

            for (int j = 0; j < totalCountOfSubtasks; j++) {

                int subtaskIndex = j;
                // subscribe with default hashing
                final TestableKinesisDataFetcher fetcher =
                        new TestableKinesisDataFetcher(
                                fakeStreams,
                                new TestSourceContext<>(),
                                new Properties(),
                                new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
                                totalCountOfSubtasks,
                                subtaskIndex,
                                new AtomicReference<>(),
                                new LinkedList<>(),
                                KinesisDataFetcher
                                        .createInitialSubscribedStreamsToLastDiscoveredShardsState(
                                                fakeStreams),
                                FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(
                                        streamToShardCount));
                Whitebox.setInternalState(
                        fetcher, "shardAssigner", allShardsSingleSubtaskFn); // override hashing
                List<StreamShardHandle> shards = fetcher.discoverNewShardsToSubscribe();
                fetcher.shutdownFetcher();

                String msg = String.format("for hash=%d, subtask=%d", hash, subtaskIndex);
                if (j == i) {
                    assertThat(shards).as(msg).hasSize(shardCount);
                } else {
                    assertThat(shards).as(msg).isEmpty();
                }
            }
        }
    }

    @Test
    public void testIsThisSubtaskShouldSubscribeTo() {
        assertThat(KinesisDataFetcher.isThisSubtaskShouldSubscribeTo(0, 2, 0)).isTrue();
        assertThat(KinesisDataFetcher.isThisSubtaskShouldSubscribeTo(1, 2, 0)).isFalse();
        assertThat(KinesisDataFetcher.isThisSubtaskShouldSubscribeTo(2, 2, 0)).isTrue();
        assertThat(KinesisDataFetcher.isThisSubtaskShouldSubscribeTo(0, 2, 1)).isFalse();
        assertThat(KinesisDataFetcher.isThisSubtaskShouldSubscribeTo(1, 2, 1)).isTrue();
        assertThat(KinesisDataFetcher.isThisSubtaskShouldSubscribeTo(2, 2, 1)).isFalse();
    }

    private static BoundedOutOfOrdernessTimestampExtractor<String> watermarkAssigner =
            new BoundedOutOfOrdernessTimestampExtractor<String>(Time.milliseconds(0)) {
                @Override
                public long extractTimestamp(String element) {
                    return Long.parseLong(element);
                }
            };

    @Test
    public void testPeriodicWatermark() {
        final MutableLong clock = new MutableLong();
        final MutableBoolean isTemporaryIdle = new MutableBoolean();
        final List<Watermark> watermarks = new ArrayList<>();

        String fakeStream1 = "fakeStream1";
        StreamShardHandle shardHandle =
                new StreamShardHandle(
                        fakeStream1,
                        new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0)));

        TestSourceContext<String> sourceContext =
                new TestSourceContext<String>() {
                    @Override
                    public void emitWatermark(Watermark mark) {
                        watermarks.add(mark);
                    }

                    @Override
                    public void markAsTemporarilyIdle() {
                        isTemporaryIdle.setTrue();
                    }
                };

        HashMap<String, String> subscribedStreamsToLastSeenShardIdsUnderTest = new HashMap<>();

        final KinesisDataFetcher<String> fetcher =
                new TestableKinesisDataFetcher<String>(
                        singletonList(fakeStream1),
                        sourceContext,
                        new java.util.Properties(),
                        new KinesisDeserializationSchemaWrapper<>(
                                new org.apache.flink.streaming.util.serialization
                                        .SimpleStringSchema()),
                        1,
                        1,
                        new AtomicReference<>(),
                        new LinkedList<>(),
                        subscribedStreamsToLastSeenShardIdsUnderTest,
                        FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(
                                new HashMap<>())) {

                    @Override
                    protected long getCurrentTimeMillis() {
                        return clock.getValue();
                    }
                };
        Whitebox.setInternalState(fetcher, "periodicWatermarkAssigner", watermarkAssigner);

        SequenceNumber seq = new SequenceNumber("fakeSequenceNumber");
        // register shards to subsequently emit records
        int shardIndex =
                fetcher.registerNewSubscribedShardState(
                        new KinesisStreamShardState(
                                KinesisDataFetcher.convertToStreamShardMetadata(shardHandle),
                                shardHandle,
                                seq));

        StreamRecord<String> record1 =
                new StreamRecord<>(String.valueOf(Long.MIN_VALUE), Long.MIN_VALUE);
        fetcher.emitRecordAndUpdateState(
                record1.getValue(), record1.getTimestamp(), shardIndex, seq);
        assertThat(sourceContext.getCollectedOutputs().poll()).isEqualTo(record1);

        fetcher.emitWatermark();
        assertThat(watermarks).as("potential watermark equals previous watermark").isEmpty();

        StreamRecord<String> record2 = new StreamRecord<>(String.valueOf(1), 1);
        fetcher.emitRecordAndUpdateState(
                record2.getValue(), record2.getTimestamp(), shardIndex, seq);
        assertThat(sourceContext.getCollectedOutputs().poll()).isEqualTo(record2);

        fetcher.emitWatermark();
        assertThat(watermarks).as("watermark advanced").isNotEmpty();
        assertThat(watermarks.remove(0)).isEqualTo(new Watermark(record2.getTimestamp()));
        assertThat(isTemporaryIdle.booleanValue()).as("not idle").isFalse();

        // test idle timeout
        long idleTimeout = 10;
        // advance clock idleTimeout
        clock.add(idleTimeout + 1);
        fetcher.emitWatermark();
        assertThat(isTemporaryIdle.booleanValue()).as("not idle").isFalse();
        assertThat(watermarks).as("not idle, no new watermark").isEmpty();

        // activate idle timeout
        Whitebox.setInternalState(fetcher, "shardIdleIntervalMillis", idleTimeout);
        fetcher.emitWatermark();
        assertThat(isTemporaryIdle.booleanValue()).as("idle").isTrue();
        assertThat(watermarks).as("idle, no watermark").isEmpty();
    }

    @Test
    public void testOriginalExceptionIsPreservedWhenInterruptedDuringShutdown() throws Exception {
        String stream = "fakeStream";

        Map<String, List<BlockingQueue<String>>> streamsToShardQueues = new HashMap<>();
        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(10);
        queue.put("item1");
        streamsToShardQueues.put(stream, singletonList(queue));

        AlwaysThrowsDeserializationSchema deserializationSchema =
                new AlwaysThrowsDeserializationSchema();
        KinesisProxyInterface fakeKinesis =
                FakeKinesisBehavioursFactory.blockingQueueGetRecords(streamsToShardQueues);

        TestableKinesisDataFetcherForShardConsumerException<String> fetcher =
                new TestableKinesisDataFetcherForShardConsumerException<>(
                        singletonList(stream),
                        new TestSourceContext<>(),
                        TestUtils.getStandardProperties(),
                        new KinesisDeserializationSchemaWrapper<>(deserializationSchema),
                        10,
                        2,
                        new AtomicReference<>(),
                        new LinkedList<>(),
                        new HashMap<>(),
                        fakeKinesis,
                        (sequence, properties, metricGroup, streamShardHandle) ->
                                mock(RecordPublisher.class));

        DummyFlinkKinesisConsumer<String> consumer =
                new DummyFlinkKinesisConsumer<>(TestUtils.getStandardProperties(), fetcher, 1, 0);

        CheckedThread consumerThread =
                new CheckedThread("FlinkKinesisConsumer") {
                    @Override
                    public void go() throws Exception {
                        consumer.run(new TestSourceContext<>());
                    }
                };
        consumerThread.start();
        fetcher.waitUntilRun();

        // ShardConsumer exception (from deserializer) will result in fetcher being shut down.
        fetcher.waitUntilShutdown(20, TimeUnit.SECONDS);

        // Ensure that KinesisDataFetcher has exited its while(running) loop and is inside its
        // awaitTermination()
        // method before we interrupt its thread, so that our interrupt doesn't get absorbed by any
        // other mechanism.
        fetcher.waitUntilAwaitTermination(20, TimeUnit.SECONDS);

        // Interrupt the thread so that KinesisDataFetcher#awaitTermination() will throw
        // InterruptedException.
        consumerThread.interrupt();

        assertThatThrownBy(consumerThread::sync)
                .isInstanceOf(RuntimeException.class)
                .hasMessage(AlwaysThrowsDeserializationSchema.EXCEPTION_MESSAGE);

        assertThat(fetcher.wasInterrupted)
                .as(
                        "Expected Fetcher to have been interrupted. This test didn't accomplish its goal.")
                .isTrue();
    }

    @Test
    @Timeout(1)
    public void testRecordPublisherFactoryIsTornDown() throws InterruptedException {
        KinesisProxyV2Interface kinesisV2 = mock(KinesisProxyV2Interface.class);

        TestableKinesisDataFetcher<String> fetcher =
                new TestableKinesisDataFetcher<String>(
                        singletonList("fakeStream1"),
                        new TestSourceContext<>(),
                        TestUtils.efoProperties(),
                        new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
                        10,
                        2,
                        new AtomicReference<>(),
                        new LinkedList<>(),
                        new HashMap<>(),
                        mock(KinesisProxyInterface.class),
                        kinesisV2) {};

        fetcher.shutdownFetcher();

        fetcher.awaitTermination();
    }

    @Test
    @Timeout(10)
    public void testRecordPublisherFactoryIsTornDownWhenDeregisterStreamConsumerThrowsException()
            throws InterruptedException {
        KinesisProxyV2Interface kinesisV2 = mock(KinesisProxyV2Interface.class);

        TestableKinesisDataFetcher<String> fetcher =
                new TestableKinesisDataFetcher<String>(
                        singletonList("fakeStream1"),
                        new TestSourceContext<>(),
                        TestUtils.efoProperties(),
                        new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
                        10,
                        2,
                        new AtomicReference<>(),
                        new LinkedList<>(),
                        new HashMap<>(),
                        mock(KinesisProxyInterface.class),
                        kinesisV2) {
                    @Override
                    protected void deregisterStreamConsumer() {
                        throw new RuntimeException();
                    }
                };

        fetcher.shutdownFetcher();

        verify(kinesisV2).close();
        fetcher.awaitTermination();
    }

    @Test
    @Timeout(10)
    public void testExecutorServiceShutDownWhenCloseRecordPublisherFactoryThrowsException()
            throws InterruptedException {
        TestableKinesisDataFetcher<String> fetcher =
                new TestableKinesisDataFetcher<String>(
                        singletonList("fakeStream1"),
                        new TestSourceContext<>(),
                        TestUtils.efoProperties(),
                        new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
                        10,
                        2,
                        new AtomicReference<>(),
                        new LinkedList<>(),
                        new HashMap<>(),
                        mock(KinesisProxyInterface.class),
                        mock(KinesisProxyV2Interface.class)) {
                    @Override
                    protected void closeRecordPublisherFactory() {
                        throw new RuntimeException();
                    }
                };

        fetcher.shutdownFetcher();

        fetcher.awaitTermination();
    }

    private KinesisDataFetcher<String> createTestDataFetcherWithNoShards(
            final int subtaskCount, final int subtaskIndex, final String... streams) {
        List<String> streamList = Arrays.stream(streams).collect(Collectors.toList());

        HashMap<String, String> subscribedStreamsToLastSeenShardIdsUnderTest =
                KinesisDataFetcher.createInitialSubscribedStreamsToLastDiscoveredShardsState(
                        streamList);

        return new TestableKinesisDataFetcher<String>(
                streamList,
                new TestSourceContext<>(),
                TestUtils.getStandardProperties(),
                new KinesisDeserializationSchemaWrapper<String>(new SimpleStringSchema()),
                subtaskCount,
                subtaskIndex,
                new AtomicReference<>(),
                new LinkedList<>(),
                subscribedStreamsToLastSeenShardIdsUnderTest,
                FakeKinesisBehavioursFactory.noShardsFoundForRequestedStreamsBehaviour());
    }
}
