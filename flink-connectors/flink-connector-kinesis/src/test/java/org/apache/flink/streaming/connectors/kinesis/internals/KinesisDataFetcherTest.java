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
import org.junit.Assert;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
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

import static java.util.Collections.singletonList;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RECORD_PUBLISHER_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RecordPublisherType.EFO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link KinesisDataFetcher}.
 */
public class KinesisDataFetcherTest extends TestLogger {

	@Test(expected = RuntimeException.class)
	public void testIfNoShardsAreFoundShouldThrowException() throws Exception {
		List<String> fakeStreams = new LinkedList<>();
		fakeStreams.add("fakeStream1");
		fakeStreams.add("fakeStream2");

		HashMap<String, String> subscribedStreamsToLastSeenShardIdsUnderTest =
			KinesisDataFetcher.createInitialSubscribedStreamsToLastDiscoveredShardsState(fakeStreams);

		TestableKinesisDataFetcher<String> fetcher =
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
				FakeKinesisBehavioursFactory.noShardsFoundForRequestedStreamsBehaviour());

		fetcher.runFetcher(); // this should throw RuntimeException
	}

	@Test
	public void testSkipCorruptedRecord() throws Exception {
		final String stream = "fakeStream";
		final int numShards = 3;

		final LinkedList<KinesisStreamShardState> testShardStates = new LinkedList<>();
		final TestSourceContext<String> sourceContext = new TestSourceContext<>();

		final TestableKinesisDataFetcher<String> fetcher = new TestableKinesisDataFetcher<>(
			singletonList(stream),
			sourceContext,
			TestUtils.getStandardProperties(),
			new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
			1,
			0,
			new AtomicReference<>(),
			testShardStates,
			new HashMap<>(),
			FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(Collections.singletonMap(stream, numShards)));

		// FlinkKinesisConsumer is responsible for setting up the fetcher before it can be run;
		// run the consumer until it reaches the point where the fetcher starts to run
		final DummyFlinkKinesisConsumer<String> consumer = new DummyFlinkKinesisConsumer<>(TestUtils.getStandardProperties(), fetcher, 1, 0);

		CheckedThread consumerThread = new CheckedThread() {
			@Override
			public void go() throws Exception {
				consumer.run(new TestSourceContext<>());
			}
		};
		consumerThread.start();

		fetcher.waitUntilRun();
		consumer.cancel();
		consumerThread.sync();

		assertEquals(numShards, testShardStates.size());

		for (int i = 0; i < numShards; i++) {
			fetcher.emitRecordAndUpdateState("record-" + i, 10L, i, new SequenceNumber("seq-num-1"));
			assertEquals(new SequenceNumber("seq-num-1"), testShardStates.get(i).getLastProcessedSequenceNum());
			assertEquals(new StreamRecord<>("record-" + i, 10L), sourceContext.removeLatestOutput());
		}

		// emitting a null (i.e., a corrupt record) should not produce any output, but still have the shard state updated
		fetcher.emitRecordAndUpdateState(null, 10L, 1, new SequenceNumber("seq-num-2"));
			assertEquals(new SequenceNumber("seq-num-2"), testShardStates.get(1).getLastProcessedSequenceNum());
		assertNull(sourceContext.removeLatestOutput()); // no output should have been collected
	}

	@Test
	public void testStreamToLastSeenShardStateIsCorrectlySetWhenNotRestoringFromFailure() throws Exception {
		List<String> fakeStreams = new LinkedList<>();
		fakeStreams.add("fakeStream1");
		fakeStreams.add("fakeStream2");
		fakeStreams.add("fakeStream3");
		fakeStreams.add("fakeStream4");

		HashMap<String, String> subscribedStreamsToLastSeenShardIdsUnderTest =
			KinesisDataFetcher.createInitialSubscribedStreamsToLastDiscoveredShardsState(fakeStreams);

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
				FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(streamToShardCount));

		final DummyFlinkKinesisConsumer<String> consumer = new DummyFlinkKinesisConsumer<>(
				TestUtils.getStandardProperties(), fetcher, 1, 0);

		CheckedThread consumerThread = new CheckedThread() {
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
		assertEquals(fakeStreams.size(), streamsInState.size());
		assertTrue(streamsInState.containsAll(fakeStreams));

		// assert that the last seen shards in state is correctly set
		for (Map.Entry<String, String> streamToLastSeenShard : subscribedStreamsToLastSeenShardIdsUnderTest.entrySet()) {
			assertEquals(
				KinesisShardIdGenerator.generateFromShardOrder(streamToShardCount.get(streamToLastSeenShard.getKey()) - 1),
				streamToLastSeenShard.getValue());
		}
	}

	@Test
	public void testStreamToLastSeenShardStateIsCorrectlySetWhenNoNewShardsSinceRestoredCheckpoint() throws Exception {
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
		streamToShardCount.put("fakeStream1", 3); // fakeStream1 will still have 3 shards after restore
		streamToShardCount.put("fakeStream2", 2); // fakeStream2 will still have 2 shards after restore

		HashMap<String, String> subscribedStreamsToLastSeenShardIdsUnderTest =
			KinesisDataFetcher.createInitialSubscribedStreamsToLastDiscoveredShardsState(fakeStreams);

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
				FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(streamToShardCount));

		for (Map.Entry<StreamShardHandle, String> restoredState : restoredStateUnderTest.entrySet()) {
			fetcher.advanceLastDiscoveredShardOfStream(restoredState.getKey().getStreamName(), restoredState.getKey().getShard().getShardId());
			fetcher.registerNewSubscribedShardState(
				new KinesisStreamShardState(KinesisDataFetcher.convertToStreamShardMetadata(restoredState.getKey()),
					restoredState.getKey(), new SequenceNumber(restoredState.getValue())));
		}

		CheckedThread runFetcherThread = new CheckedThread() {
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
		assertEquals(fakeStreams.size(), streamsInState.size());
		assertTrue(streamsInState.containsAll(fakeStreams));

		// assert that the last seen shards in state is correctly set
		for (Map.Entry<String, String> streamToLastSeenShard : subscribedStreamsToLastSeenShardIdsUnderTest.entrySet()) {
			assertEquals(
				KinesisShardIdGenerator.generateFromShardOrder(streamToShardCount.get(streamToLastSeenShard.getKey()) - 1),
				streamToLastSeenShard.getValue());
		}
	}

	@Test
	public void testStreamToLastSeenShardStateIsCorrectlySetWhenNewShardsFoundSinceRestoredCheckpoint() throws Exception {
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
		streamToShardCount.put("fakeStream1", 3 + 1); // fakeStream1 had 3 shards before & 1 new shard after restore
		streamToShardCount.put("fakeStream2", 2 + 3); // fakeStream2 had 2 shards before & 3 new shard after restore

		HashMap<String, String> subscribedStreamsToLastSeenShardIdsUnderTest =
			KinesisDataFetcher.createInitialSubscribedStreamsToLastDiscoveredShardsState(fakeStreams);

		// using a non-resharded streams kinesis behaviour to represent that Kinesis is not resharded AFTER the restore
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
				FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(streamToShardCount));

		for (Map.Entry<StreamShardHandle, String> restoredState : restoredStateUnderTest.entrySet()) {
			fetcher.advanceLastDiscoveredShardOfStream(restoredState.getKey().getStreamName(), restoredState.getKey().getShard().getShardId());
			fetcher.registerNewSubscribedShardState(
				new KinesisStreamShardState(KinesisDataFetcher.convertToStreamShardMetadata(restoredState.getKey()),
					restoredState.getKey(), new SequenceNumber(restoredState.getValue())));
		}

		CheckedThread runFetcherThread = new CheckedThread() {
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
		assertEquals(fakeStreams.size(), streamsInState.size());
		assertTrue(streamsInState.containsAll(fakeStreams));

		// assert that the last seen shards in state is correctly set
		for (Map.Entry<String, String> streamToLastSeenShard : subscribedStreamsToLastSeenShardIdsUnderTest.entrySet()) {
			assertEquals(
				KinesisShardIdGenerator.generateFromShardOrder(streamToShardCount.get(streamToLastSeenShard.getKey()) - 1),
				streamToLastSeenShard.getValue());
		}
	}

	@Test
	public void testStreamToLastSeenShardStateIsCorrectlySetWhenNoNewShardsSinceRestoredCheckpointAndSomeStreamsDoNotExist() throws Exception {
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
			KinesisDataFetcher.createInitialSubscribedStreamsToLastDiscoveredShardsState(fakeStreams);

		// using a non-resharded streams kinesis behaviour to represent that Kinesis is not resharded AFTER the restore
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
				FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(streamToShardCount));

		for (Map.Entry<StreamShardHandle, String> restoredState : restoredStateUnderTest.entrySet()) {
			fetcher.advanceLastDiscoveredShardOfStream(restoredState.getKey().getStreamName(), restoredState.getKey().getShard().getShardId());
			fetcher.registerNewSubscribedShardState(
				new KinesisStreamShardState(KinesisDataFetcher.convertToStreamShardMetadata(restoredState.getKey()),
					restoredState.getKey(), new SequenceNumber(restoredState.getValue())));
		}

		CheckedThread runFetcherThread = new CheckedThread() {
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
		assertEquals(fakeStreams.size(), streamsInState.size());
		assertTrue(streamsInState.containsAll(fakeStreams));

		// assert that the last seen shards in state is correctly set
		assertEquals(
			KinesisShardIdGenerator.generateFromShardOrder(2),
			subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream1"));
		assertEquals(
			KinesisShardIdGenerator.generateFromShardOrder(1),
			subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream2"));
		assertNull(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream3"));
		assertNull(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream4"));
	}

	@Test
	public void testStreamToLastSeenShardStateIsCorrectlySetWhenNewShardsFoundSinceRestoredCheckpointAndSomeStreamsDoNotExist() throws Exception {
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
		streamToShardCount.put("fakeStream1", 3 + 1); // fakeStream1 had 3 shards before & 1 new shard after restore
		streamToShardCount.put("fakeStream2", 2 + 3); // fakeStream2 had 2 shards before & 2 new shard after restore
		streamToShardCount.put("fakeStream3", 0); // no shards can be found for fakeStream3
		streamToShardCount.put("fakeStream4", 0); // no shards can be found for fakeStream4

		HashMap<String, String> subscribedStreamsToLastSeenShardIdsUnderTest =
			KinesisDataFetcher.createInitialSubscribedStreamsToLastDiscoveredShardsState(fakeStreams);

		// using a non-resharded streams kinesis behaviour to represent that Kinesis is not resharded AFTER the restore
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
				FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(streamToShardCount));

		for (Map.Entry<StreamShardHandle, String> restoredState : restoredStateUnderTest.entrySet()) {
			fetcher.advanceLastDiscoveredShardOfStream(restoredState.getKey().getStreamName(), restoredState.getKey().getShard().getShardId());
			fetcher.registerNewSubscribedShardState(
				new KinesisStreamShardState(KinesisDataFetcher.convertToStreamShardMetadata(restoredState.getKey()),
					restoredState.getKey(), new SequenceNumber(restoredState.getValue())));
		}

		CheckedThread runFetcherThread = new CheckedThread() {
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
		assertEquals(fakeStreams.size(), streamsInState.size());
		assertTrue(streamsInState.containsAll(fakeStreams));

		// assert that the last seen shards in state is correctly set
		assertEquals(
			KinesisShardIdGenerator.generateFromShardOrder(3),
			subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream1"));
		assertEquals(
			KinesisShardIdGenerator.generateFromShardOrder(4),
			subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream2"));
		assertNull(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream3"));
		assertNull(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream4"));
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

		Shard shard = new Shard()
			.withShardId(shardId)
			.withParentShardId(parentShardId)
			.withAdjacentParentShardId(adjacentParentShardId)
			.withHashKeyRange(new HashKeyRange()
				.withStartingHashKey(startingHashKey)
				.withEndingHashKey(endingHashKey))
			.withSequenceNumberRange(new SequenceNumberRange()
				.withStartingSequenceNumber(startingSequenceNumber)
				.withEndingSequenceNumber(endingSequenceNumber));
		StreamShardHandle streamShardHandle = new StreamShardHandle(streamName, shard);

		assertEquals(kinesisStreamShard, KinesisDataFetcher.convertToStreamShardMetadata(streamShardHandle));
		assertEquals(streamShardHandle, KinesisDataFetcher.convertToStreamShardHandle(kinesisStreamShard));
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
						KinesisDataFetcher.createInitialSubscribedStreamsToLastDiscoveredShardsState(fakeStreams),
						FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(streamToShardCount));
				Whitebox.setInternalState(fetcher, "shardAssigner", allShardsSingleSubtaskFn); // override hashing
				List<StreamShardHandle> shards = fetcher.discoverNewShardsToSubscribe();
				fetcher.shutdownFetcher();

				String msg = String.format("for hash=%d, subtask=%d", hash, subtaskIndex);
				if (j == i) {
					assertEquals(msg, shardCount, shards.size());
				} else {
					assertEquals(msg, 0, shards.size());
				}
			}

		}

	}

	@Test
	public void testIsThisSubtaskShouldSubscribeTo() {
		assertTrue(KinesisDataFetcher.isThisSubtaskShouldSubscribeTo(0, 2, 0));
		assertFalse(KinesisDataFetcher.isThisSubtaskShouldSubscribeTo(1, 2, 0));
		assertTrue(KinesisDataFetcher.isThisSubtaskShouldSubscribeTo(2, 2, 0));
		assertFalse(KinesisDataFetcher.isThisSubtaskShouldSubscribeTo(0, 2, 1));
		assertTrue(KinesisDataFetcher.isThisSubtaskShouldSubscribeTo(1, 2, 1));
		assertFalse(KinesisDataFetcher.isThisSubtaskShouldSubscribeTo(2, 2, 1));
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
				new KinesisDeserializationSchemaWrapper<>(new org.apache.flink.streaming.util.serialization.SimpleStringSchema()),
				1,
				1,
				new AtomicReference<>(),
				new LinkedList<>(),
				subscribedStreamsToLastSeenShardIdsUnderTest,
				FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(new HashMap<>())) {

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
					KinesisDataFetcher.convertToStreamShardMetadata(shardHandle), shardHandle, seq));

		StreamRecord<String> record1 =
			new StreamRecord<>(String.valueOf(Long.MIN_VALUE), Long.MIN_VALUE);
		fetcher.emitRecordAndUpdateState(record1.getValue(), record1.getTimestamp(), shardIndex, seq);
		Assert.assertEquals(record1, sourceContext.getCollectedOutputs().poll());

		fetcher.emitWatermark();
		Assert.assertTrue("potential watermark equals previous watermark", watermarks.isEmpty());

		StreamRecord<String> record2 = new StreamRecord<>(String.valueOf(1), 1);
		fetcher.emitRecordAndUpdateState(record2.getValue(), record2.getTimestamp(), shardIndex, seq);
		Assert.assertEquals(record2, sourceContext.getCollectedOutputs().poll());

		fetcher.emitWatermark();
		Assert.assertFalse("watermark advanced", watermarks.isEmpty());
		Assert.assertEquals(new Watermark(record2.getTimestamp()), watermarks.remove(0));
		Assert.assertFalse("not idle", isTemporaryIdle.booleanValue());

		// test idle timeout
		long idleTimeout = 10;
		// advance clock idleTimeout
		clock.add(idleTimeout + 1);
		fetcher.emitWatermark();
		Assert.assertFalse("not idle", isTemporaryIdle.booleanValue());
		Assert.assertTrue("not idle, no new watermark", watermarks.isEmpty());

		// activate idle timeout
		Whitebox.setInternalState(fetcher, "shardIdleIntervalMillis", idleTimeout);
		fetcher.emitWatermark();
		Assert.assertTrue("idle", isTemporaryIdle.booleanValue());
		Assert.assertTrue("idle, no watermark", watermarks.isEmpty());
	}

	@Test
	public void testOriginalExceptionIsPreservedWhenInterruptedDuringShutdown() throws Exception {
		String stream = "fakeStream";

		Map<String, List<BlockingQueue<String>>> streamsToShardQueues = new HashMap<>();
		LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(10);
		queue.put("item1");
		streamsToShardQueues.put(stream, singletonList(queue));

		AlwaysThrowsDeserializationSchema deserializationSchema = new AlwaysThrowsDeserializationSchema();
		KinesisProxyInterface fakeKinesis =
			FakeKinesisBehavioursFactory.blockingQueueGetRecords(streamsToShardQueues);

		TestableKinesisDataFetcherForShardConsumerException<String> fetcher = new TestableKinesisDataFetcherForShardConsumerException<>(
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
			(sequence, properties, metricGroup, streamShardHandle) -> mock(RecordPublisher.class));

		DummyFlinkKinesisConsumer<String> consumer = new DummyFlinkKinesisConsumer<>(
			TestUtils.getStandardProperties(), fetcher, 1, 0);

		CheckedThread consumerThread = new CheckedThread("FlinkKinesisConsumer") {
			@Override
			public void go() throws Exception {
				consumer.run(new TestSourceContext<>());
			}
		};
		consumerThread.start();
		fetcher.waitUntilRun();

		// ShardConsumer exception (from deserializer) will result in fetcher being shut down.
		fetcher.waitUntilShutdown(20, TimeUnit.SECONDS);

		// Ensure that KinesisDataFetcher has exited its while(running) loop and is inside its awaitTermination()
		// method before we interrupt its thread, so that our interrupt doesn't get absorbed by any other mechanism.
		fetcher.waitUntilAwaitTermination(20, TimeUnit.SECONDS);

		// Interrupt the thread so that KinesisDataFetcher#awaitTermination() will throw InterruptedException.
		consumerThread.interrupt();

		try {
			consumerThread.sync();
		} catch (InterruptedException e) {
			fail("Expected exception from deserializer, but got InterruptedException, probably from " +
				"KinesisDataFetcher, which obscures the cause of the failure. " + e);
		} catch (RuntimeException e) {
			if (!e.getMessage().equals(AlwaysThrowsDeserializationSchema.EXCEPTION_MESSAGE)) {
				fail("Expected exception from deserializer, but got: " + e);
			}
		} catch (Exception e) {
			fail("Expected exception from deserializer, but got: " + e);
		}

		assertTrue("Expected Fetcher to have been interrupted. This test didn't accomplish its goal.",
			fetcher.wasInterrupted);
	}

	@Test
	public void testRecordPublisherFactoryIsTornDown() {
		Properties config = TestUtils.getStandardProperties();
		config.setProperty(RECORD_PUBLISHER_TYPE, EFO.name());

		KinesisProxyV2Interface kinesisV2 = mock(KinesisProxyV2Interface.class);

		TestableKinesisDataFetcher<String> fetcher =
			new TestableKinesisDataFetcher<String>(
				singletonList("fakeStream1"),
				new TestSourceContext<>(),
				config,
				new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
				10,
				2,
				new AtomicReference<>(),
				new LinkedList<>(),
				new HashMap<>(),
				mock(KinesisProxyInterface.class),
				kinesisV2) {
			};

		fetcher.shutdownFetcher();

		verify(kinesisV2).close();
	}

}
