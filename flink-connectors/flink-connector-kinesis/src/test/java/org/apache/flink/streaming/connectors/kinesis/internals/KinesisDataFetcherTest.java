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
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardMetadata;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisBehavioursFactory;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisShardIdGenerator;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestableKinesisDataFetcher;

import com.amazonaws.services.kinesis.model.HashKeyRange;
import com.amazonaws.services.kinesis.model.SequenceNumberRange;
import com.amazonaws.services.kinesis.model.Shard;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link KinesisDataFetcher}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(TestableKinesisDataFetcher.class)
public class KinesisDataFetcherTest {

	@Test(expected = RuntimeException.class)
	public void testIfNoShardsAreFoundShouldThrowException() throws Exception {
		List<String> fakeStreams = new LinkedList<>();
		fakeStreams.add("fakeStream1");
		fakeStreams.add("fakeStream2");

		HashMap<String, String> subscribedStreamsToLastSeenShardIdsUnderTest =
			KinesisDataFetcher.createInitialSubscribedStreamsToLastDiscoveredShardsState(fakeStreams);

		TestableKinesisDataFetcher fetcher =
			new TestableKinesisDataFetcher(
				fakeStreams,
				new Properties(),
				10,
				2,
				new AtomicReference<Throwable>(),
				new LinkedList<KinesisStreamShardState>(),
				subscribedStreamsToLastSeenShardIdsUnderTest,
				FakeKinesisBehavioursFactory.noShardsFoundForRequestedStreamsBehaviour());

		fetcher.runFetcher(); // this should throw RuntimeException
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

		final TestableKinesisDataFetcher fetcher =
			new TestableKinesisDataFetcher(
				fakeStreams,
				new Properties(),
				10,
				2,
				new AtomicReference<Throwable>(),
				new LinkedList<KinesisStreamShardState>(),
				subscribedStreamsToLastSeenShardIdsUnderTest,
				FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(streamToShardCount));

		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		final DummyFlinkKafkaConsumer<String> consumer = new DummyFlinkKafkaConsumer<>(testConfig, fetcher);

		PowerMockito.whenNew(ShardConsumer.class).withAnyArguments().thenReturn(Mockito.mock(ShardConsumer.class));
		Thread consumerThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					consumer.run(mock(SourceFunction.SourceContext.class));
				} catch (Exception e) {
					//
				}
			}
		});
		consumerThread.start();

		fetcher.waitUntilRun();
		consumer.cancel();
		consumerThread.join();

		// assert that the streams tracked in the state are identical to the subscribed streams
		Set<String> streamsInState = subscribedStreamsToLastSeenShardIdsUnderTest.keySet();
		assertTrue(streamsInState.size() == fakeStreams.size());
		assertTrue(streamsInState.containsAll(fakeStreams));

		// assert that the last seen shards in state is correctly set
		for (Map.Entry<String, String> streamToLastSeenShard : subscribedStreamsToLastSeenShardIdsUnderTest.entrySet()) {
			assertTrue(
				streamToLastSeenShard.getValue().equals(
					KinesisShardIdGenerator.generateFromShardOrder(streamToShardCount.get(streamToLastSeenShard.getKey()) - 1)));
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

		final TestableKinesisDataFetcher fetcher =
			new TestableKinesisDataFetcher(
				fakeStreams,
				new Properties(),
				10,
				2,
				new AtomicReference<Throwable>(),
				new LinkedList<KinesisStreamShardState>(),
				subscribedStreamsToLastSeenShardIdsUnderTest,
				FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(streamToShardCount));

		for (Map.Entry<StreamShardHandle, String> restoredState : restoredStateUnderTest.entrySet()) {
			fetcher.advanceLastDiscoveredShardOfStream(restoredState.getKey().getStreamName(), restoredState.getKey().getShard().getShardId());
			fetcher.registerNewSubscribedShardState(
				new KinesisStreamShardState(KinesisDataFetcher.convertToStreamShardMetadata(restoredState.getKey()),
					restoredState.getKey(), new SequenceNumber(restoredState.getValue())));
		}

		PowerMockito.whenNew(ShardConsumer.class).withAnyArguments().thenReturn(Mockito.mock(ShardConsumer.class));
		Thread runFetcherThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					fetcher.runFetcher();
				} catch (Exception e) {
					//
				}
			}
		});
		runFetcherThread.start();
		Thread.sleep(1000); // sleep a while before closing
		fetcher.shutdownFetcher();

		// assert that the streams tracked in the state are identical to the subscribed streams
		Set<String> streamsInState = subscribedStreamsToLastSeenShardIdsUnderTest.keySet();
		assertTrue(streamsInState.size() == fakeStreams.size());
		assertTrue(streamsInState.containsAll(fakeStreams));

		// assert that the last seen shards in state is correctly set
		for (Map.Entry<String, String> streamToLastSeenShard : subscribedStreamsToLastSeenShardIdsUnderTest.entrySet()) {
			assertTrue(
				streamToLastSeenShard.getValue().equals(
					KinesisShardIdGenerator.generateFromShardOrder(streamToShardCount.get(streamToLastSeenShard.getKey()) - 1)));
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
		final TestableKinesisDataFetcher fetcher =
			new TestableKinesisDataFetcher(
				fakeStreams,
				new Properties(),
				10,
				2,
				new AtomicReference<Throwable>(),
				new LinkedList<KinesisStreamShardState>(),
				subscribedStreamsToLastSeenShardIdsUnderTest,
				FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(streamToShardCount));

		for (Map.Entry<StreamShardHandle, String> restoredState : restoredStateUnderTest.entrySet()) {
			fetcher.advanceLastDiscoveredShardOfStream(restoredState.getKey().getStreamName(), restoredState.getKey().getShard().getShardId());
			fetcher.registerNewSubscribedShardState(
				new KinesisStreamShardState(KinesisDataFetcher.convertToStreamShardMetadata(restoredState.getKey()),
					restoredState.getKey(), new SequenceNumber(restoredState.getValue())));
		}

		PowerMockito.whenNew(ShardConsumer.class).withAnyArguments().thenReturn(Mockito.mock(ShardConsumer.class));
		Thread runFetcherThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					fetcher.runFetcher();
				} catch (Exception e) {
					//
				}
			}
		});
		runFetcherThread.start();
		Thread.sleep(1000); // sleep a while before closing
		fetcher.shutdownFetcher();

		// assert that the streams tracked in the state are identical to the subscribed streams
		Set<String> streamsInState = subscribedStreamsToLastSeenShardIdsUnderTest.keySet();
		assertTrue(streamsInState.size() == fakeStreams.size());
		assertTrue(streamsInState.containsAll(fakeStreams));

		// assert that the last seen shards in state is correctly set
		for (Map.Entry<String, String> streamToLastSeenShard : subscribedStreamsToLastSeenShardIdsUnderTest.entrySet()) {
			assertTrue(
				streamToLastSeenShard.getValue().equals(
					KinesisShardIdGenerator.generateFromShardOrder(streamToShardCount.get(streamToLastSeenShard.getKey()) - 1)));
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
		final TestableKinesisDataFetcher fetcher =
			new TestableKinesisDataFetcher(
				fakeStreams,
				new Properties(),
				10,
				2,
				new AtomicReference<Throwable>(),
				new LinkedList<KinesisStreamShardState>(),
				subscribedStreamsToLastSeenShardIdsUnderTest,
				FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(streamToShardCount));

		for (Map.Entry<StreamShardHandle, String> restoredState : restoredStateUnderTest.entrySet()) {
			fetcher.advanceLastDiscoveredShardOfStream(restoredState.getKey().getStreamName(), restoredState.getKey().getShard().getShardId());
			fetcher.registerNewSubscribedShardState(
				new KinesisStreamShardState(KinesisDataFetcher.convertToStreamShardMetadata(restoredState.getKey()),
					restoredState.getKey(), new SequenceNumber(restoredState.getValue())));
		}

		PowerMockito.whenNew(ShardConsumer.class).withAnyArguments().thenReturn(Mockito.mock(ShardConsumer.class));
		Thread runFetcherThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					fetcher.runFetcher();
				} catch (Exception e) {
					//
				}
			}
		});
		runFetcherThread.start();
		Thread.sleep(1000); // sleep a while before closing
		fetcher.shutdownFetcher();

		// assert that the streams tracked in the state are identical to the subscribed streams
		Set<String> streamsInState = subscribedStreamsToLastSeenShardIdsUnderTest.keySet();
		assertTrue(streamsInState.size() == fakeStreams.size());
		assertTrue(streamsInState.containsAll(fakeStreams));

		// assert that the last seen shards in state is correctly set
		assertTrue(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream1").equals(
			KinesisShardIdGenerator.generateFromShardOrder(2)));
		assertTrue(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream2").equals(
			KinesisShardIdGenerator.generateFromShardOrder(1)));
		assertTrue(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream3") == null);
		assertTrue(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream4") == null);
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
		final TestableKinesisDataFetcher fetcher =
			new TestableKinesisDataFetcher(
				fakeStreams,
				new Properties(),
				10,
				2,
				new AtomicReference<Throwable>(),
				new LinkedList<KinesisStreamShardState>(),
				subscribedStreamsToLastSeenShardIdsUnderTest,
				FakeKinesisBehavioursFactory.nonReshardedStreamsBehaviour(streamToShardCount));

		for (Map.Entry<StreamShardHandle, String> restoredState : restoredStateUnderTest.entrySet()) {
			fetcher.advanceLastDiscoveredShardOfStream(restoredState.getKey().getStreamName(), restoredState.getKey().getShard().getShardId());
			fetcher.registerNewSubscribedShardState(
				new KinesisStreamShardState(KinesisDataFetcher.convertToStreamShardMetadata(restoredState.getKey()),
					restoredState.getKey(), new SequenceNumber(restoredState.getValue())));
		}

		PowerMockito.whenNew(ShardConsumer.class).withAnyArguments().thenReturn(Mockito.mock(ShardConsumer.class));
		Thread runFetcherThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					fetcher.runFetcher();
				} catch (Exception e) {
					//
				}
			}
		});
		runFetcherThread.start();
		Thread.sleep(1000); // sleep a while before closing
		fetcher.shutdownFetcher();

		// assert that the streams tracked in the state are identical to the subscribed streams
		Set<String> streamsInState = subscribedStreamsToLastSeenShardIdsUnderTest.keySet();
		assertTrue(streamsInState.size() == fakeStreams.size());
		assertTrue(streamsInState.containsAll(fakeStreams));

		// assert that the last seen shards in state is correctly set
		assertTrue(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream1").equals(
			KinesisShardIdGenerator.generateFromShardOrder(3)));
		assertTrue(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream2").equals(
			KinesisShardIdGenerator.generateFromShardOrder(4)));
		assertTrue(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream3") == null);
		assertTrue(subscribedStreamsToLastSeenShardIdsUnderTest.get("fakeStream4") == null);
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

	private static class DummyFlinkKafkaConsumer<T> extends FlinkKinesisConsumer<T> {
		private static final long serialVersionUID = 1L;

		private KinesisDataFetcher<T> fetcher;

		@SuppressWarnings("unchecked")
		DummyFlinkKafkaConsumer(Properties properties, KinesisDataFetcher<T> fetcher) {
			super("test", mock(KinesisDeserializationSchema.class), properties);
			this.fetcher = fetcher;
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
			when(context.getIndexOfThisSubtask()).thenReturn(0);
			when(context.getNumberOfParallelSubtasks()).thenReturn(1);
			return context;
		}
	}
}
