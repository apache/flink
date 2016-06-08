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

import com.amazonaws.services.kinesis.model.Shard;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShardState;
import org.apache.flink.streaming.connectors.kinesis.testutils.FakeKinesisBehavioursFactory;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisShardIdGenerator;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestableKinesisDataFetcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertTrue;

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

		fetcher.setIsRestoringFromFailure(false); // not restoring

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

		Map<String,Integer> streamToShardCount = new HashMap<>();
		Random rand = new Random();
		for (String fakeStream : fakeStreams) {
			streamToShardCount.put(fakeStream, rand.nextInt(5)+1);
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

		fetcher.setIsRestoringFromFailure(false);

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
		for (Map.Entry<String,String> streamToLastSeenShard : subscribedStreamsToLastSeenShardIdsUnderTest.entrySet()) {
			assertTrue(
				streamToLastSeenShard.getValue().equals(
					KinesisShardIdGenerator.generateFromShardOrder(streamToShardCount.get(streamToLastSeenShard.getKey())-1)));
		}
	}

	@Test
	public void testStreamToLastSeenShardStateIsCorrectlySetWhenNoNewShardsSinceRestoredCheckpoint() throws Exception {
		List<String> fakeStreams = new LinkedList<>();
		fakeStreams.add("fakeStream1");
		fakeStreams.add("fakeStream2");

		Map<KinesisStreamShard, String> restoredStateUnderTest = new HashMap<>();

		// fakeStream1 has 3 shards before restore
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))),
			UUID.randomUUID().toString());

		// fakeStream2 has 2 shards before restore
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
			UUID.randomUUID().toString());

		Map<String,Integer> streamToShardCount = new HashMap<>();
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

		for (Map.Entry<KinesisStreamShard, String> restoredState : restoredStateUnderTest.entrySet()) {
			fetcher.advanceLastDiscoveredShardOfStream(restoredState.getKey().getStreamName(), restoredState.getKey().getShard().getShardId());
			fetcher.registerNewSubscribedShardState(
				new KinesisStreamShardState(restoredState.getKey(), new SequenceNumber(restoredState.getValue())));
		}

		fetcher.setIsRestoringFromFailure(true);

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
		for (Map.Entry<String,String> streamToLastSeenShard : subscribedStreamsToLastSeenShardIdsUnderTest.entrySet()) {
			assertTrue(
				streamToLastSeenShard.getValue().equals(
					KinesisShardIdGenerator.generateFromShardOrder(streamToShardCount.get(streamToLastSeenShard.getKey())-1)));
		}
	}

	@Test
	public void testStreamToLastSeenShardStateIsCorrectlySetWhenNewShardsFoundSinceRestoredCheckpoint() throws Exception {
		List<String> fakeStreams = new LinkedList<>();
		fakeStreams.add("fakeStream1");
		fakeStreams.add("fakeStream2");

		Map<KinesisStreamShard, String> restoredStateUnderTest = new HashMap<>();

		// fakeStream1 has 3 shards before restore
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))),
			UUID.randomUUID().toString());

		// fakeStream2 has 2 shards before restore
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
			UUID.randomUUID().toString());

		Map<String,Integer> streamToShardCount = new HashMap<>();
		streamToShardCount.put("fakeStream1", 3+1); // fakeStream1 had 3 shards before & 1 new shard after restore
		streamToShardCount.put("fakeStream2", 2+3); // fakeStream2 had 2 shards before & 3 new shard after restore

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

		for (Map.Entry<KinesisStreamShard, String> restoredState : restoredStateUnderTest.entrySet()) {
			fetcher.advanceLastDiscoveredShardOfStream(restoredState.getKey().getStreamName(), restoredState.getKey().getShard().getShardId());
			fetcher.registerNewSubscribedShardState(
				new KinesisStreamShardState(restoredState.getKey(), new SequenceNumber(restoredState.getValue())));
		}

		fetcher.setIsRestoringFromFailure(true);

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
		for (Map.Entry<String,String> streamToLastSeenShard : subscribedStreamsToLastSeenShardIdsUnderTest.entrySet()) {
			assertTrue(
				streamToLastSeenShard.getValue().equals(
					KinesisShardIdGenerator.generateFromShardOrder(streamToShardCount.get(streamToLastSeenShard.getKey())-1)));
		}
	}

	@Test
	public void testStreamToLastSeenShardStateIsCorrectlySetWhenNoNewShardsSinceRestoredCheckpointAndSomeStreamsDoNotExist() throws Exception {
		List<String> fakeStreams = new LinkedList<>();
		fakeStreams.add("fakeStream1");
		fakeStreams.add("fakeStream2");
		fakeStreams.add("fakeStream3"); // fakeStream3 will not have any shards
		fakeStreams.add("fakeStream4"); // fakeStream4 will not have any shards

		Map<KinesisStreamShard, String> restoredStateUnderTest = new HashMap<>();

		// fakeStream1 has 3 shards before restore
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))),
			UUID.randomUUID().toString());

		// fakeStream2 has 2 shards before restore
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
			UUID.randomUUID().toString());

		Map<String,Integer> streamToShardCount = new HashMap<>();
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

		for (Map.Entry<KinesisStreamShard, String> restoredState : restoredStateUnderTest.entrySet()) {
			fetcher.advanceLastDiscoveredShardOfStream(restoredState.getKey().getStreamName(), restoredState.getKey().getShard().getShardId());
			fetcher.registerNewSubscribedShardState(
				new KinesisStreamShardState(restoredState.getKey(), new SequenceNumber(restoredState.getValue())));
		}

		fetcher.setIsRestoringFromFailure(true);

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

		Map<KinesisStreamShard, String> restoredStateUnderTest = new HashMap<>();

		// fakeStream1 has 3 shards before restore
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(2))),
			UUID.randomUUID().toString());

		// fakeStream2 has 2 shards before restore
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			UUID.randomUUID().toString());
		restoredStateUnderTest.put(
			new KinesisStreamShard(
				"fakeStream2",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(1))),
			UUID.randomUUID().toString());

		Map<String,Integer> streamToShardCount = new HashMap<>();
		streamToShardCount.put("fakeStream1", 3+1); // fakeStream1 had 3 shards before & 1 new shard after restore
		streamToShardCount.put("fakeStream2", 2+3); // fakeStream2 had 2 shards before & 2 new shard after restore
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

		for (Map.Entry<KinesisStreamShard, String> restoredState : restoredStateUnderTest.entrySet()) {
			fetcher.advanceLastDiscoveredShardOfStream(restoredState.getKey().getStreamName(), restoredState.getKey().getShard().getShardId());
			fetcher.registerNewSubscribedShardState(
				new KinesisStreamShardState(restoredState.getKey(), new SequenceNumber(restoredState.getValue())));
		}

		fetcher.setIsRestoringFromFailure(true);

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
}
