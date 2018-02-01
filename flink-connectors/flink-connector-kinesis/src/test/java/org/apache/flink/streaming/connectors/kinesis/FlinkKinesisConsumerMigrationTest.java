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

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardMetadata;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisShardIdGenerator;
import org.apache.flink.streaming.connectors.kinesis.testutils.TestSourceContext;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OperatorSnapshotUtil;
import org.apache.flink.streaming.util.migration.MigrationTestUtil;
import org.apache.flink.streaming.util.migration.MigrationVersion;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import com.amazonaws.services.kinesis.model.Shard;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for checking whether {@link FlinkKinesisConsumer} can restore from snapshots that were
 * done using an older {@code FlinkKinesisConsumer}.
 *
 * <p>For regenerating the binary snapshot files run {@link #writeSnapshot()} on the corresponding
 * Flink release-* branch.
 */
@RunWith(Parameterized.class)
public class FlinkKinesisConsumerMigrationTest {

	/**
	 * TODO change this to the corresponding savepoint version to be written (e.g. {@link MigrationVersion#v1_3} for 1.3)
	 * TODO and remove all @Ignore annotations on the writeSnapshot() method to generate savepoints
	 */
	private final MigrationVersion flinkGenerateSavepointVersion = null;

	private static final String TEST_STREAM_NAME = "fakeStream1";

	private static final HashMap<StreamShardMetadata, SequenceNumber> TEST_STATE = new HashMap<>();
	static {
		StreamShardMetadata shardMetadata = new StreamShardMetadata();
		shardMetadata.setStreamName(TEST_STREAM_NAME);
		shardMetadata.setShardId(KinesisShardIdGenerator.generateFromShardOrder(0));

		TEST_STATE.put(shardMetadata, new SequenceNumber("987654321"));
	}

	private final MigrationVersion testMigrateVersion;

	@Parameterized.Parameters(name = "Migration Savepoint: {0}")
	public static Collection<MigrationVersion> parameters () {
		return Arrays.asList(MigrationVersion.v1_3);
	}

	public FlinkKinesisConsumerMigrationTest(MigrationVersion testMigrateVersion) {
		this.testMigrateVersion = testMigrateVersion;
	}

	/**
	 * Manually run this to write binary snapshot data.
	 */
	@Ignore
	@Test
	public void writeSnapshot() throws Exception {
		writeSnapshot("src/test/resources/kinesis-consumer-migration-test-flink" + flinkGenerateSavepointVersion + "-snapshot", TEST_STATE);

		// write empty state snapshot
		writeSnapshot("src/test/resources/kinesis-consumer-migration-test-flink" + flinkGenerateSavepointVersion + "-empty-snapshot", new HashMap<>());
	}

	@Test
	public void testRestoreWithEmptyState() throws Exception {
		final List<StreamShardHandle> initialDiscoveryShards = new ArrayList<>(TEST_STATE.size());
		for (StreamShardMetadata shardMetadata : TEST_STATE.keySet()) {
			Shard shard = new Shard();
			shard.setShardId(shardMetadata.getShardId());

			initialDiscoveryShards.add(new StreamShardHandle(shardMetadata.getStreamName(), shard));
		}

		final TestFetcher<String> fetcher = new TestFetcher<>(
			Collections.singletonList(TEST_STREAM_NAME),
			new TestSourceContext<>(),
			getMockRuntimeContext(1, 0),
			getStandardProperties(),
			new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
			null,
			initialDiscoveryShards);

		final DummyFlinkKinesisConsumer<String> consumerFunction = new DummyFlinkKinesisConsumer<>(
			fetcher, new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()));

		StreamSource<String, DummyFlinkKinesisConsumer<String>> consumerOperator = new StreamSource<>(consumerFunction);

		final AbstractStreamOperatorTestHarness<String> testHarness =
			new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

		testHarness.setup();
		MigrationTestUtil.restoreFromSnapshot(
			testHarness,
			"src/test/resources/kinesis-consumer-migration-test-flink" + testMigrateVersion + "-empty-snapshot", testMigrateVersion);
		testHarness.open();

		consumerFunction.run(new TestSourceContext<>());

		// assert that no state was restored
		assertTrue(consumerFunction.getRestoredState().isEmpty());

		consumerOperator.close();
		consumerOperator.cancel();
	}

	@Test
	public void testRestore() throws Exception {
		final List<StreamShardHandle> initialDiscoveryShards = new ArrayList<>(TEST_STATE.size());
		for (StreamShardMetadata shardMetadata : TEST_STATE.keySet()) {
			Shard shard = new Shard();
			shard.setShardId(shardMetadata.getShardId());

			initialDiscoveryShards.add(new StreamShardHandle(shardMetadata.getStreamName(), shard));
		}

		final TestFetcher<String> fetcher = new TestFetcher<>(
			Collections.singletonList(TEST_STREAM_NAME),
			new TestSourceContext<>(),
			getMockRuntimeContext(1, 0),
			getStandardProperties(),
			new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
			null,
			initialDiscoveryShards);

		final DummyFlinkKinesisConsumer<String> consumerFunction = new DummyFlinkKinesisConsumer<>(
			fetcher, new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()));

		StreamSource<String, DummyFlinkKinesisConsumer<String>> consumerOperator =
			new StreamSource<>(consumerFunction);

		final AbstractStreamOperatorTestHarness<String> testHarness =
			new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

		testHarness.setup();
		MigrationTestUtil.restoreFromSnapshot(
			testHarness,
			"src/test/resources/kinesis-consumer-migration-test-flink" + testMigrateVersion + "-snapshot", testMigrateVersion);
		testHarness.open();

		consumerFunction.run(new TestSourceContext<>());

		// assert that state is correctly restored
		assertNotEquals(null, consumerFunction.getRestoredState());
		assertEquals(1, consumerFunction.getRestoredState().size());
		assertEquals(TEST_STATE, consumerFunction.getRestoredState());

		consumerOperator.close();
		consumerOperator.cancel();
	}

	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private void writeSnapshot(String path, HashMap<StreamShardMetadata, SequenceNumber> state) throws Exception {
		final TestFetcher<String> fetcher = new TestFetcher<>(
			Collections.singletonList(TEST_STREAM_NAME),
			new TestSourceContext<>(),
			getMockRuntimeContext(1, 0),
			new Properties(),
			new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()),
			state,
			null);

		final DummyFlinkKinesisConsumer<String> consumer = new DummyFlinkKinesisConsumer<>(
			fetcher, new KinesisDeserializationSchemaWrapper<>(new SimpleStringSchema()));

		StreamSource<String, DummyFlinkKinesisConsumer<String>> consumerOperator = new StreamSource<>(consumer);

		final AbstractStreamOperatorTestHarness<String> testHarness =
				new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

		testHarness.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		testHarness.setup();
		testHarness.open();

		final AtomicReference<Throwable> error = new AtomicReference<>();

		// run the source asynchronously
		Thread runner = new Thread() {
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

		final OperatorStateHandles snapshot;
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

		private static Properties dummyConfig = new Properties();
		static {
			dummyConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
			dummyConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
			dummyConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		}

		DummyFlinkKinesisConsumer(KinesisDataFetcher<T> mockFetcher, KinesisDeserializationSchema<T> schema) {
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

			super(streams, sourceContext, runtimeContext, configProps, deserializationSchema);

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

	private static Properties getStandardProperties() {
		Properties config = new Properties();
		config.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
		config.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		config.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		return config;
	}

	private static RuntimeContext getMockRuntimeContext(int numSubtasks, int thisSubtaskIndex) {
		RuntimeContext runtimeContext = Mockito.mock(RuntimeContext.class);

		Mockito.when(runtimeContext.getNumberOfParallelSubtasks()).thenReturn(numSubtasks);
		Mockito.when(runtimeContext.getNumberOfParallelSubtasks()).thenReturn(thisSubtaskIndex);

		return runtimeContext;
	}
}
