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
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardMetadata;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisShardIdGenerator;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OperatorSnapshotUtil;
import org.apache.flink.streaming.util.migration.MigrationTestUtil;
import org.apache.flink.streaming.util.migration.MigrationVersion;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

	private static final HashMap<StreamShardMetadata, SequenceNumber> TEST_STATE = new HashMap<>();
	static {
		StreamShardMetadata shardMetadata = new StreamShardMetadata();
		shardMetadata.setStreamName("fakeStream1");
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
		final DummyFlinkKinesisConsumer<String> consumerFunction = new DummyFlinkKinesisConsumer<>(mock(KinesisDataFetcher.class));

		StreamSource<String, DummyFlinkKinesisConsumer<String>> consumerOperator = new StreamSource<>(consumerFunction);

		final AbstractStreamOperatorTestHarness<String> testHarness =
			new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

		testHarness.setup();
		MigrationTestUtil.restoreFromSnapshot(
			testHarness,
			"src/test/resources/kinesis-consumer-migration-test-flink" + testMigrateVersion + "-empty-snapshot", testMigrateVersion);
		testHarness.open();

		// assert that no state was restored
		assertTrue(consumerFunction.getRestoredState().isEmpty());

		consumerOperator.close();
		consumerOperator.cancel();
	}

	@Test
	public void testRestore() throws Exception {
		final DummyFlinkKinesisConsumer<String> consumerFunction = new DummyFlinkKinesisConsumer<>(mock(KinesisDataFetcher.class));

		StreamSource<String, DummyFlinkKinesisConsumer<String>> consumerOperator =
			new StreamSource<>(consumerFunction);

		final AbstractStreamOperatorTestHarness<String> testHarness =
			new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

		testHarness.setup();
		MigrationTestUtil.restoreFromSnapshot(
			testHarness,
			"src/test/resources/kinesis-consumer-migration-test-flink" + testMigrateVersion + "-snapshot", testMigrateVersion);
		testHarness.open();

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
		final OneShotLatch latch = new OneShotLatch();

		final KinesisDataFetcher<String> fetcher = mock(KinesisDataFetcher.class);
		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				latch.trigger();
				return null;
			}
		}).when(fetcher).runFetcher();
		when(fetcher.snapshotState()).thenReturn(state);

		final DummyFlinkKinesisConsumer<String> consumer = new DummyFlinkKinesisConsumer<>(fetcher);

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
					consumer.run(mock(SourceFunction.SourceContext.class));
				} catch (Throwable t) {
					t.printStackTrace();
					error.set(t);
				}
			}
		};
		runner.start();

		if (!latch.isTriggered()) {
			latch.await();
		}

		final OperatorStateHandles snapshot;
		synchronized (testHarness.getCheckpointLock()) {
			snapshot = testHarness.snapshot(0L, 0L);
		}

		OperatorSnapshotUtil.writeStateHandle(snapshot, path);

		consumerOperator.close();
		runner.join();
	}

	private static class DummyFlinkKinesisConsumer<T> extends FlinkKinesisConsumer<T> {

		private KinesisDataFetcher<T> mockFetcher;

		private static Properties dummyConfig = new Properties();
		static {
			dummyConfig.setProperty(AWSConfigConstants.AWS_REGION, "us-east-1");
			dummyConfig.setProperty(AWSConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
			dummyConfig.setProperty(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");
		}

		DummyFlinkKinesisConsumer(KinesisDataFetcher<T> mockFetcher) {
			super("dummy-topic", mock(KinesisDeserializationSchema.class), dummyConfig);
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
}
