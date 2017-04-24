/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.kafka;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OperatorSnapshotUtil;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.SerializedValue;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Tests for checking whether {@link FlinkKafkaConsumerBase} can restore from snapshots that were
 * done using the Flink 1.2 {@link FlinkKafkaConsumerBase}.
 *
 * <p>For regenerating the binary snapshot files run {@link #writeSnapshot()} on the Flink 1.2
 * branch.
 */
public class FlinkKafkaConsumerBaseFrom12MigrationTest {

	final static HashMap<KafkaTopicPartition, Long> PARTITION_STATE = new HashMap<>();

	static {
		PARTITION_STATE.put(new KafkaTopicPartition("abc", 13), 16768L);
		PARTITION_STATE.put(new KafkaTopicPartition("def", 7), 987654321L);
	}

	/**
	 * Manually run this to write binary snapshot data.
	 */
	@Ignore
	@Test
	public void writeSnapshot() throws Exception {
		writeSnapshot("src/test/resources/kafka-consumer-migration-test-flink1.2-snapshot", PARTITION_STATE);

		final HashMap<KafkaTopicPartition, Long> emptyState = new HashMap<>();
		writeSnapshot("src/test/resources/kafka-consumer-migration-test-flink1.2-snapshot-empty-state", emptyState);
	}

	private void writeSnapshot(String path, HashMap<KafkaTopicPartition, Long> state) throws Exception {

		final OneShotLatch latch = new OneShotLatch();
		final AbstractFetcher<String, ?> fetcher = mock(AbstractFetcher.class);

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				latch.trigger();
				return null;
			}
		}).when(fetcher).runFetchLoop();

		when(fetcher.snapshotCurrentState()).thenReturn(state);

		final List<KafkaTopicPartition> partitions = new ArrayList<>(PARTITION_STATE.keySet());

		final DummyFlinkKafkaConsumer<String> consumerFunction = new DummyFlinkKafkaConsumer<>(
				new FetcherFactory<String>() {
					private static final long serialVersionUID = -2803131905656983619L;

					@Override
					public AbstractFetcher<String, ?> createFetcher() {
						return fetcher;
					}
				},
				partitions);

		StreamSource<String, DummyFlinkKafkaConsumer<String>> consumerOperator =
				new StreamSource<>(consumerFunction);


		final AbstractStreamOperatorTestHarness<String> testHarness =
				new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

		testHarness.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		testHarness.setup();
		testHarness.open();

		final Throwable[] error = new Throwable[1];

		// run the source asynchronously
		Thread runner = new Thread() {
			@Override
			public void run() {
				try {
					consumerFunction.run(new DummySourceContext() {
						@Override
						public void collect(String element) {
							latch.trigger();
						}
					});
				}
				catch (Throwable t) {
					t.printStackTrace();
					error[0] = t;
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

	@Test
	public void testRestoreWithEmptyStateNoPartitions() throws Exception {
		// --------------------------------------------------------------------
		//   prepare fake states
		// --------------------------------------------------------------------

		final OneShotLatch latch = new OneShotLatch();
		final AbstractFetcher<String, ?> fetcher = mock(AbstractFetcher.class);

		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				latch.trigger();
				Assert.fail("This should never be called");
				return null;
			}
		}).when(fetcher).restoreOffsets(anyMapOf(KafkaTopicPartition.class, Long.class));

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				latch.trigger();
				Assert.fail("This should never be called");
				return null;
			}
		}).when(fetcher).runFetchLoop();

		final DummyFlinkKafkaConsumer<String> consumerFunction = new DummyFlinkKafkaConsumer<>(
				new FetcherFactory<String>() {
					private static final long serialVersionUID = -2803131905656983619L;

					@Override
					public AbstractFetcher<String, ?> createFetcher() {
						return fetcher;
					}
				},
				Collections.<KafkaTopicPartition>emptyList());

		StreamSource<String, DummyFlinkKafkaConsumer<String>> consumerOperator =
			new StreamSource<>(consumerFunction);

		final AbstractStreamOperatorTestHarness<String> testHarness =
			new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

		testHarness.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		testHarness.setup();
		testHarness.initializeState(
				OperatorSnapshotUtil.readStateHandle(
						OperatorSnapshotUtil.getResourceFilename("kafka-consumer-migration-test-flink1.2-snapshot-empty-state")));
		testHarness.open();

		final Throwable[] error = new Throwable[1];

		// run the source asynchronously
		Thread runner = new Thread() {
			@Override
			public void run() {
				try {
					consumerFunction.run(new DummySourceContext() {
						@Override
						public void collect(String element) {
							latch.trigger();
							Assert.fail("This should never be called.");
						}

						@Override
						public void emitWatermark(Watermark mark) {
							latch.trigger();
							assertEquals(Long.MAX_VALUE, mark.getTimestamp());
						}
					});
				}
				catch (Throwable t) {
					t.printStackTrace();
					error[0] = t;
				}
			}
		};
		runner.start();

		if (!latch.isTriggered()) {
			latch.await(2, TimeUnit.MINUTES);
		}

		assertTrue("Latch was not triggered within the given timeout.", latch.isTriggered());

		consumerOperator.cancel();
		consumerOperator.close();

		runner.interrupt();
		runner.join();

		assertNull("Got error: " + error[0], error[0]);
	}

	@Test
	public void testRestoreWithEmptyStateWithPartitions() throws Exception {
		final OneShotLatch latch = new OneShotLatch();
		final AbstractFetcher<String, ?> fetcher = mock(AbstractFetcher.class);

		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				latch.trigger();
				Assert.fail("This should never be called");
				return null;
			}
		}).when(fetcher).restoreOffsets(anyMapOf(KafkaTopicPartition.class, Long.class));

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				latch.trigger();
				return null;
			}
		}).when(fetcher).runFetchLoop();

		final List<KafkaTopicPartition> partitions = new ArrayList<>(PARTITION_STATE.keySet());

		final DummyFlinkKafkaConsumer<String> consumerFunction = new DummyFlinkKafkaConsumer<>(
				new FetcherFactory<String>() {
					private static final long serialVersionUID = -2803131905656983619L;

					@Override
					public AbstractFetcher<String, ?> createFetcher() {
						return fetcher;
					}
				},
				partitions);

		StreamSource<String, DummyFlinkKafkaConsumer<String>> consumerOperator =
			new StreamSource<>(consumerFunction);

		final AbstractStreamOperatorTestHarness<String> testHarness =
			new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

		testHarness.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		testHarness.setup();
		testHarness.initializeState(
				OperatorSnapshotUtil.readStateHandle(
						OperatorSnapshotUtil.getResourceFilename("kafka-consumer-migration-test-flink1.2-snapshot-empty-state")));
		testHarness.open();

		final Throwable[] error = new Throwable[1];

		// run the source asynchronously
		Thread runner = new Thread() {
			@Override
			public void run() {
				try {
					consumerFunction.run(new DummySourceContext() {
						@Override
						public void collect(String element) {
							latch.trigger();
							Assert.fail("This should never be called.");
						}

						@Override
						public void emitWatermark(Watermark mark) {
							latch.trigger();
							assertEquals(Long.MAX_VALUE, mark.getTimestamp());
						}
					});
				}
				catch (Throwable t) {
					t.printStackTrace();
					error[0] = t;
				}
			}
		};
		runner.start();

		if (!latch.isTriggered()) {
			latch.await();
		}

		consumerOperator.close();
		runner.interrupt();
		runner.join();

		assertNull("Got error: " + error[0], error[0]);
	}

	@Test
	public void testRestore() throws Exception {
		// --------------------------------------------------------------------
		//   prepare fake states
		// --------------------------------------------------------------------

		final boolean[] verifiedState = new boolean[1];

		final OneShotLatch latch = new OneShotLatch();
		final AbstractFetcher<String, ?> fetcher = mock(AbstractFetcher.class);

		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				Map<KafkaTopicPartition, Long> map = (HashMap<KafkaTopicPartition, Long>) invocationOnMock.getArguments()[0];

				latch.trigger();
				assertEquals(PARTITION_STATE, map);
				verifiedState[0] = true;
				return null;
			}
		}).when(fetcher).restoreOffsets(anyMapOf(KafkaTopicPartition.class, Long.class));


		final List<KafkaTopicPartition> partitions = new ArrayList<>(PARTITION_STATE.keySet());

		final DummyFlinkKafkaConsumer<String> consumerFunction = new DummyFlinkKafkaConsumer<>(
				new FetcherFactory<String>() {
					private static final long serialVersionUID = -2803131905656983619L;

					@Override
					public AbstractFetcher<String, ?> createFetcher() {
						return fetcher;
					}
				},
				partitions);

		StreamSource<String, DummyFlinkKafkaConsumer<String>> consumerOperator =
			new StreamSource<>(consumerFunction);

		final AbstractStreamOperatorTestHarness<String> testHarness =
			new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

		testHarness.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		testHarness.setup();
		testHarness.initializeState(
				OperatorSnapshotUtil.readStateHandle(
						OperatorSnapshotUtil.getResourceFilename("kafka-consumer-migration-test-flink1.2-snapshot")));
		testHarness.open();

		final Throwable[] error = new Throwable[1];

		// run the source asynchronously
		Thread runner = new Thread() {
			@Override
			public void run() {
				try {
					consumerFunction.run(new DummySourceContext() {
						@Override
						public void collect(String element) {
						}
					});
				}
				catch (Throwable t) {
					t.printStackTrace();
					error[0] = t;
				}
			}
		};
		runner.start();

		if (!latch.isTriggered()) {
			latch.await();
		}

		consumerOperator.close();

		runner.join();

		assertNull("Got error: " + error[0], error[0]);

		assertTrue(verifiedState[0]);
	}

	private abstract static class DummySourceContext
		implements SourceFunction.SourceContext<String> {

		private final Object lock = new Object();

		@Override
		public void collectWithTimestamp(String element, long timestamp) {
		}

		@Override
		public void emitWatermark(Watermark mark) {
		}

		@Override
		public Object getCheckpointLock() {
			return lock;
		}

		@Override
		public void close() {
		}
	}

	// ------------------------------------------------------------------------

	private interface FetcherFactory<T> extends Serializable {
		AbstractFetcher<T, ?> createFetcher();
	}

	private static class DummyFlinkKafkaConsumer<T> extends FlinkKafkaConsumerBase<T> {
		private static final long serialVersionUID = 1L;

		private final FetcherFactory<T> fetcherFactory;

		private final List<KafkaTopicPartition> partitions;

		@SuppressWarnings("unchecked")
		DummyFlinkKafkaConsumer(
				FetcherFactory<T> fetcherFactory,
				List<KafkaTopicPartition> partitions) {
			super(Arrays.asList("dummy-topic"), (KeyedDeserializationSchema< T >) mock(KeyedDeserializationSchema.class));
			this.fetcherFactory = fetcherFactory;
			this.partitions = partitions;
		}

		@Override
		protected AbstractFetcher<T, ?> createFetcher(SourceContext<T> sourceContext, List<KafkaTopicPartition> thisSubtaskPartitions, SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic, SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated, StreamingRuntimeContext runtimeContext) throws Exception {
			return fetcherFactory.createFetcher();
		}

		@Override
		protected List<KafkaTopicPartition> getKafkaPartitions(List<String> topics) {
			return partitions;
		}
	}
}
