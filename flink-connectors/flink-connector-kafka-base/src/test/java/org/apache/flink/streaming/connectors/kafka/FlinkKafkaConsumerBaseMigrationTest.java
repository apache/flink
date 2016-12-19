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
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorTest;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.util.SerializedValue;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.Serializable;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyMapOf;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Tests for checking whether {@link FlinkKafkaConsumerBase} can restore from snapshots that were
 * done using the Flink 1.1 {@link FlinkKafkaConsumerBase}.
 *
 * <p>For regenerating the binary snapshot file you have to run the commented out portion
 * of each test on a checkout of the Flink 1.1 branch.
 */
public class FlinkKafkaConsumerBaseMigrationTest {

	private static String getResourceFilename(String filename) {
		ClassLoader cl = WindowOperatorTest.class.getClassLoader();
		URL resource = cl.getResource(filename);
		if (resource == null) {
			throw new NullPointerException("Missing snapshot resource.");
		}
		return resource.getFile();
	}

	@Test
	public void testRestoreFromFlink11WithEmptyStateNoPartitions() throws Exception {
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
		testHarness.initializeStateFromLegacyCheckpoint(
			getResourceFilename("kafka-consumer-migration-test-flink1.1-snapshot-empty-state"));
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

		consumerOperator.cancel();
		runner.interrupt();
		runner.join();

		Assert.assertNull(error[0]);
	}

	@Test
	public void testRestoreFromFlink11WithEmptyStateWithPartitions() throws Exception {
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

		final List<KafkaTopicPartition> partitions = new ArrayList<>();
		partitions.add(new KafkaTopicPartition("abc", 13));
		partitions.add(new KafkaTopicPartition("def", 7));

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
		testHarness.initializeStateFromLegacyCheckpoint(
			getResourceFilename("kafka-consumer-migration-test-flink1.1-snapshot-empty-state"));
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
							Assert.fail("This should never be called.");
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

		Assert.assertNull(error[0]);
	}

	@Test
	public void testRestoreFromFlink11() throws Exception {
		// --------------------------------------------------------------------
		//   prepare fake states
		// --------------------------------------------------------------------

		final HashMap<KafkaTopicPartition, Long> state1 = new HashMap<>();
		state1.put(new KafkaTopicPartition("abc", 13), 16768L);
		state1.put(new KafkaTopicPartition("def", 7), 987654321L);

		final OneShotLatch latch = new OneShotLatch();
		final AbstractFetcher<String, ?> fetcher = mock(AbstractFetcher.class);

		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				Map<KafkaTopicPartition, Long> map = (HashMap<KafkaTopicPartition, Long>) invocationOnMock.getArguments()[0];

				latch.trigger();
				assertEquals(state1, map);
				return null;
			}
		}).when(fetcher).restoreOffsets(anyMapOf(KafkaTopicPartition.class, Long.class));


		final List<KafkaTopicPartition> partitions = new ArrayList<>();
		partitions.add(new KafkaTopicPartition("abc", 13));
		partitions.add(new KafkaTopicPartition("def", 7));

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
		testHarness.initializeStateFromLegacyCheckpoint(
			getResourceFilename("kafka-consumer-migration-test-flink1.1-snapshot"));
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
							//latch.trigger();
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

		Assert.assertNull(error[0]);
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

/*
	THE CODE FOR FLINK 1.1

	@Test
	public void testRestoreFromFlink11() throws Exception {
		// --------------------------------------------------------------------
		//   prepare fake states
		// --------------------------------------------------------------------

		final HashMap<KafkaTopicPartition, Long> state1 = new HashMap<>();
		state1.put(new KafkaTopicPartition("abc", 13), 16768L);
		state1.put(new KafkaTopicPartition("def", 7), 987654321L);

		final OneShotLatch latch = new OneShotLatch();
		final AbstractFetcher<String, ?> fetcher = mock(AbstractFetcher.class);

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				latch.trigger();
				return null;
			}
		}).when(fetcher).runFetchLoop();

		when(fetcher.snapshotCurrentState()).thenReturn(state1);

		final DummyFlinkKafkaConsumer<String> consumerFunction = new DummyFlinkKafkaConsumer<>(
			new FetcherFactory<String>() {
				private static final long serialVersionUID = -2803131905656983619L;

				@Override
				public AbstractFetcher<String, ?> createFetcher() {
					return fetcher;
				}
			});

		StreamSource<String, DummyFlinkKafkaConsumer<String>> consumerOperator =
			new StreamSource<>(consumerFunction);

		final OneInputStreamOperatorTestHarness<Void, String> testHarness =
			new OneInputStreamOperatorTestHarness<>(consumerOperator);

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

		StreamTaskState snapshot = testHarness.snapshot(0L, 0L);
		testHarness.snaphotToFile(snapshot, "src/test/resources/kafka-consumer-migration-test-flink1.1-snapshot-2");
		consumerOperator.run(new Object());

		consumerOperator.close();
		runner.join();

		System.out.println("Killed");
	}

	private static abstract class DummySourceContext
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

		@SuppressWarnings("unchecked")
		public DummyFlinkKafkaConsumer(FetcherFactory<T> fetcherFactory) {
			super((KeyedDeserializationSchema< T >) mock(KeyedDeserializationSchema.class));

			final List<KafkaTopicPartition> partitions = new ArrayList<>();
			partitions.add(new KafkaTopicPartition("dummy-topic", 0));
			setSubscribedPartitions(partitions);

			this.fetcherFactory = fetcherFactory;
		}

		@Override
		protected AbstractFetcher<T, ?> createFetcher(SourceContext<T> sourceContext, List<KafkaTopicPartition> thisSubtaskPartitions, SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic, SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated, StreamingRuntimeContext runtimeContext) throws Exception {
			return fetcherFactory.createFetcher();
		}
	}
* */
