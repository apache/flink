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

import com.amazonaws.services.kinesis.model.Shard;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisShardIdGenerator;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.junit.Test;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests for checking whether {@link FlinkKinesisConsumer} can restore from snapshots that were
 * done using the Flink 1.1 {@link FlinkKinesisConsumer}.
 *
 * <p>For regenerating the binary snapshot file you have to run the commented out portion
 * of each test on a checkout of the Flink 1.1 branch.
 */
public class FlinkKinesisConsumerMigrationTest {

	@Test
	public void testRestoreFromFlink11WithEmptyState() throws Exception {
		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		final DummyFlinkKafkaConsumer<String> consumerFunction = new DummyFlinkKafkaConsumer<>(testConfig);

		StreamSource<String, DummyFlinkKafkaConsumer<String>> consumerOperator = new StreamSource<>(consumerFunction);

		final AbstractStreamOperatorTestHarness<String> testHarness =
			new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

		testHarness.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		testHarness.setup();
		// restore state from binary snapshot file using legacy method
		testHarness.initializeStateFromLegacyCheckpoint(
			getResourceFilename("kinesis-consumer-migration-test-flink1.1-snapshot-empty"));
		testHarness.open();

		// assert that no state was restored
		assertEquals(null, consumerFunction.getRestoredState());

		consumerOperator.close();
		consumerOperator.cancel();
	}

	@Test
	public void testRestoreFromFlink11() throws Exception {
		Properties testConfig = new Properties();
		testConfig.setProperty(ConsumerConfigConstants.AWS_REGION, "us-east-1");
		testConfig.setProperty(ConsumerConfigConstants.AWS_CREDENTIALS_PROVIDER, "BASIC");
		testConfig.setProperty(ConsumerConfigConstants.AWS_ACCESS_KEY_ID, "accessKeyId");
		testConfig.setProperty(ConsumerConfigConstants.AWS_SECRET_ACCESS_KEY, "secretKey");

		final DummyFlinkKafkaConsumer<String> consumerFunction = new DummyFlinkKafkaConsumer<>(testConfig);

		StreamSource<String, DummyFlinkKafkaConsumer<String>> consumerOperator =
			new StreamSource<>(consumerFunction);

		final AbstractStreamOperatorTestHarness<String> testHarness =
			new AbstractStreamOperatorTestHarness<>(consumerOperator, 1, 1, 0);

		testHarness.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		testHarness.setup();
		// restore state from binary snapshot file using legacy method
		testHarness.initializeStateFromLegacyCheckpoint(
			getResourceFilename("kinesis-consumer-migration-test-flink1.1-snapshot"));
		testHarness.open();

		// the expected state in "kafka-consumer-migration-test-flink1.1-snapshot"
		final HashMap<KinesisStreamShard, SequenceNumber> expectedState = new HashMap<>();
		expectedState.put(new KinesisStreamShard("fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			new SequenceNumber("987654321"));

		// assert that state is correctly restored from legacy checkpoint
		assertNotEquals(null, consumerFunction.getRestoredState());
		assertEquals(1, consumerFunction.getRestoredState().size());
		assertEquals(expectedState, consumerFunction.getRestoredState());

		consumerOperator.close();
		consumerOperator.cancel();
	}

	// ------------------------------------------------------------------------

	private static String getResourceFilename(String filename) {
		ClassLoader cl = FlinkKinesisConsumerMigrationTest.class.getClassLoader();
		URL resource = cl.getResource(filename);
		if (resource == null) {
			throw new NullPointerException("Missing snapshot resource.");
		}
		return resource.getFile();
	}

	private static class DummyFlinkKafkaConsumer<T> extends FlinkKinesisConsumer<T> {
		private static final long serialVersionUID = 1L;

		@SuppressWarnings("unchecked")
		DummyFlinkKafkaConsumer(Properties properties) {
			super("test", mock(KinesisDeserializationSchema.class), properties);
		}

		@Override
		protected KinesisDataFetcher<T> createFetcher(List<String> streams,
													  	SourceFunction.SourceContext<T> sourceContext,
													  	RuntimeContext runtimeContext,
													  	Properties configProps,
													  	KinesisDeserializationSchema<T> deserializationSchema) {
			return mock(KinesisDataFetcher.class);
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
		final HashMap<KinesisStreamShard, SequenceNumber> state1 = new HashMap<>();
		state1.put(new KinesisStreamShard("fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0))),
			new SequenceNumber("987654321"));

		final OneShotLatch latch = new OneShotLatch();
		final KinesisDataFetcher<String> fetcher = mock(KinesisDataFetcher.class);

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				latch.trigger();
				return null;
			}
		}).when(fetcher).runFetcher();

		when(fetcher.snapshotState()).thenReturn(state1);

		// assume the given config is correct
		PowerMockito.mockStatic(KinesisConfigUtil.class);
		PowerMockito.doNothing().when(KinesisConfigUtil.class);

		final DummyFlinkKinesisConsumer consumerFunction = new DummyFlinkKinesisConsumer(
			new FetcherFactory() {
				private static final long serialVersionUID = -2803131905656983619L;
				@Override
				public KinesisDataFetcher<String> createFetcher() {
					return fetcher;
				}
			}, new Properties());

		StreamSource<String, DummyFlinkKinesisConsumer> consumerOperator =
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
		testHarness.snaphotToFile(snapshot, "src/test/resources/kinesis-consumer-migration-test-flink1.1-snapshot");
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

	private interface FetcherFactory extends Serializable {
		KinesisDataFetcher createFetcher();
	}

	public static class DummyFlinkKinesisConsumer extends TestableFlinkKinesisConsumer {
		private static final long serialVersionUID = 1L;

		private final FetcherFactory fetcherFactory;

		public DummyFlinkKinesisConsumer(FetcherFactory fetcherFactory, Properties properties) {
			super("fakeStream", properties, 1, 0);
			this.fetcherFactory = fetcherFactory;
		}

		@Override
		@SuppressWarnings("unchecked")
		protected KinesisDataFetcher createFetcher(List<String> streams,
												   	SourceFunction.SourceContext<String> sourceContext,
												   	RuntimeContext runtimeContext,
												   	Properties configProps,
												   	KinesisDeserializationSchema<String> deserializationSchema) {
			return fetcherFactory.createFetcher();
		}
	}
*/

