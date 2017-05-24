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
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.internals.KinesisDataFetcher;
import org.apache.flink.streaming.connectors.kinesis.model.KinesisStreamShard;
import org.apache.flink.streaming.connectors.kinesis.model.SequenceNumber;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardMetadata;
import org.apache.flink.streaming.connectors.kinesis.serialization.KinesisDeserializationSchema;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisShardIdGenerator;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;

import com.amazonaws.services.kinesis.model.Shard;
import org.junit.Test;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;

/**
 * Tests for checking whether {@link FlinkKinesisConsumer} can restore from snapshots that were
 * done using the Flink 1.1 {@code FlinkKinesisConsumer}.
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
			getResourceFilename("kinesis-consumer-migration-test-flink1.1-empty-snapshot"));
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
		final HashMap<StreamShardMetadata, SequenceNumber> expectedState = new HashMap<>();
		expectedState.put(KinesisStreamShard.convertToStreamShardMetadata(new KinesisStreamShard("fakeStream1",
				new Shard().withShardId(KinesisShardIdGenerator.generateFromShardOrder(0)))),
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
		protected KinesisDataFetcher<T> createFetcher(
				List<String> streams,
				SourceFunction.SourceContext<T> sourceContext,
				RuntimeContext runtimeContext,
				Properties configProps,
				KinesisDeserializationSchema<T> deserializationSchema) {
			return mock(KinesisDataFetcher.class);
		}
	}
}
