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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;

/**
 * Migration test for the FlinkKafkaProducer across operators.
 */
public class FlinkKafkaProducerMigrationOperatorTest extends KafkaTestBase {

	private static String topic = "flink-kafka-producer-operator-migration-test";

	protected TypeInformationSerializationSchema<Integer> integerSerializationSchema =
		new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());
	protected KeyedSerializationSchema<Integer> integerKeyedSerializationSchema =
		new KeyedSerializationSchemaWrapper<>(integerSerializationSchema);

	@Test
	public void migrationFromFlinkKafkaProducer011() throws Exception{

		FlinkKafkaProducer011<Integer> kafkaProducer011 = new FlinkKafkaProducer011<Integer>(
			topic,
			integerKeyedSerializationSchema,
			createProperties(),
			FlinkKafkaProducer011.Semantic.EXACTLY_ONCE
		);

		OneInputStreamOperatorTestHarness testHarness011 = new OneInputStreamOperatorTestHarness<>(
			new StreamSink<>(kafkaProducer011),
			1,
			1,
			0,
			IntSerializer.INSTANCE,
			new OperatorID(1, 1)
		);
		testHarness011.setup();
		testHarness011.open();
		// produce element with the FlinkKafkaProducer011.
		testHarness011.processElement(32, 0L);
		testHarness011.processElement(33, 1L);
		testHarness011.snapshot(0L, 2L);
		testHarness011.notifyOfCompletedCheckpoint(0L);

		testHarness011.processElement(34, 3L);
		OperatorSubtaskState savepoint = testHarness011.snapshot(1L, 4L);

		// Create an other testHarness with a universal FlinkKafkaProducer.
		OneInputStreamOperatorTestHarness testHarness = createTestHarness();

		testHarness.setup();
		// Restore from savepoint.
		testHarness.initializeState(savepoint);
		testHarness.open();

		// Produce new element with the FlinkKafkaProducer.
		testHarness.processElement(35, 4L);
		testHarness.snapshot(2L, 5L);
		testHarness.notifyOfCompletedCheckpoint(2L);

		// We should have:
		// 32 and 33 committed.
		// 34 pending transaction aborted.
		// 35 committed.
		assertExactlyOnceForTopic(createProperties(), topic, 0, Arrays.asList(32, 33, 35));
		testHarness011.close();
		testHarness.close();
	}

	private Properties createProperties() {
		Properties properties = new Properties();
		properties.putAll(standardProps);
		properties.putAll(secureProps);
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-client-id");
		properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "producer-transaction-id");
		properties.put(FlinkKafkaProducer.KEY_DISABLE_METRICS, "true");
		return properties;
	}

	private OneInputStreamOperatorTestHarness<Integer, Object> createTestHarness() throws Exception {

		FlinkKafkaProducer<Integer> kafkaProducer = new FlinkKafkaProducer<>(
			topic,
			integerKeyedSerializationSchema,
			createProperties(),
			FlinkKafkaProducer.Semantic.EXACTLY_ONCE
		);

		return new OneInputStreamOperatorTestHarness<>(
			new StreamSink<>(kafkaProducer),
			1,
			1,
			0,
			IntSerializer.INSTANCE,
			new OperatorID(1, 1));
	}
}
