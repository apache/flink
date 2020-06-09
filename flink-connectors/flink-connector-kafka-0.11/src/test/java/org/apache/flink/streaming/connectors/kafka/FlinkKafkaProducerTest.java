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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link FlinkKafkaProducer011}.
 */
public class FlinkKafkaProducerTest {
	@Test
	public void testOpenProducer() throws Exception {

		OpenTestingSerializationSchema schema = new OpenTestingSerializationSchema();
		FlinkKafkaProducer011<Integer> kafkaProducer = new FlinkKafkaProducer011<>(
			"localhost:9092",
			"test-topic",
			schema
		);

		OneInputStreamOperatorTestHarness<Integer, Object> testHarness = new OneInputStreamOperatorTestHarness<>(
			new StreamSink<>(kafkaProducer),
			1,
			1,
			0,
			IntSerializer.INSTANCE,
			new OperatorID(1, 1));

		testHarness.open();

		assertThat(schema.openCalled, equalTo(true));
	}

	private static class OpenTestingSerializationSchema implements SerializationSchema<Integer> {
		private boolean openCalled;

		@Override
		public void open(InitializationContext context) throws Exception {
			openCalled = true;
		}

		@Override
		public byte[] serialize(Integer element) {
			return new byte[0];
		}
	}
}
