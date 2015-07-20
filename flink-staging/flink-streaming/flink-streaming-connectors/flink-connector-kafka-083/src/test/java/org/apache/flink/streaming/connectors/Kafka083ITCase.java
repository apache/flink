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
package org.apache.flink.streaming.connectors;

import org.apache.flink.streaming.connectors.internals.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.junit.Ignore;

import java.util.Arrays;
import java.util.Properties;


public class Kafka083ITCase extends KafkaTestBase {
	@Override
	<T> FlinkKafkaConsumerBase<T> getConsumer(String topic, DeserializationSchema deserializationSchema, Properties props) {
		return new TestFlinkKafkaConsumer083<T>(topic, deserializationSchema, props);
	}

	@Override
	long[] getFinalOffsets() {
		return TestFlinkKafkaConsumer083.finalOffset;
	}

	@Override
	void resetOffsets() {
		TestFlinkKafkaConsumer083.finalOffset = null;
	}


	public static class TestFlinkKafkaConsumer083<OUT> extends FlinkKafkaConsumer083<OUT> {
		public static long[] finalOffset;
		public TestFlinkKafkaConsumer083(String topicName, DeserializationSchema<OUT> deserializationSchema, Properties consumerConfig) {
			super(topicName, deserializationSchema, consumerConfig);
		}

		@Override
		public void close() throws Exception {
			super.close();
			synchronized (commitedOffsets) {
				LOG.info("Setting final offset from "+ Arrays.toString(commitedOffsets));
				if (finalOffset == null) {
					finalOffset = new long[commitedOffsets.length];
				}
				for(int i = 0; i < commitedOffsets.length; i++) {
					if(commitedOffsets[i] > 0) {
						if(finalOffset[i] > 0) {
							throw new RuntimeException("This is unexpected on i = "+i);
						}
						finalOffset[i] = commitedOffsets[i];
					}
				}
			}
		}
	}

	@Ignore
	@Override
	public void brokerFailureTest() throws Exception {
		// Skipping test: The test is committing the offsets to the Kafka Broker.
		// only 0.8.3 brokers support that.
		return;
	}

	@Ignore
	@Override
	public void testFlinkKafkaConsumerWithOffsetUpdates() throws Exception {
		// Skipping test (see above)
		return;
	}
}
