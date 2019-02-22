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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * IT cases for Kafka 0.9 .
 */
public class Kafka09ITCase extends KafkaConsumerTestBase {

	// ------------------------------------------------------------------------
	//  Suite of Tests
	// ------------------------------------------------------------------------

	@Test(timeout = 60000)
	public void testFailOnNoBroker() throws Exception {
		runFailOnNoBrokerTest();
	}

	@Test(timeout = 60000)
	public void testConcurrentProducerConsumerTopology() throws Exception {
		runSimpleConcurrentProducerConsumerTopology();
	}

	@Test(timeout = 60000)
	public void testKeyValueSupport() throws Exception {
		runKeyValueTest();
	}

	// --- canceling / failures ---

	@Test(timeout = 60000)
	public void testCancelingEmptyTopic() throws Exception {
		runCancelingOnEmptyInputTest();
	}

	@Test(timeout = 60000)
	public void testCancelingFullTopic() throws Exception {
		runCancelingOnFullInputTest();
	}

	// --- source to partition mappings and exactly once ---

	@Test(timeout = 60000)
	public void testOneToOneSources() throws Exception {
		runOneToOneExactlyOnceTest();
	}

	@Test(timeout = 60000)
	public void testOneSourceMultiplePartitions() throws Exception {
		runOneSourceMultiplePartitionsExactlyOnceTest();
	}

	@Test(timeout = 60000)
	public void testMultipleSourcesOnePartition() throws Exception {
		runMultipleSourcesOnePartitionExactlyOnceTest();
	}

	// --- broker failure ---

	@Test(timeout = 60000)
	public void testBrokerFailure() throws Exception {
		runBrokerFailureTest();
	}

	// --- special executions ---

	@Test(timeout = 60000)
	public void testBigRecordJob() throws Exception {
		runBigRecordTestTopology();
	}

	@Test(timeout = 60000)
	public void testMultipleTopics() throws Exception {
		runProduceConsumeMultipleTopics(true);
	}

	@Test(timeout = 60000)
	public void testAllDeletes() throws Exception {
		runAllDeletesTest();
	}

	@Test(timeout = 60000)
	public void testEndOfStream() throws Exception {
		runEndOfStreamTest();
	}

	@Test(timeout = 60000)
	public void testMetrics() throws Throwable {
		runMetricsTest();
	}

	// --- startup mode ---

	@Test(timeout = 60000)
	public void testStartFromEarliestOffsets() throws Exception {
		runStartFromEarliestOffsets();
	}

	@Test(timeout = 60000)
	public void testStartFromLatestOffsets() throws Exception {
		runStartFromLatestOffsets();
	}

	@Test(timeout = 60000)
	public void testStartFromGroupOffsets() throws Exception {
		runStartFromGroupOffsets();
	}

	@Test(timeout = 60000)
	public void testStartFromSpecificOffsets() throws Exception {
		runStartFromSpecificOffsets();
	}

	// --- offset committing ---

	@Test(timeout = 60000)
	public void testCommitOffsetsToKafka() throws Exception {
		runCommitOffsetsToKafka();
	}

	@Test(timeout = 60000)
	public void testAutoOffsetRetrievalAndCommitToKafka() throws Exception {
		runAutoOffsetRetrievalAndCommitToKafka();
	}

	/**
	 * Kafka09 specific RateLimiter test. This test produces 100 bytes of data to a test topic
	 * and then runs a job with {@link FlinkKafkaConsumer09} as the source and a {@link GuavaFlinkConnectorRateLimiter} with
	 * a desired rate of 3 bytes / second. Based on the execution time, the test asserts that this rate was not surpassed.
	 * If no rate limiter is set on the consumer, the test should fail.
	 */
	@Test(timeout = 60000)
	public void testRateLimitedConsumer() throws Exception {
		final String testTopic = "testRateLimitedConsumer";
		createTestTopic(testTopic, 3, 1);

		// ---------- Produce a stream into Kafka -------------------

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env.getConfig().disableSysoutLogging();

		DataStream<String> stream = env.addSource(new SourceFunction<String>() {
			private static final long serialVersionUID = 1L;
			boolean running = true;

			@Override
			public void run(SourceContext<String> ctx) {
				long i = 0;
				while (running) {
					byte[] data = new byte[] {1};
					synchronized (ctx.getCheckpointLock()) {
						ctx.collect(new String(data)); // 1 byte
					}
					if (i++ == 100L) {
						running = false;
					}
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		});

		Properties producerProperties = new Properties();
		producerProperties.putAll(standardProps);
		producerProperties.putAll(secureProps);
		producerProperties.put("retries", 3);

		stream.addSink(new FlinkKafkaProducer09<>(testTopic, new SimpleStringSchema(), producerProperties));
		env.execute("Produce 100 bytes of data to test topic");

		// ---------- Consumer from Kafka in a ratelimited way -----------

		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env.getConfig().disableSysoutLogging();

		// ---------- RateLimiter config -------------
		final long globalRate = 10; // bytes/second
		FlinkKafkaConsumer09<String> consumer09 = new FlinkKafkaConsumer09<>(testTopic,
			new StringDeserializer(globalRate), standardProps);
		FlinkConnectorRateLimiter rateLimiter = new GuavaFlinkConnectorRateLimiter();
		rateLimiter.setRate(globalRate);
		consumer09.setRateLimiter(rateLimiter);

		DataStream<String> stream1 = env.addSource(consumer09);
		stream1.addSink(new DiscardingSink<>());
		env.execute("Consume 100 bytes of data from test topic");

		// ------- Assertions --------------
		Assert.assertNotNull(consumer09.getRateLimiter());
		Assert.assertEquals(globalRate, consumer09.getRateLimiter().getRate());

		deleteTestTopic(testTopic);
	}

	private static class StringDeserializer implements KeyedDeserializationSchema<String> {

		private static final long serialVersionUID = 1L;
		private final TypeInformation<String> ti;
		private final TypeSerializer<String> ser;
		long cnt = 0;
		long startTime;
		long endTime;
		long globalRate;

		public StringDeserializer(long rate) {
			this.ti = Types.STRING;
			this.ser = ti.createSerializer(new ExecutionConfig());
			this.startTime = System.currentTimeMillis();
			this.globalRate = rate;
		}

		@Override
		public TypeInformation<String> getProducedType() {
			return ti;
		}

		@Override
		public String deserialize(byte[] messageKey, byte[] message, String topic, int partition,
			long offset) throws IOException {
			cnt++;
			DataInputView in = new DataInputViewStreamWrapper(new ByteArrayInputStream(message));
			String e = ser.deserialize(in);
			return e;
		}

		@Override
		public boolean isEndOfStream(String nextElement) {
			if (cnt > 100L) {
				endTime = System.currentTimeMillis();
				// Approximate bytes/second read based on job execution time.
				long bytesPerSecond = 100 * 1000L / (endTime - startTime);
				Assert.assertTrue(bytesPerSecond > 0);
				Assert.assertTrue(bytesPerSecond <= globalRate);
				return true;
			}
			return false;
		}
	}
}
