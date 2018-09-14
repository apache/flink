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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;

/**
 * IT cases for Kafka 0.11 .
 */
public class Kafka011ITCase extends KafkaConsumerTestBase {

	@BeforeClass
	public static void prepare() throws ClassNotFoundException {
		KafkaProducerTestBase.prepare();
		((KafkaTestEnvironmentImpl) kafkaServer).setProducerSemantic(FlinkKafkaProducer011.Semantic.AT_LEAST_ONCE);
	}

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
		runProduceConsumeMultipleTopics();
	}

	@Test(timeout = 60000)
	public void testAllDeletes() throws Exception {
		runAllDeletesTest();
	}

	@Test(timeout = 60000)
	public void testMetricsAndEndOfStream() throws Exception {
		runEndOfStreamTest();
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

	@Test(timeout = 60000)
	public void testStartFromTimestamp() throws Exception {
		runStartFromTimestamp();
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
	 * Kafka 0.11 specific test, ensuring Timestamps are properly written to and read from Kafka.
	 */
	@Test(timeout = 60000)
	public void testTimestamps() throws Exception {

		final String topic = "tstopic";
		createTestTopic(topic, 3, 1);

		// ---------- Produce an event time stream into Kafka -------------------

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env.getConfig().disableSysoutLogging();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Long> streamWithTimestamps = env.addSource(new SourceFunction<Long>() {
			private static final long serialVersionUID = -2255115836471289626L;
			boolean running = true;

			@Override
			public void run(SourceContext<Long> ctx) throws Exception {
				long i = 0;
				while (running) {
					ctx.collectWithTimestamp(i, i * 2);
					if (i++ == 1110L) {
						running = false;
					}
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		});

		final TypeInformationSerializationSchema<Long> longSer = new TypeInformationSerializationSchema<>(Types.LONG, env.getConfig());
		FlinkKafkaProducer011<Long> prod = new FlinkKafkaProducer011<>(topic, new KeyedSerializationSchemaWrapper<>(longSer), standardProps, Optional.of(new FlinkKafkaPartitioner<Long>() {
			private static final long serialVersionUID = -6730989584364230617L;

			@Override
			public int partition(Long next, byte[] key, byte[] value, String targetTopic, int[] partitions) {
				return (int) (next % 3);
			}
		}));
		prod.setWriteTimestampToKafka(true);

		streamWithTimestamps.addSink(prod).setParallelism(3);

		env.execute("Produce some");

		// ---------- Consume stream from Kafka -------------------

		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env.getConfig().disableSysoutLogging();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		FlinkKafkaConsumer011<Long> kafkaSource = new FlinkKafkaConsumer011<>(topic, new LimitedLongDeserializer(), standardProps);
		kafkaSource.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Long>() {
			private static final long serialVersionUID = -4834111173247835189L;

			@Nullable
			@Override
			public Watermark checkAndGetNextWatermark(Long lastElement, long extractedTimestamp) {
				if (lastElement % 11 == 0) {
					return new Watermark(lastElement);
				}
				return null;
			}

			@Override
			public long extractTimestamp(Long element, long previousElementTimestamp) {
				return previousElementTimestamp;
			}
		});

		DataStream<Long> stream = env.addSource(kafkaSource);
		GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
		stream.transform("timestamp validating operator", objectTypeInfo, new TimestampValidatingOperator()).setParallelism(1);

		env.execute("Consume again");

		deleteTestTopic(topic);
	}

	private static class TimestampValidatingOperator extends StreamSink<Long> {

		private static final long serialVersionUID = 1353168781235526806L;

		public TimestampValidatingOperator() {
			super(new SinkFunction<Long>() {
				private static final long serialVersionUID = -6676565693361786524L;

				@Override
				public void invoke(Long value) throws Exception {
					throw new RuntimeException("Unexpected");
				}
			});
		}

		long elCount = 0;
		long wmCount = 0;
		long lastWM = Long.MIN_VALUE;

		@Override
		public void processElement(StreamRecord<Long> element) throws Exception {
			elCount++;
			if (element.getValue() * 2 != element.getTimestamp()) {
				throw new RuntimeException("Invalid timestamp: " + element);
			}
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			wmCount++;

			if (lastWM <= mark.getTimestamp()) {
				lastWM = mark.getTimestamp();
			} else {
				throw new RuntimeException("Received watermark higher than the last one");
			}

			if (mark.getTimestamp() % 11 != 0 && mark.getTimestamp() != Long.MAX_VALUE) {
				throw new RuntimeException("Invalid watermark: " + mark.getTimestamp());
			}
		}

		@Override
		public void close() throws Exception {
			super.close();
			if (elCount != 1110L) {
				throw new RuntimeException("Wrong final element count " + elCount);
			}

			if (wmCount <= 2) {
				throw new RuntimeException("Almost no watermarks have been sent " + wmCount);
			}
		}
	}

	private static class LimitedLongDeserializer implements KeyedDeserializationSchema<Long> {

		private static final long serialVersionUID = 6966177118923713521L;
		private final TypeInformation<Long> ti;
		private final TypeSerializer<Long> ser;
		long cnt = 0;

		public LimitedLongDeserializer() {
			this.ti = Types.LONG;
			this.ser = ti.createSerializer(new ExecutionConfig());
		}

		@Override
		public TypeInformation<Long> getProducedType() {
			return ti;
		}

		@Override
		public Long deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) throws IOException {
			cnt++;
			DataInputView in = new DataInputViewStreamWrapper(new ByteArrayInputStream(message));
			Long e = ser.deserialize(in);
			return e;
		}

		@Override
		public boolean isEndOfStream(Long nextElement) {
			return cnt > 1110L;
		}
	}

}
