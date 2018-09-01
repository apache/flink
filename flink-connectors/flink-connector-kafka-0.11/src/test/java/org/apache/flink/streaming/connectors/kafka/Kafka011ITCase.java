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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
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
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import org.apache.flink.shaded.guava18.com.google.common.primitives.Longs;

import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

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
		public Long deserialize(Record record) throws IOException {
			cnt++;
			DataInputView in = new DataInputViewStreamWrapper(new ByteArrayInputStream(record.value()));
			Long e = ser.deserialize(in);
			return e;
		}

		@Override
		public boolean isEndOfStream(Long nextElement) {
			return cnt > 1110L;
		}
	}

	/**
	 * Kafka 0.11 specific test, ensuring Kafka Headers are properly written to and read from Kafka.
	 */
	@Test(timeout = 60000)
	public void testHeaders() throws Exception {
		final String topic = "headers-topic";
		final long testSequenceLength = 127L;
		createTestTopic(topic, 3, 1);
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env.getConfig().disableSysoutLogging();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Long> testSequence = env.addSource(new SourceFunction<Long>() {
			private static final long serialVersionUID = 1L;
			boolean running = true;

			@Override
			public void run(SourceContext<Long> ctx) throws Exception {
				long i = 0;
				while (running) {
					ctx.collectWithTimestamp(i, i * 2);
					if (i++ == testSequenceLength) {
						running = false;
					}
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		});

		FlinkKafkaProducer011<Long> producer = new FlinkKafkaProducer011<>(topic,
			new TestHeadersKeyedSerializationSchema(topic), standardProps, Optional.empty());
		testSequence.addSink(producer).setParallelism(3);
		env.execute("Produce some data");

		// Now let's consume data and check that headers deserialized correctly
		env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
		env.getConfig().disableSysoutLogging();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

		FlinkKafkaConsumer011<TestHeadersElement> kafkaSource = new FlinkKafkaConsumer011<>(topic, new TestHeadersKeyedDeserializationSchema(testSequenceLength), standardProps);

		env.addSource(kafkaSource).addSink(new TestHeadersElementValid());
		env.execute("Consume again");

		deleteTestTopic(topic);
	}

	/**
	 * Element consisting of key, value and headers represented as list of tuples: key, list of Bytes.
	 */
	public static class TestHeadersElement extends Tuple3<Long, Byte, List<Tuple2<String, List<Byte>>>> {

	}

	/**
	 * Generate "headers" for given element.
	 * @param element - sequence element
	 * @return headers
	 */
	private static Iterable<Map.Entry<String, byte[]>> headersFor(Long element) {
		final long x = element;
		return Arrays.asList(
			new AbstractMap.SimpleImmutableEntry<>("low", new byte[]{
				(byte) ((x >>> 8) & 0xFF),
				(byte) ((x) & 0xFF)
			}),
			new AbstractMap.SimpleImmutableEntry<>("low", new byte[]{
				(byte) ((x >>> 24) & 0xFF),
				(byte) ((x >>> 16) & 0xFF)
			}),
			new AbstractMap.SimpleImmutableEntry<>("high", new byte[]{
				(byte) ((x >>> 40) & 0xFF),
				(byte) ((x >>> 32) & 0xFF)
			}),
			new AbstractMap.SimpleImmutableEntry<>("high", new byte[]{
				(byte) ((x >>> 56) & 0xFF),
				(byte) ((x >>> 48) & 0xFF)
			})
		);
	}

	/**
	 * Convert headers into list of tuples representation. List of tuples is more convenient to use in
	 * assert expressions, because they have equals
	 * @param headers - headers
	 * @return list of tuples(string, list of Bytes)
	 */
	private static List<Tuple2<String, List<Byte>>> headersAsList(Iterable<Map.Entry<String, byte[]>> headers) {
		List<Tuple2<String, List<Byte>>> r = new ArrayList<>();
		for (Map.Entry<String, byte[]> entry: headers) {
			final Tuple2<String, List<Byte>> t = new Tuple2<>();
			t.f0 = entry.getKey();
			t.f1 = new ArrayList<>(entry.getValue().length);
			for (byte b: entry.getValue()) {
				t.f1.add(b);
			}
			r.add(t);
		}
		return r;
	}

	/**
	 * Sink consuming TestHeadersElement, while consuming sink generates headers using
	 * message value and validates that headers generated from message
	 * are equal to headers in element, which were read from Kafka.
	 */
	private static class TestHeadersElementValid implements SinkFunction<TestHeadersElement> {
		private static final long serialVersionUID = 1L;
		@Override
		public void invoke(TestHeadersElement value, Context context) throws Exception {
			// calculate Headers from message
			final Iterable<Map.Entry<String, byte[]>> headers = headersFor(value.f0);
			final List<Tuple2<String, List<Byte>>> expected = headersAsList(headers);
			assertEquals(expected, value.f2);
		}
	}

	/**
	 * Serialization schema, which serialize given element as value, lowest element byte as key,
	 * low 32-bit integer is also stored as two "low" headers with 16-bit parts as headers values,
	 * and similar high 32-bit integer is stored as two "high" headers, each 16-bit part is "high" header value.
	 */
	private static class TestHeadersKeyedSerializationSchema implements KeyedSerializationSchema<Long> {
		private final String topic;

		TestHeadersKeyedSerializationSchema(String topic) {
			this.topic = Objects.requireNonNull(topic);
		}

		@Override
		public byte[] serializeKey(Long element) {
			return new byte[] { element.byteValue() };
		}

		@Override
		public byte[] serializeValue(Long data) {
			return data == null ? null : Longs.toByteArray(data);
		}

		@Override
		public String getTargetTopic(Long element) {
			return topic;
		}

		@Override
		public Iterable<Map.Entry<String, byte[]>> headers(Long element) {
			return headersFor(element);
		}
	}

	/**
	 * Deserialization schema for TestHeadersElement elements.
	 */
	private static class TestHeadersKeyedDeserializationSchema implements KeyedDeserializationSchema<TestHeadersElement> {
		private final long count;

		TestHeadersKeyedDeserializationSchema(long count){
			this.count = count;
		}

		@Override
		public TypeInformation<TestHeadersElement> getProducedType() {
			return TypeInformation.of(TestHeadersElement.class);
		}

		@Override
		public boolean isEndOfStream(TestHeadersElement nextElement) {
			return nextElement.f0 >= count;
		}

		@Override
		public TestHeadersElement deserialize(Record record) {
			final TestHeadersElement element = new TestHeadersElement();
			element.f0 = Longs.fromByteArray(record.value());
			element.f1 = record.key()[0];
			element.f2 = headersAsList(record.headers());
			return element;
		}
	}

}
