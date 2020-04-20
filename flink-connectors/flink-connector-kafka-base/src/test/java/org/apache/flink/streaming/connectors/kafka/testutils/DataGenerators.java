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

package org.apache.flink.streaming.connectors.kafka.testutils;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaTestEnvironment;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import java.util.Collection;
import java.util.Properties;
import java.util.Random;

/**
 * Test data generators.
 */
@SuppressWarnings("serial")
public class DataGenerators {

	public static void generateRandomizedIntegerSequence(
			StreamExecutionEnvironment env,
			KafkaTestEnvironment testServer, String topic,
			final int numPartitions,
			final int numElements,
			final boolean randomizeOrder) throws Exception {
		env.setParallelism(numPartitions);
				env.setRestartStrategy(RestartStrategies.noRestart());

		DataStream<Integer> stream = env.addSource(
				new RichParallelSourceFunction<Integer>() {

					private volatile boolean running = true;

					@Override
					public void run(SourceContext<Integer> ctx) {
						// create a sequence
						int[] elements = new int[numElements];
						for (int i = 0, val = getRuntimeContext().getIndexOfThisSubtask();
							i < numElements;
							i++, val += getRuntimeContext().getNumberOfParallelSubtasks()) {

							elements[i] = val;
						}

						// scramble the sequence
						if (randomizeOrder) {
							Random rnd = new Random();
							for (int i = 0; i < elements.length; i++) {
								int otherPos = rnd.nextInt(elements.length);

								int tmp = elements[i];
								elements[i] = elements[otherPos];
								elements[otherPos] = tmp;
							}
						}

						// emit the sequence
						int pos = 0;
						while (running && pos < elements.length) {
							ctx.collect(elements[pos++]);
						}
					}

					@Override
					public void cancel() {
						running = false;
					}
				});

		Properties props = new Properties();
		props.putAll(FlinkKafkaProducerBase.getPropertiesFromBrokerList(testServer.getBrokerConnectionString()));
		Properties secureProps = testServer.getSecureProperties();
		if (secureProps != null) {
			props.putAll(testServer.getSecureProperties());
		}
		// Ensure the producer enables idempotence.
		props.putAll(testServer.getIdempotentProducerConfig());

		stream = stream.rebalance();
		testServer.produceIntoKafka(stream, topic,
				new KeyedSerializationSchemaWrapper<>(new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, env.getConfig())),
				props,
				new FlinkKafkaPartitioner<Integer>() {
					@Override
					public int partition(Integer next, byte[] serializedKey, byte[] serializedValue, String topic, int[] partitions) {
						return next % partitions.length;
					}
				});

		env.execute("Scrambles int sequence generator");
	}

	// ------------------------------------------------------------------------

	/**
	 * A generator that continuously writes strings into the configured topic. The generation is stopped if an exception
	 * occurs or {@link #shutdown()} is called.
	 */
	public static class InfiniteStringsGenerator extends Thread {

		private final KafkaTestEnvironment server;

		private final String topic;

		private volatile Throwable error;

		private volatile boolean running = true;

		public InfiniteStringsGenerator(KafkaTestEnvironment server, String topic) {
			this.server = server;
			this.topic = topic;
		}

		@Override
		public void run() {
			// we manually feed data into the Kafka sink
			RichFunction producer = null;
			try {
				Properties producerProperties = FlinkKafkaProducerBase.getPropertiesFromBrokerList(server.getBrokerConnectionString());
				producerProperties.setProperty("retries", "3");
				Transformation<String> mockTransform = new MockTransformation();
				DataStream<String> stream = new DataStream<>(new DummyStreamExecutionEnvironment(), mockTransform);

				StreamSink<String> sink = server.getProducerSink(
						topic,
						new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()),
						producerProperties,
						new FlinkFixedPartitioner<String>());

				OneInputStreamOperatorTestHarness<String, Object> testHarness =
						new OneInputStreamOperatorTestHarness<>(sink);

				testHarness.open();

				final StringBuilder bld = new StringBuilder();
				final Random rnd = new Random();

				while (running) {
					bld.setLength(0);

					int len = rnd.nextInt(100) + 1;
					for (int i = 0; i < len; i++) {
						bld.append((char) (rnd.nextInt(20) + 'a'));
					}

					String next = bld.toString();
					testHarness.processElement(new StreamRecord<>(next));
				}
			}
			catch (Throwable t) {
				this.error = t;
			}
			finally {
				if (producer != null) {
					try {
						producer.close();
					}
					catch (Throwable t) {
						// ignore
					}
				}
			}
		}

		public void shutdown() {
			this.running = false;
			this.interrupt();
		}

		public Throwable getError() {
			return this.error;
		}

		private static class MockTransformation extends Transformation<String> {
			public MockTransformation() {
				super("MockTransform", BasicTypeInfo.STRING_TYPE_INFO, 1);
			}

			@Override
			public Collection<Transformation<?>> getTransitivePredecessors() {
				return null;
			}
		}

		private static class DummyStreamExecutionEnvironment extends StreamExecutionEnvironment {

			@Override
			public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
				return null;
			}
		}
	}
}
