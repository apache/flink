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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.util.serialization.JavaDefaultStringSchema;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;

import java.util.Random;

@SuppressWarnings("serial")
public class DataGenerators {
	
	public static void generateLongStringTupleSequence(StreamExecutionEnvironment env,
														String brokerConnection, String topic,
														int numPartitions,
														final int from, final int to) throws Exception {

		TypeInformation<Tuple2<Integer, Integer>> resultType = TypeInfoParser.parse("Tuple2<Integer, Integer>");

		env.setParallelism(numPartitions);
		env.getConfig().disableSysoutLogging();
		env.setNumberOfExecutionRetries(0);
		
		DataStream<Tuple2<Integer, Integer>> stream =env.addSource(
				new RichParallelSourceFunction<Tuple2<Integer, Integer>>() {

					private volatile boolean running = true;

					@Override
					public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
						int cnt = from;
						int partition = getRuntimeContext().getIndexOfThisSubtask();

						while (running && cnt <= to) {
							ctx.collect(new Tuple2<Integer, Integer>(partition, cnt));
							cnt++;
						}
					}

					@Override
					public void cancel() {
						running = false;
					}
				});

		stream.addSink(new FlinkKafkaProducer<>(topic,
				new TypeInformationSerializationSchema<>(resultType, env.getConfig()),
				FlinkKafkaProducer.getPropertiesFromBrokerList(brokerConnection),
				new Tuple2Partitioner(numPartitions)
		));

		env.execute("Data generator (Int, Int) stream to topic " + topic);
	}

	// ------------------------------------------------------------------------
	
	public static void generateRandomizedIntegerSequence(StreamExecutionEnvironment env,
															String brokerConnection, String topic,
															final int numPartitions,
															final int numElements,
															final boolean randomizeOrder) throws Exception {
		env.setParallelism(numPartitions);
		env.getConfig().disableSysoutLogging();
		env.setNumberOfExecutionRetries(0);

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

		stream
				.rebalance()
				.addSink(new FlinkKafkaProducer<>(topic,
						new TypeInformationSerializationSchema<>(BasicTypeInfo.INT_TYPE_INFO, env.getConfig()),
						FlinkKafkaProducer.getPropertiesFromBrokerList(brokerConnection),
						new KafkaPartitioner() {
							@Override
							public int partition(Object key, int numPartitions) {
								return ((Integer) key) % numPartitions;
							}
						}));

		env.execute("Scrambles int sequence generator");
	}
	
	// ------------------------------------------------------------------------
	
	public static class InfiniteStringsGenerator extends Thread {

		private final String kafkaConnectionString;
		
		private final String topic;
		
		private volatile Throwable error;
		
		private volatile boolean running = true;

		
		public InfiniteStringsGenerator(String kafkaConnectionString, String topic) {
			this.kafkaConnectionString = kafkaConnectionString;
			this.topic = topic;
		}

		@Override
		public void run() {
			// we manually feed data into the Kafka sink
			FlinkKafkaProducer<String> producer = null;
			try {
				producer = new FlinkKafkaProducer<>(kafkaConnectionString, topic, new JavaDefaultStringSchema());
				producer.setRuntimeContext(new MockRuntimeContext(1,0));
				producer.open(new Configuration());
				
				final StringBuilder bld = new StringBuilder();
				final Random rnd = new Random();
				
				while (running) {
					bld.setLength(0);
					
					int len = rnd.nextInt(100) + 1;
					for (int i = 0; i < len; i++) {
						bld.append((char) (rnd.nextInt(20) + 'a') );
					}
					
					String next = bld.toString();
					producer.invoke(next);
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
	}
}
