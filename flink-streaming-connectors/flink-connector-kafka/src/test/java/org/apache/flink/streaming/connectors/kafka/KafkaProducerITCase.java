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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.testutils.SuccessException;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;

import org.junit.Test;

import java.io.Serializable;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@SuppressWarnings("serial")
public class KafkaProducerITCase extends KafkaTestBase {


	/**
	 * 
	 * <pre>
	 *             +------> (sink) --+--> [KAFKA-1] --> (source) -> (map) --+
	 *            /                  |                                       \
	 *           /                   |                                        \
	 * (source) ----------> (sink) --+--> [KAFKA-2] --> (source) -> (map) -----+-> (sink)
	 *           \                   |                                        /
	 *            \                  |                                       /
	 *             +------> (sink) --+--> [KAFKA-3] --> (source) -> (map) --+
	 * </pre>
	 * 
	 * The mapper validates that the values come consistently from the correct Kafka partition.
	 * 
	 * The final sink validates that there are no duplicates and that all partitions are present.
	 */
	@Test
	public void testCustomPartitioning() {
		try {
			LOG.info("Starting KafkaProducerITCase.testCustomPartitioning()");

			final String topic = "customPartitioningTestTopic";
			final int parallelism = 3;
			
			createTestTopic(topic, parallelism, 1);

			TypeInformation<Tuple2<Long, String>> longStringInfo = TypeInfoParser.parse("Tuple2<Long, String>");

			StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
			env.setNumberOfExecutionRetries(0);
			env.getConfig().disableSysoutLogging();

			TypeInformationSerializationSchema<Tuple2<Long, String>> serSchema =
					new TypeInformationSerializationSchema<>(longStringInfo, env.getConfig());

			TypeInformationSerializationSchema<Tuple2<Long, String>> deserSchema =
					new TypeInformationSerializationSchema<>(longStringInfo, env.getConfig());

			// ------ producing topology ---------
			
			// source has DOP 1 to make sure it generates no duplicates
			DataStream<Tuple2<Long, String>> stream = env.addSource(new SourceFunction<Tuple2<Long, String>>() {

				private boolean running = true;

				@Override
				public void run(SourceContext<Tuple2<Long, String>> ctx) throws Exception {
					long cnt = 0;
					while (running) {
						ctx.collect(new Tuple2<Long, String>(cnt, "kafka-" + cnt));
						cnt++;
					}
				}

				@Override
				public void cancel() {
					running = false;
				}
			})
			.setParallelism(1);
			
			// sink partitions into 
			stream.addSink(new FlinkKafkaProducer<>(topic, serSchema, FlinkKafkaProducer.getPropertiesFromBrokerList(brokerConnectionStrings), new CustomPartitioner(parallelism)))
			.setParallelism(parallelism);

			// ------ consuming topology ---------
			
			FlinkKafkaConsumer<Tuple2<Long, String>> source = 
					new FlinkKafkaConsumer<>(Collections.singletonList(topic), deserSchema, standardProps,
							FlinkKafkaConsumer.OffsetStore.FLINK_ZOOKEEPER,
							FlinkKafkaConsumer.FetcherType.LEGACY_LOW_LEVEL);
			
			env.addSource(source).setParallelism(parallelism)

					// mapper that validates partitioning and maps to partition
					.map(new RichMapFunction<Tuple2<Long, String>, Integer>() {
						
						private int ourPartition = -1;
						@Override
						public Integer map(Tuple2<Long, String> value) {
							int partition = value.f0.intValue() % parallelism;
							if (ourPartition != -1) {
								assertEquals("inconsistent partitioning", ourPartition, partition);
							} else {
								ourPartition = partition;
							}
							return partition;
						}
					}).setParallelism(parallelism)
					
					.addSink(new SinkFunction<Integer>() {
						
						private int[] valuesPerPartition = new int[parallelism];
						
						@Override
						public void invoke(Integer value) throws Exception {
							valuesPerPartition[value]++;
							
							boolean missing = false;
							for (int i : valuesPerPartition) {
								if (i < 100) {
									missing = true;
									break;
								}
							}
							if (!missing) {
								throw new SuccessException();
							}
						}
					}).setParallelism(1);
			
			tryExecute(env, "custom partitioning test");

			deleteTestTopic(topic);
			
			LOG.info("Finished KafkaProducerITCase.testCustomPartitioning()");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	
	// ------------------------------------------------------------------------

	public static class CustomPartitioner extends KafkaPartitioner implements Serializable {

		private final int expectedPartitions;

		public CustomPartitioner(int expectedPartitions) {
			this.expectedPartitions = expectedPartitions;
		}

		@Override
		public int partition(Object key, int numPartitions) {
			@SuppressWarnings("unchecked")
			Tuple2<Long, String> tuple = (Tuple2<Long, String>) key;
			
			assertEquals(expectedPartitions, numPartitions);
			
			return (int) (tuple.f0 % numPartitions);
		}
	}
}
