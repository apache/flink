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

package org.apache.flink.streaming.connectors.kafka.api.simple;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.OperatorState;
import org.apache.flink.streaming.api.streamvertex.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.api.simple.iterator.KafkaIdleConsumerIterator;
import org.apache.flink.streaming.connectors.kafka.api.simple.iterator.KafkaMultiplePartitionsIterator;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.CurrentOffset;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.GivenOffset;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.KafkaOffset;
import org.apache.flink.streaming.connectors.util.DeserializationSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka source persisting its offset through the {@link OperatorState} interface.
 * This allows the offset to be restored to the latest one that has been acknowledged
 * by the whole execution graph.
 *
 * @param <OUT>
 *            Type of the messages on the topic.
 */
public class PersistentKafkaSource<OUT> extends SimpleKafkaSource<OUT> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(PersistentKafkaSource.class);

	private transient OperatorState<Map<Integer, KafkaOffset>> kafkaOffSet;
	private transient Map<Integer, KafkaOffset> partitions;

	private int partition;
	
	public PersistentKafkaSource(String topicId, String host,
			DeserializationSchema<OUT> deserializationSchema) {
		super(topicId, host, deserializationSchema);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void open(Configuration parameters) throws InterruptedException {
		StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
		int indexOfSubtask = context.getIndexOfThisSubtask();
		int numberOfSubtasks = context.getNumberOfParallelSubtasks();
		int numberOfPartitions = new KafkaTopicUtils(hostName, 5000, 5000).getNumberOfPartitions(topicId);

		if (indexOfSubtask >= numberOfPartitions) {
			iterator = new KafkaIdleConsumerIterator();
		} else {
			if (context.containsState("kafka")) {
				kafkaOffSet = (OperatorState<Map<Integer, KafkaOffset>>) context.getState("kafka");

				partitions = kafkaOffSet.getState();

				iterator = new KafkaMultiplePartitionsIterator(hostName, topicId, partitions, 500);
			} else {
				partitions = new HashMap<Integer, KafkaOffset>();

				partition = indexOfSubtask;

				for (int partitionIndex = indexOfSubtask;
					 partitionIndex < numberOfPartitions;
					 partitionIndex += numberOfSubtasks) {
					partitions.put(partitionIndex, new CurrentOffset());
				}

				kafkaOffSet = new OperatorState<Map<Integer, KafkaOffset>>(partitions);

				context.registerState("kafka", kafkaOffSet);

				iterator = new KafkaMultiplePartitionsIterator(hostName, topicId, partitions, 500);
			}

			if (LOG.isInfoEnabled()) {
				LOG.info("KafkaSource ({}/{}) listening to partitions {} of topic {}.",
						indexOfSubtask + 1, numberOfSubtasks, partitions.keySet(), topicId);
			}
		}

		iterator.initialize();
	}

	@Override
	public void run(Collector<OUT> collector) throws Exception {
		MessageWithMetadata msg;
		while (iterator.hasNext()) {
			msg = iterator.nextWithOffset();
			OUT out = schema.deserialize(msg.getMessage());
			collector.collect(out);

			// TODO avoid object creation
			partitions.put(msg.getPartition(), new GivenOffset(msg.getOffset()));
			kafkaOffSet.update(partitions);
		}
	}
}
