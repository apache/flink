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
import org.apache.flink.streaming.connectors.ConnectorSource;
import org.apache.flink.streaming.connectors.kafka.api.simple.iterator.KafkaConsumerIterator;
import org.apache.flink.streaming.connectors.kafka.api.simple.iterator.KafkaIdleConsumerIterator;
import org.apache.flink.streaming.connectors.kafka.api.simple.iterator.KafkaMultiplePartitionsIterator;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.BeginningOffset;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.CurrentOffset;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.GivenOffset;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.KafkaOffset;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.Offset;
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
 * 		Type of the messages on the topic.
 */
public class PersistentKafkaSource<OUT> extends ConnectorSource<OUT> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(PersistentKafkaSource.class);

	private final String topicId;
	private final String zookeeperServerAddress;
	private final int zookeeperSyncTimeMillis;
	private final int waitOnEmptyFetchMillis;
	private final KafkaOffset startingOffset;

	private transient KafkaConsumerIterator iterator;
	private transient OperatorState<Map<Integer, KafkaOffset>> kafkaOffSet;

	private transient Map<Integer, KafkaOffset> partitions;

	/**
	 * Creates a persistent Kafka source that consumes a topic.
	 * If there is are no new messages on the topic, this consumer will wait
	 * 100 milliseconds before trying to fetch messages again.
	 * The consumer will start consuming from the latest messages in the topic.
	 *
	 * @param zookeeperAddress
	 * 		Address of the Zookeeper host (with port number).
	 * @param topicId
	 * 		ID of the Kafka topic.
	 * @param deserializationSchema
	 * 		User defined deserialization schema.
	 */
	public PersistentKafkaSource(String zookeeperAddress, String topicId,
			DeserializationSchema<OUT> deserializationSchema) {
		this(zookeeperAddress, topicId, deserializationSchema, KafkaTopicUtils.DEFAULT_ZOOKEEPER_CONNECTION_TIMEOUT_MS, 100);
	}

	/**
	 * Creates a persistent Kafka source that consumes a topic.
	 * If there is are no new messages on the topic, this consumer will wait
	 * waitOnEmptyFetchMillis milliseconds before trying to fetch messages again.
	 * The consumer will start consuming from the latest messages in the topic.
	 *
	 * @param zookeeperAddress
	 * 		Address of the Zookeeper host (with port number).
	 * @param topicId
	 * 		ID of the Kafka topic.
	 * @param deserializationSchema
	 * 		User defined deserialization schema.
	 * @param zookeeperSyncTimeMillis
	 * 		Synchronization time with zookeeper.
	 * @param waitOnEmptyFetchMillis
	 * 		Time to wait before fetching for new message.
	 */
	public PersistentKafkaSource(String zookeeperAddress, String topicId,
			DeserializationSchema<OUT> deserializationSchema, int zookeeperSyncTimeMillis, int waitOnEmptyFetchMillis) {
		this(zookeeperAddress, topicId, deserializationSchema, zookeeperSyncTimeMillis, waitOnEmptyFetchMillis, Offset.FROM_CURRENT);
	}

	/**
	 * Creates a persistent Kafka source that consumes a topic.
	 * If there is are no new messages on the topic, this consumer will wait
	 * waitOnEmptyFetchMillis milliseconds before trying to fetch messages again.
	 *
	 * @param zookeeperAddress
	 * 		Address of the Zookeeper host (with port number).
	 * @param topicId
	 * 		ID of the Kafka topic.
	 * @param deserializationSchema
	 * 		User defined deserialization schema.
	 * @param zookeeperSyncTimeMillis
	 * 		Synchronization time with zookeeper.
	 * @param waitOnEmptyFetchMillis
	 * 		Time to wait before fetching for new message.
	 * @param startOffsetType
	 * 		The offset to start from (beginning or current).
	 */
	public PersistentKafkaSource(String zookeeperAddress, String topicId,
			DeserializationSchema<OUT> deserializationSchema, int zookeeperSyncTimeMillis,
			int waitOnEmptyFetchMillis, Offset startOffsetType) {
		super(deserializationSchema);

		this.topicId = topicId;
		this.zookeeperServerAddress = zookeeperAddress;

		switch (startOffsetType) {
			case FROM_BEGINNING:
				this.startingOffset = new BeginningOffset();
				break;
			case FROM_CURRENT:
				this.startingOffset = new CurrentOffset();
				break;
			default:
				this.startingOffset = new CurrentOffset();
				break;
		}

		this.zookeeperSyncTimeMillis = zookeeperSyncTimeMillis;
		this.waitOnEmptyFetchMillis = waitOnEmptyFetchMillis;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void open(Configuration parameters) throws InterruptedException {
		StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
		int indexOfSubtask = context.getIndexOfThisSubtask();
		int numberOfSubtasks = context.getNumberOfParallelSubtasks();

		KafkaTopicUtils kafkaTopicUtils =
				new KafkaTopicUtils(zookeeperServerAddress, zookeeperSyncTimeMillis, zookeeperSyncTimeMillis);

		int numberOfPartitions = kafkaTopicUtils.getNumberOfPartitions(topicId);

		String brokerAddress = kafkaTopicUtils.getLeaderBrokerAddressForTopic(topicId);

		if (indexOfSubtask >= numberOfPartitions) {
			iterator = new KafkaIdleConsumerIterator();
		} else {
			if (context.containsState("kafka")) {
				kafkaOffSet = (OperatorState<Map<Integer, KafkaOffset>>) context.getState("kafka");

				partitions = kafkaOffSet.getState();
			} else {
				partitions = new HashMap<Integer, KafkaOffset>();

				for (int partitionIndex = indexOfSubtask; partitionIndex < numberOfPartitions; partitionIndex += numberOfSubtasks) {
					partitions.put(partitionIndex, startingOffset);
				}

				kafkaOffSet = new OperatorState<Map<Integer, KafkaOffset>>(partitions);

				context.registerState("kafka", kafkaOffSet);
			}

			iterator = getMultiKafkaIterator(brokerAddress, topicId, partitions, waitOnEmptyFetchMillis);

			if (LOG.isInfoEnabled()) {
				LOG.info("KafkaSource ({}/{}) listening to partitions {} of topic {}.",
						indexOfSubtask + 1, numberOfSubtasks, partitions.keySet(), topicId);
			}
		}

		iterator.initialize();
	}

	protected KafkaConsumerIterator getMultiKafkaIterator(String hostName, String topic, Map<Integer, KafkaOffset> partitionsWithOffset, int waitOnEmptyFetch) {
		return new KafkaMultiplePartitionsIterator(hostName, topic, partitionsWithOffset, waitOnEmptyFetch);
	}

	@Override
	public void run(Collector<OUT> collector) throws Exception {
		MessageWithMetadata msg;
		while (iterator.hasNext()) {
			msg = iterator.nextWithOffset();
			OUT out = schema.deserialize(msg.getMessage());

			if (schema.isEndOfStream(out)) {
				break;
			}

			collector.collect(out);

			// TODO avoid object creation
			partitions.put(msg.getPartition(), new GivenOffset(msg.getOffset()));
			kafkaOffSet.update(partitions);
		}
	}

	@Override
	public void cancel() {
	}
}
