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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Preconditions;
import kafka.consumer.ConsumerConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.OperatorState;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.ConnectorSource;
import org.apache.flink.streaming.connectors.kafka.api.simple.iterator.KafkaConsumerIterator;
import org.apache.flink.streaming.connectors.kafka.api.simple.iterator.KafkaIdleConsumerIterator;
import org.apache.flink.streaming.connectors.kafka.api.simple.iterator.KafkaMultiplePartitionsIterator;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.BeginningOffset;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.CurrentOffset;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.GivenOffset;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.KafkaOffset;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.Offset;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
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

	public static final String WAIT_ON_EMPTY_FETCH_KEY = "flink.waitOnEmptyFetchMillis";
	public static final String WAIT_ON_FAILED_LEADER_MS_KEY = "flink.waitOnFailedLeaderDetection";
	public static final int WAIT_ON_FAILED_LEADER__MS_DEFAULT = 2000;

	public static final String MAX_FAILED_LEADER_RETRIES_KEY = "flink.maxLeaderDetectionRetries";
	public static final int MAX_FAILED_LEADER_RETRIES_DEFAULT = 3;

	private final String topicId;
	private final KafkaOffset startingOffset;
	private transient ConsumerConfig consumerConfig; // ConsumerConfig is not serializable.

	private transient KafkaConsumerIterator iterator;
	private transient OperatorState<Map<Integer, KafkaOffset>> kafkaOffSetOperatorState;

	private transient Map<Integer, KafkaOffset> partitionOffsets;

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
	 * THIS CONSTRUCTOR IS DEPRECATED: USE the constructor with the ConsumerConfig.
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
	@Deprecated
	public PersistentKafkaSource(String zookeeperAddress, String topicId,DeserializationSchema<OUT> deserializationSchema, int zookeeperSyncTimeMillis, int waitOnEmptyFetchMillis, Offset startOffsetType) {
		this(topicId, deserializationSchema, startOffsetType, legacyParametersToConsumerConfig(zookeeperAddress, zookeeperSyncTimeMillis, waitOnEmptyFetchMillis));
	}

	private static ConsumerConfig legacyParametersToConsumerConfig(String zookeeperAddress, int zookeeperSyncTimeMillis, int waitOnEmptyFetchMillis) {
		Properties props = new Properties();
		props.setProperty("zookeeper.sync.time.ms", Integer.toString(zookeeperSyncTimeMillis));
		props.setProperty(WAIT_ON_EMPTY_FETCH_KEY, Integer.toString(waitOnEmptyFetchMillis));
		props.setProperty("zookeeper.connect", zookeeperAddress);
		props.setProperty("group.id", "flink-persistent-kafka-source");
		return new ConsumerConfig(props);
	}

	/**
	 * Creates a persistent Kafka source that consumes a topic.
	 * If there is are no new messages on the topic, this consumer will wait
	 * waitOnEmptyFetchMillis milliseconds before trying to fetch messages again.
	 *
	 * @param topicId
	 * 		ID of the Kafka topic.
	 * @param deserializationSchema
	 * 		User defined deserialization schema.
	 * @param startOffsetType
	 * 		The offset to start from (beginning or current).
	 * @param consumerConfig
	 * 		Additional configuration for the PersistentKafkaSource.
	 * 		NOTE: This source will only respect certain configuration values from the config!
	 */
	public PersistentKafkaSource(String topicId, DeserializationSchema<OUT> deserializationSchema, Offset startOffsetType, ConsumerConfig consumerConfig) {
		super(deserializationSchema);
		Preconditions.checkNotNull(topicId, "The topic id can not be null");
		Preconditions.checkNotNull(deserializationSchema, "The deserialization schema can not be null");
		Preconditions.checkNotNull(consumerConfig, "ConsumerConfig can not be null");

		this.consumerConfig = consumerConfig;

		this.topicId = topicId;

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
	}

	// ---------------------- Source lifecycle methods (open / run / cancel ) -----------------

	@SuppressWarnings("unchecked")
	@Override
	public void open(Configuration parameters) throws InterruptedException {
		LOG.info("Starting PersistentKafkaSource");
		StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();
		int indexOfSubtask = context.getIndexOfThisSubtask();
		int numberOfSubtasks = context.getNumberOfParallelSubtasks();

		KafkaTopicUtils kafkaTopicUtils = new KafkaTopicUtils(consumerConfig.zkConnect(), consumerConfig.zkSyncTimeMs(), consumerConfig.zkConnectionTimeoutMs());

		int numberOfPartitions = kafkaTopicUtils.getNumberOfPartitions(topicId);

		if (indexOfSubtask >= numberOfPartitions) {
			LOG.info("Creating idle consumer because this subtask ({}) is higher than the number partitions ({})", indexOfSubtask + 1, numberOfPartitions);
			iterator = new KafkaIdleConsumerIterator();
		} else {
			if (context.containsState("kafka")) {
				LOG.info("Initializing PersistentKafkaSource from existing state.");
				kafkaOffSetOperatorState = (OperatorState<Map<Integer, KafkaOffset>>) context.getState("kafka");

				partitionOffsets = kafkaOffSetOperatorState.getState();
			} else {
				LOG.info("No existing state found. Creating new");
				partitionOffsets = new HashMap<Integer, KafkaOffset>();

				for (int partitionIndex = indexOfSubtask; partitionIndex < numberOfPartitions; partitionIndex += numberOfSubtasks) {
					partitionOffsets.put(partitionIndex, startingOffset);
				}

				kafkaOffSetOperatorState = new OperatorState<Map<Integer, KafkaOffset>>(partitionOffsets);

				context.registerState("kafka", kafkaOffSetOperatorState);
			}

			iterator = new KafkaMultiplePartitionsIterator(topicId, partitionOffsets, kafkaTopicUtils, this.consumerConfig);

			if (LOG.isInfoEnabled()) {
				LOG.info("PersistentKafkaSource ({}/{}) listening to partitionOffsets {} of topic {}.",
						indexOfSubtask + 1, numberOfSubtasks, partitionOffsets.keySet(), topicId);
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

			if (schema.isEndOfStream(out)) {
				break;
			}

			collector.collect(out);

			// TODO avoid object creation
			partitionOffsets.put(msg.getPartition(), new GivenOffset(msg.getOffset()));
			kafkaOffSetOperatorState.update(partitionOffsets);
		}
	}

	@Override
	public void cancel() {
		LOG.info("PersistentKafkaSource has been cancelled");
	}



	// ---------------------- (Java)Serialization methods for the consumerConfig -----------------

	private void writeObject(ObjectOutputStream out)
			throws IOException, ClassNotFoundException {
		out.defaultWriteObject();
		out.writeObject(consumerConfig.props().props());
	}

	private void readObject(ObjectInputStream in)
			throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		Properties props = (Properties) in.readObject();
		consumerConfig = new ConsumerConfig(props);
	}
}
