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

package org.apache.flink.streaming.connectors.kafka.api;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.ConnectorSource;
import org.apache.flink.streaming.connectors.util.DeserializationSchema;
import org.apache.flink.util.Collector;

/**
 * Source that listens to a Kafka topic using the high level Kafka API.
 * 
 * @param <OUT>
 *            Type of the messages on the topic.
 */
public class KafkaSource<OUT> extends ConnectorSource<OUT> {
	private static final long serialVersionUID = 1L;

	private final String zookeeperHost;
	private final String groupId;
	private final String topicId;

	private transient ConsumerConnector consumer;
	private transient ConsumerIterator<byte[], byte[]> consumerIterator;

	private long zookeeperSyncTimeMillis;
	private static final long ZOOKEEPER_DEFAULT_SYNC_TIME = 200;
	private static final String DEFAULT_GROUP_ID = "flink-group";

	private volatile boolean isRunning = false;

	/**
	 * Creates a KafkaSource that consumes a topic.
	 * 
	 * @param zookeeperHost
	 *            Address of the Zookeeper host (with port number).
	 * @param topicId
	 *            ID of the Kafka topic.
	 * @param deserializationSchema
	 *            User defined deserialization schema.
	 * @param zookeeperSyncTimeMillis
	 *            Synchronization time with zookeeper.
	 */
	public KafkaSource(String zookeeperHost, String topicId, String groupId,
			DeserializationSchema<OUT> deserializationSchema, long zookeeperSyncTimeMillis) {
		super(deserializationSchema);
		this.zookeeperHost = zookeeperHost;
		this.groupId = groupId;
		this.topicId = topicId;
		this.zookeeperSyncTimeMillis = zookeeperSyncTimeMillis;
	}

	public KafkaSource(String zookeeperHost, String topicId,
			DeserializationSchema<OUT> deserializationSchema, long zookeeperSyncTimeMillis) {
		this(zookeeperHost, topicId, DEFAULT_GROUP_ID, deserializationSchema,
				ZOOKEEPER_DEFAULT_SYNC_TIME);
	}

	public KafkaSource(String zookeeperHost, String topicId,
			DeserializationSchema<OUT> deserializationSchema) {
		this(zookeeperHost, topicId, deserializationSchema, ZOOKEEPER_DEFAULT_SYNC_TIME);
	}

	/**
	 * Initializes the connection to Kafka.
	 */
	private void initializeConnection() {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeperHost);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "10000");
		props.put("zookeeper.sync.time.ms", Long.toString(zookeeperSyncTimeMillis));
		props.put("auto.commit.interval.ms", "1000");

		consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(Collections.singletonMap(topicId, 1));
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topicId);
		KafkaStream<byte[], byte[]> stream = streams.get(0);

		consumer.commitOffsets();

		consumerIterator = stream.iterator();
	}

	/**
	 * Called to forward the data from the source to the {@link DataStream}.
	 * 
	 * @param collector
	 *            The Collector for sending data to the dataStream
	 */
	@Override
	public void run(Collector<OUT> collector) throws Exception {
		isRunning = true;
		try {
			while (isRunning && consumerIterator.hasNext()) {
				OUT out = schema.deserialize(consumerIterator.next().message());
				if (schema.isEndOfStream(out)) {
					break;
				}
				collector.collect(out);
			}
		} finally {
			consumer.shutdown();
		}
	}

	@Override
	public void open(Configuration config) throws Exception {
		initializeConnection();
	}

	@Override
	public void cancel() {
		isRunning = false;
		if (consumer != null) {
			consumer.shutdown();
		}
	}
}
