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

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.admin.AdminUtils;
import kafka.api.PartitionMetadata;
import kafka.api.TopicMetadata;
import kafka.cluster.Broker;
import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * For retrieving Kafka topic information (e.g. number of partitions),
 * or creating a topic.
 */
public class KafkaTopicUtils {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicUtils.class);

	private final ZkClient zkClient;

	public static final int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS = 10000;
	public static final int DEFAULT_ZOOKEEPER_CONNECTION_TIMEOUT_MS = 10000;

	public KafkaTopicUtils(String zookeeperServer) {
		this(zookeeperServer, DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS, DEFAULT_ZOOKEEPER_CONNECTION_TIMEOUT_MS);
	}

	public KafkaTopicUtils(String zookeeperAddress, int sessionTimeoutMs, int connectionTimeoutMs) {
		zkClient = new ZkClient(zookeeperAddress, sessionTimeoutMs, connectionTimeoutMs,
				new KafkaZKStringSerializer());
		zkClient.waitUntilConnected();
	}

	public void createTopic(String topicName, int numOfPartitions, int replicationFactor) {
		LOG.info("Creating Kafka topic '{}'", topicName);
		Properties topicConfig = new Properties();
		if (topicExists(topicName)) {
			if (LOG.isWarnEnabled()) {
				LOG.warn("Kafka topic \"{}\" already exists. Returning without action.", topicName);
			}
		} else {
			AdminUtils.createTopic(zkClient, topicName, numOfPartitions, replicationFactor, topicConfig);
		}
	}

	public int getNumberOfPartitions(String topicName) {
		Seq<PartitionMetadata> partitionMetadataSeq = getTopicInfo(topicName).partitionsMetadata();
		return JavaConversions.asJavaCollection(partitionMetadataSeq).size();
	}

	public String getLeaderBrokerAddressForTopic(String topicName) {
		TopicMetadata topicInfo = getTopicInfo(topicName);

		Collection<PartitionMetadata> partitions = JavaConversions.asJavaCollection(topicInfo.partitionsMetadata());
		PartitionMetadata partitionMetadata = partitions.iterator().next();

		Broker leader = JavaConversions.asJavaCollection(partitionMetadata.isr()).iterator().next();

		// TODO for Kafka version 8.2.0
		//		return leader.connectionString();
		return leader.connectionString();
	}

	public TopicMetadata getTopicInfo(String topicName) {
		if (topicExists(topicName)) {
			return AdminUtils.fetchTopicMetadataFromZk(topicName, zkClient);
		} else {
			throw new RuntimeException("Topic does not exist: " + topicName);
		}
	}

	public boolean topicExists(String topicName) {
		return AdminUtils.topicExists(zkClient, topicName);
	}

	private static class KafkaZKStringSerializer implements ZkSerializer {

		@Override
		public byte[] serialize(Object data) throws ZkMarshallingError {
			try {
				return ((String) data).getBytes("UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public Object deserialize(byte[] bytes) throws ZkMarshallingError {
			if (bytes == null) {
				return null;
			} else {
				try {
					return new String(bytes, "UTF-8");
				} catch (UnsupportedEncodingException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
}
