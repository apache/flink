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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.admin.AdminUtils;
import kafka.api.PartitionMetadata;
import kafka.api.TopicMetadata;
import kafka.cluster.Broker;
import kafka.common.LeaderNotAvailableException;
import kafka.common.UnknownTopicOrPartitionException;
import scala.collection.JavaConversions;
import scala.collection.Seq;

/**
 * For retrieving Kafka topic information (e.g. number of partitions),
 * or creating a topic.
 */
public class KafkaTopicUtils {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicUtils.class);

	private ZkClient zkClient;

	public static final int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS = 10000;
	public static final int DEFAULT_ZOOKEEPER_CONNECTION_TIMEOUT_MS = 10000;

	private final String zookeeperAddress;
	private final int sessionTimeoutMs;
	private final int connectionTimeoutMs;

	private volatile boolean isRunning = false;

	public KafkaTopicUtils(String zookeeperServer) {
		this(zookeeperServer, DEFAULT_ZOOKEEPER_SESSION_TIMEOUT_MS, DEFAULT_ZOOKEEPER_CONNECTION_TIMEOUT_MS);
	}

	public KafkaTopicUtils(String zookeeperAddress, int sessionTimeoutMs, int connectionTimeoutMs) {
		this.zookeeperAddress = zookeeperAddress;
		this.sessionTimeoutMs = sessionTimeoutMs;
		this.connectionTimeoutMs = connectionTimeoutMs;
	}

	public void createTopic(String topicName, int numOfPartitions, int replicationFactor) {

		LOG.info("Creating Kafka topic '{}'", topicName);
		Properties topicConfig = new Properties();
		if (topicExists(topicName)) {
			if (LOG.isWarnEnabled()) {
				LOG.warn("Kafka topic \"{}\" already exists. Returning without action.", topicName);
			}
		} else {
			LOG.info("Connecting zookeeper");

			initZkClient();
			AdminUtils.createTopic(zkClient, topicName, numOfPartitions, replicationFactor, topicConfig);
			closeZkClient();
		}
	}

	public String getBrokerList(String topicName) {
		return getBrokerAddressList(getBrokerAddresses(topicName));
	}

	public String getBrokerList(String topicName, int partitionId) {
		return getBrokerAddressList(getBrokerAddresses(topicName, partitionId));
	}

	public Set<String> getBrokerAddresses(String topicName) {
		int numOfPartitions = getNumberOfPartitions(topicName);

		HashSet<String> brokers = new HashSet<String>();
		for (int i = 0; i < numOfPartitions; i++) {
			brokers.addAll(getBrokerAddresses(topicName, i));
		}
		return brokers;
	}

	public Set<String> getBrokerAddresses(String topicName, int partitionId) {
		PartitionMetadata partitionMetadata = waitAndGetPartitionMetadata(topicName, partitionId);
		Collection<Broker> inSyncReplicas = JavaConversions.asJavaCollection(partitionMetadata.isr());

		HashSet<String> addresses = new HashSet<String>();
		for (Broker broker : inSyncReplicas) {
			addresses.add(broker.connectionString());
		}
		return addresses;
	}

	private static String getBrokerAddressList(Set<String> brokerAddresses) {
		StringBuilder brokerAddressList = new StringBuilder("");
		for (String broker : brokerAddresses) {
			brokerAddressList.append(broker);
			brokerAddressList.append(',');
		}
		brokerAddressList.deleteCharAt(brokerAddressList.length() - 1);

		return brokerAddressList.toString();
	}

	public int getNumberOfPartitions(String topicName) {
		Seq<PartitionMetadata> partitionMetadataSeq = getTopicMetadata(topicName).partitionsMetadata();
		return JavaConversions.asJavaCollection(partitionMetadataSeq).size();
	}

	public PartitionMetadata waitAndGetPartitionMetadata(String topicName, int partitionId) {
		isRunning = true;
		PartitionMetadata partitionMetadata = null;
		while (isRunning) {
			try {
				partitionMetadata = getPartitionMetadata(topicName, partitionId);
				return partitionMetadata;
			} catch (LeaderNotAvailableException e) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Got {} trying to fetch metadata again", e.getMessage());
				}
			}
		}
		isRunning = false;
		return partitionMetadata;
	}

	public PartitionMetadata getPartitionMetadata(String topicName, int partitionId) {
		PartitionMetadata partitionMetadata = getPartitionMetadataWithErrorCode(topicName, partitionId);
		switch (partitionMetadata.errorCode()) {
			case 0:
				return partitionMetadata;
			case 3:
				throw new UnknownTopicOrPartitionException("While fetching metadata for " + topicName + " / " + partitionId);
			case 5:
				throw new LeaderNotAvailableException("While fetching metadata for " + topicName + " / " + partitionId);
				default:
					throw new RuntimeException("Unknown error occurred while fetching metadata for "
							+ topicName + " / " + partitionId + ", with error code: " + partitionMetadata.errorCode());
		}
	}

	private PartitionMetadata getPartitionMetadataWithErrorCode(String topicName, int partitionId) {
		TopicMetadata topicInfo = getTopicMetadata(topicName);

		Collection<PartitionMetadata> partitions = JavaConversions.asJavaCollection(topicInfo.partitionsMetadata());

		Iterator<PartitionMetadata> iterator = partitions.iterator();
		for (PartitionMetadata partition : partitions) {
			if (partition.partitionId() == partitionId) {
				return partition;
			}
		}

		throw new RuntimeException("No such partition: " + topicName + " / " + partitionId);
	}

	public TopicMetadata getTopicMetadata(String topicName) {
		TopicMetadata topicMetadata = getTopicMetadataWithErrorCode(topicName);
		switch (topicMetadata.errorCode()) {
			case 0:
				return topicMetadata;
			case 3:
				throw new UnknownTopicOrPartitionException("While fetching metadata for topic " + topicName);
			case 5:
				throw new LeaderNotAvailableException("While fetching metadata for topic " + topicName);
			default:
				throw new RuntimeException("Unknown error occurred while fetching metadata for topic "
						+ topicName + ", with error code: " + topicMetadata.errorCode());
		}
	}

	private TopicMetadata getTopicMetadataWithErrorCode(String topicName) {
		if (topicExists(topicName)) {
			initZkClient();
			TopicMetadata topicMetadata = AdminUtils.fetchTopicMetadataFromZk(topicName, zkClient);
			closeZkClient();

			return topicMetadata;
		} else {
			throw new RuntimeException("Topic does not exist: " + topicName);
		}
	}

	public boolean topicExists(String topicName) {
		initZkClient();
		boolean topicExists = AdminUtils.topicExists(zkClient, topicName);
		closeZkClient();

		return topicExists;
	}

	private void initZkClient() {
		zkClient = new ZkClient(zookeeperAddress, sessionTimeoutMs, connectionTimeoutMs,
				new KafkaZKStringSerializer());
		zkClient.waitUntilConnected();
	}

	private void closeZkClient() {
		zkClient.close();
		zkClient = null;
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
