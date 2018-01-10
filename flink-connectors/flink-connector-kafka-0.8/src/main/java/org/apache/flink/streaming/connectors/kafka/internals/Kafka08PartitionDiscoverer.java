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

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.NetUtils;

import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08.DEFAULT_GET_PARTITIONS_RETRIES;
import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08.GET_PARTITIONS_RETRIES_KEY;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.PropertiesUtil.getInt;

/**
 * A partition discoverer that can be used to discover topics and partitions metadata
 * from Kafka brokers via the Kafka 0.8 low-level consumer API.
 */
@Internal
public class Kafka08PartitionDiscoverer extends AbstractPartitionDiscoverer {

	private static final Logger LOG = LoggerFactory.getLogger(Kafka08PartitionDiscoverer.class);

	private static final String dummyClientId = "flink-kafka-consumer-partition-lookup";

	/** All seed broker addresses. */
	private final String[] seedBrokerAddresses;

	/** Configuration for the Kafka client. */
	private final int numRetries;
	private final int soTimeout;
	private final int bufferSize;

	/**
	 * The current seed broker address to use.
	 * Each subtask starts with an assigned seed broker using round-robin assigning.
	 * If this subtask fails in any one of the fetch attempts, the next address in the seed brokers list
	 * will be used.
	 */
	private int currentContactSeedBrokerIndex;

	/** Low-level consumer used to fetch topics and partitions metadata. */
	private SimpleConsumer consumer;

	public Kafka08PartitionDiscoverer(
			KafkaTopicsDescriptor topicsDescriptor,
			int indexOfThisSubtask,
			int numParallelSubtasks,
			Properties kafkaProperties) {

		super(topicsDescriptor, indexOfThisSubtask, numParallelSubtasks);

		checkNotNull(kafkaProperties);

		String seedBrokersConfString = kafkaProperties.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
		checkArgument(seedBrokersConfString != null && !seedBrokersConfString.isEmpty(),
				"Configuration property %s not set", ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);

		this.seedBrokerAddresses = seedBrokersConfString.split(",");

		// evenly distribute seed brokers across subtasks, to
		// avoid too much pressure on a single broker on startup
		this.currentContactSeedBrokerIndex = indexOfThisSubtask % seedBrokerAddresses.length;

		this.numRetries = getInt(kafkaProperties, GET_PARTITIONS_RETRIES_KEY, DEFAULT_GET_PARTITIONS_RETRIES);
		this.soTimeout = getInt(kafkaProperties, "socket.timeout.ms", 30000);
		this.bufferSize = getInt(kafkaProperties, "socket.receive.buffer.bytes", 65536);
	}

	@Override
	protected void initializeConnections() {
		URL contactUrl = NetUtils.getCorrectHostnamePort(seedBrokerAddresses[currentContactSeedBrokerIndex]);
		this.consumer = new SimpleConsumer(contactUrl.getHost(), contactUrl.getPort(), soTimeout, bufferSize, dummyClientId);
	}

	@Override
	protected List<String> getAllTopics() {
		List<String> topics = new LinkedList<>();

		retryLoop: for (int retry = 0; retry < numRetries; retry++) {
			brokersLoop: for (int arrIdx = 0; arrIdx < seedBrokerAddresses.length; arrIdx++) {
				LOG.info("Trying to get topic metadata from broker {} in try {}/{}", seedBrokerAddresses[currentContactSeedBrokerIndex], retry, numRetries);

				try {
					// clear in case we have an incomplete list from previous tries
					topics.clear();

					for (TopicMetadata item : consumer.send(new TopicMetadataRequest(Collections.<String>emptyList())).topicsMetadata()) {
						if (item.errorCode() != ErrorMapping.NoError()) {
							// warn and try more brokers
							LOG.warn("Error while getting metadata from broker {} to find partitions for {}. Error: {}.",
								seedBrokerAddresses[currentContactSeedBrokerIndex], topics.toString(), ErrorMapping.exceptionFor(item.errorCode()).getMessage());

							useNextAddressAsNewContactSeedBroker();
							continue brokersLoop;
						}

						topics.add(item.topic());
					}
					break retryLoop; // leave the loop through the brokers
				}
				catch (Exception e) {
					//validates seed brokers in case of a ClosedChannelException
					validateSeedBrokers(seedBrokerAddresses, e);
					LOG.warn("Error communicating with broker {} to find partitions for {}. {} Message: {}",
						seedBrokerAddresses[currentContactSeedBrokerIndex], topics, e.getClass().getName(), e.getMessage());
					LOG.debug("Detailed trace", e);

					// we sleep a bit. Retrying immediately doesn't make sense in cases where Kafka is reorganizing the leader metadata
					try {
						Thread.sleep(500);
					} catch (InterruptedException e1) {
						// sleep shorter.
					}

					useNextAddressAsNewContactSeedBroker();
				}
			} // brokers loop
		} // retries loop

		return topics;
	}

	@Override
	public List<KafkaTopicPartition> getAllPartitionsForTopics(List<String> topics) {
		return KafkaTopicPartition.dropLeaderData(getPartitionLeadersForTopics(topics));
	}

	@Override
	protected void wakeupConnections() {
		// nothing to do, as Kafka 0.8's SimpleConsumer does not support wakeup
	}

	@Override
	protected void closeConnections() throws Exception {
		if (consumer != null) {
			SimpleConsumer consumer = this.consumer;
			consumer.close();

			// de-reference the consumer to avoid closing multiple times
			this.consumer = null;
		}
	}

	/**
	 * Send request to Kafka to get partitions for topics.
	 *
	 * @param topics The name of the topics.
	 */
	public List<KafkaTopicPartitionLeader> getPartitionLeadersForTopics(List<String> topics) {
		List<KafkaTopicPartitionLeader> partitions = new LinkedList<>();

		retryLoop: for (int retry = 0; retry < numRetries; retry++) {
			brokersLoop: for (int arrIdx = 0; arrIdx < seedBrokerAddresses.length; arrIdx++) {
				LOG.info("Trying to get topic metadata from broker {} in try {}/{}", seedBrokerAddresses[currentContactSeedBrokerIndex], retry, numRetries);

				try {
					// clear in case we have an incomplete list from previous tries
					partitions.clear();

					for (TopicMetadata item : consumer.send(new TopicMetadataRequest(topics)).topicsMetadata()) {
						if (item.errorCode() != ErrorMapping.NoError()) {
							// warn and try more brokers
							LOG.warn("Error while getting metadata from broker {} to find partitions for {}. Error: {}.",
								seedBrokerAddresses[currentContactSeedBrokerIndex], topics.toString(), ErrorMapping.exceptionFor(item.errorCode()).getMessage());

							useNextAddressAsNewContactSeedBroker();
							continue brokersLoop;
						}

						if (!topics.contains(item.topic())) {
							LOG.warn("Received metadata from topic " + item.topic() + " even though it was not requested. Skipping ...");

							useNextAddressAsNewContactSeedBroker();
							continue brokersLoop;
						}

						for (PartitionMetadata part : item.partitionsMetadata()) {
							Node leader = brokerToNode(part.leader());
							KafkaTopicPartition ktp = new KafkaTopicPartition(item.topic(), part.partitionId());
							KafkaTopicPartitionLeader pInfo = new KafkaTopicPartitionLeader(ktp, leader);
							partitions.add(pInfo);
						}
					}
					break retryLoop; // leave the loop through the brokers
				}
				catch (Exception e) {
					//validates seed brokers in case of a ClosedChannelException
					validateSeedBrokers(seedBrokerAddresses, e);
					LOG.warn("Error communicating with broker {} to find partitions for {}. {} Message: {}",
						seedBrokerAddresses[currentContactSeedBrokerIndex], topics, e.getClass().getName(), e.getMessage());
					LOG.debug("Detailed trace", e);

					// we sleep a bit. Retrying immediately doesn't make sense in cases where Kafka is reorganizing the leader metadata
					try {
						Thread.sleep(500);
					} catch (InterruptedException e1) {
						// sleep shorter.
					}

					useNextAddressAsNewContactSeedBroker();
				}
			} // brokers loop
		} // retries loop

		return partitions;
	}

	/**
	 * Re-establish broker connection using the next available seed broker address.
	 */
	private void useNextAddressAsNewContactSeedBroker() {
		if (++currentContactSeedBrokerIndex == seedBrokerAddresses.length) {
			currentContactSeedBrokerIndex = 0;
		}

		URL newContactUrl = NetUtils.getCorrectHostnamePort(seedBrokerAddresses[currentContactSeedBrokerIndex]);
		this.consumer = new SimpleConsumer(newContactUrl.getHost(), newContactUrl.getPort(), soTimeout, bufferSize, dummyClientId);
	}

	/**
	 * Turn a broker instance into a node instance.
	 *
	 * @param broker broker instance
	 * @return Node representing the given broker
	 */
	private static Node brokerToNode(Broker broker) {
		return new Node(broker.id(), broker.host(), broker.port());
	}

	/**
	 * Validate that at least one seed broker is valid in case of a
	 * ClosedChannelException.
	 *
	 * @param seedBrokers
	 *            array containing the seed brokers e.g. ["host1:port1",
	 *            "host2:port2"]
	 * @param exception
	 *            instance
	 */
	private static void validateSeedBrokers(String[] seedBrokers, Exception exception) {
		if (!(exception instanceof ClosedChannelException)) {
			return;
		}
		int unknownHosts = 0;
		for (String broker : seedBrokers) {
			URL brokerUrl = NetUtils.getCorrectHostnamePort(broker.trim());
			try {
				InetAddress.getByName(brokerUrl.getHost());
			} catch (UnknownHostException e) {
				unknownHosts++;
			}
		}
		// throw meaningful exception if all the provided hosts are invalid
		if (unknownHosts == seedBrokers.length) {
			throw new IllegalArgumentException("All the servers provided in: '"
				+ ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG + "' config are invalid. (unknown hosts)");
		}
	}
}
