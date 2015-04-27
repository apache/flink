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

package org.apache.flink.streaming.connectors.kafka.api.simple.iterator;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import kafka.consumer.ConsumerConfig;
import org.apache.flink.streaming.connectors.kafka.api.simple.KafkaTopicUtils;
import org.apache.flink.streaming.connectors.kafka.api.simple.MessageWithMetadata;
import org.apache.flink.streaming.connectors.kafka.api.simple.PersistentKafkaSource;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.BeginningOffset;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.CurrentOffset;
import org.apache.flink.streaming.connectors.kafka.api.simple.offset.KafkaOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.NotLeaderForPartitionException;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

/**
 * Iterates the records received from a partition of a Kafka topic as byte arrays.
 *
 * This code is in parts based on https://gist.github.com/ashrithr/5811266.
 */
public class KafkaSinglePartitionIterator implements KafkaConsumerIterator, Serializable {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(KafkaSinglePartitionIterator.class);

	private List<String> hosts;
	private String topic;
	private int partition;
	private long readOffset;
	private transient SimpleConsumer consumer;
	private List<String> replicaBrokers;
	private String clientName;
	private Broker leadBroker;
	private final ConsumerConfig consumerConfig;

	private KafkaOffset initialOffset;
	private transient Iterator<MessageAndOffset> iter;
	private transient FetchResponse fetchResponse;

	/**
	 * Constructor with configurable wait time on empty fetch. For connecting to the Kafka service
	 * we use the so called simple or low level Kafka API thus directly connecting to one of the brokers.
	 *
	 * @param topic
	 * 		Name of the topic to listen to
	 * @param partition
	 * 		Partition in the chosen topic
	 * @param initialOffset
	 * 		Offset to start consuming at
	 * @param kafkaTopicUtils
	 * 		Util for receiving topic metadata
	 */
	public KafkaSinglePartitionIterator(String topic, int partition, KafkaOffset initialOffset,
			KafkaTopicUtils kafkaTopicUtils, ConsumerConfig consumerConfig) {

		Set<String> brokerAddresses = kafkaTopicUtils.getBrokerAddresses(topic, partition);
		this.hosts = new ArrayList<String>(brokerAddresses);

		this.consumerConfig = consumerConfig;
		this.topic = topic;
		this.partition = partition;

		this.initialOffset = initialOffset;

		this.replicaBrokers = new ArrayList<String>();
	}

	// --------------------------------------------------------------------------------------------
	//  Initializing a connection
	// --------------------------------------------------------------------------------------------

	/**
	 * Initializes the connection by detecting the leading broker of
	 * the topic and establishing a connection to it.
	 */
	public void initialize() {
		if (LOG.isInfoEnabled()) {
			LOG.info("Initializing consumer {} / {} with hosts {}", topic, partition, hosts);
		}

		PartitionMetadata metadata = getPartitionMetadata();

		leadBroker = metadata.leader();
		clientName = "Client_" + topic + "_" + partition;

		consumer = new SimpleConsumer(leadBroker.host(), leadBroker.port(), consumerConfig.socketTimeoutMs(), consumerConfig.socketReceiveBufferBytes(), clientName);

		try {
			readOffset = initialOffset.getOffset(consumer, topic, partition, clientName);
		} catch (NotLeaderForPartitionException e) {
			throw new RuntimeException("Unable to get offset",e);
		}

		try {
			resetFetchResponse(readOffset);
		} catch (ClosedChannelException e) {
			if (LOG.isWarnEnabled()) {
				LOG.warn("Got ClosedChannelException, trying to find new leader.");
			}
			findNewLeader();
		}
	}

	private PartitionMetadata getPartitionMetadata() {
		PartitionMetadata metadata;
		int retry = 0;
		int waitTime = consumerConfig.props().getInt(PersistentKafkaSource.WAIT_ON_FAILED_LEADER_MS_KEY, PersistentKafkaSource.WAIT_ON_FAILED_LEADER__MS_DEFAULT);
		do {
			metadata = findLeader(hosts, topic, partition);
			/*try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				throw new RuntimeException("Establishing connection to Kafka failed", e);
			} */
			if(metadata == null) {
				retry++;
				if(retry == consumerConfig.props().getInt(PersistentKafkaSource.MAX_FAILED_LEADER_RETRIES_KEY, PersistentKafkaSource.MAX_FAILED_LEADER_RETRIES_DEFAULT)) {
					throw new RuntimeException("Tried finding a leader "+retry+" times without success");
				}
				LOG.warn("Unable to get leader and partition metadata. Waiting {} ms until retrying. Retries so far {}",waitTime, retry-1);
				try {
					Thread.sleep(waitTime);
				} catch (InterruptedException e) {
					throw new RuntimeException("Establishing connection to Kafka failed", e);
				}
			}
		} while (metadata == null);

		if (metadata.leader() == null) {
			throw new RuntimeException("Can't find Leader for Topic and Partition. (at " + hosts + ")");
		}

		return metadata;
	}

	/**
	 * Sets the partition to read from.
	 *
	 * @param partition
	 * 		partition number
	 */
	public void setPartition(int partition) {
		this.partition = partition;
	}

	// --------------------------------------------------------------------------------------------
	//  Iterator methods
	// --------------------------------------------------------------------------------------------

	/**
	 * Convenience method to emulate iterator behaviour.
	 *
	 * @return whether the iterator has a next element
	 */
	public boolean hasNext() {
		return true;
	}

	/**
	 * Returns the next message received from Kafka as a
	 * byte array.
	 *
	 * @return next message as a byte array.
	 */
	public byte[] next() {
		return nextWithOffset().getMessage();
	}

	public boolean fetchHasNext() {
		synchronized (fetchResponse) {
			if (!iter.hasNext()) {
				try {
					resetFetchResponse(readOffset);
				} catch (ClosedChannelException e) {
					if (LOG.isWarnEnabled()) {
						LOG.warn("Got ClosedChannelException, trying to find new leader.", e);
					}
					findNewLeader();
				}
				return iter.hasNext();
			} else {
				return true;
			}
		}
	}

	/**
	 * Returns the next message and its offset received from
	 * Kafka encapsulated in a POJO.
	 *
	 * @return next message and its offset.
	 */
	public MessageWithMetadata nextWithOffset() {

		synchronized (fetchResponse) {
			if (!iter.hasNext()) {
				throw new RuntimeException(
						"Trying to read when response is not fetched. Call fetchHasNext() first.");
			}

			MessageAndOffset messageAndOffset = iter.next();
			long currentOffset = messageAndOffset.offset();

			while (currentOffset < readOffset) {
				messageAndOffset = iter.next();
				currentOffset = messageAndOffset.offset();
			}

			readOffset = messageAndOffset.nextOffset();
			ByteBuffer payload = messageAndOffset.message().payload();

			byte[] bytes = new byte[payload.limit()];
			payload.get(bytes);

			return new MessageWithMetadata(messageAndOffset.offset(), bytes, partition);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Internal utilities
	// --------------------------------------------------------------------------------------------

	private void resetFetchResponse(long offset) throws ClosedChannelException {
		FetchRequest req = new FetchRequestBuilder().clientId(clientName)
				.addFetch(topic, partition, offset, consumerConfig.fetchMessageMaxBytes()).build();

		fetchResponse = consumer.fetch(req);

		if (fetchResponse.hasError()) {
			short code = fetchResponse.errorCode(topic, partition);

			if (LOG.isErrorEnabled()) {
				LOG.error("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
			}

			if (code == ErrorMapping.OffsetOutOfRangeCode()) {
				if (LOG.isErrorEnabled()) {
					LOG.error("Asked for invalid offset {}", offset);
				}
				String reset = consumerConfig.autoOffsetReset();
				if(reset.equals("smallest")) {
					LOG.info("Setting read offset to beginning (smallest)");
					readOffset = new BeginningOffset().getOffset(consumer, topic, partition, clientName);
				} else if(reset.equals("largest")) {
					LOG.info("Setting read offset to current offset (largest)");
					readOffset = new CurrentOffset().getOffset(consumer, topic, partition, clientName);
				} else {
					throw new RuntimeException("Unknown 'autooffset.reset' mode '"+reset+"' Supported values are 'smallest' and 'largest'.");
				}
			}

			findNewLeader();
		}

		iter = fetchResponse.messageSet(topic, partition).iterator();
	}

	private void findNewLeader() {
		consumer.close();
		consumer = null;
		leadBroker = findNewLeader(leadBroker, topic, partition);
		consumer = new SimpleConsumer(leadBroker.host(), leadBroker.port(), consumerConfig.socketTimeoutMs(), consumerConfig.socketReceiveBufferBytes(), clientName);
	}

	private PartitionMetadata findLeader(List<String> addresses, String topic, int partition) {

		PartitionMetadata returnMetaData = null;
		loop:
		for (String address : addresses) {

			if (LOG.isDebugEnabled()) {
				LOG.debug("Trying to find leader via broker: {}", address);
			}

			String[] split = address.split(":");
			String host = split[0];
			int port = Integer.parseInt(split[1]);

			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(host, port, consumerConfig.socketTimeoutMs(), consumerConfig.socketReceiveBufferBytes(), "leaderLookup");
				List<String> topics = Collections.singletonList(topic);

				TopicMetadataRequest req = new TopicMetadataRequest(topics);

				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

				List<TopicMetadata> metaData = resp.topicsMetadata();
				for (TopicMetadata item : metaData) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == partition) {
							returnMetaData = part;
							break loop;
						}
					}
				}
			} catch (Exception e) {
				if (e instanceof ClosedChannelException) {
					LOG.warn("Got ClosedChannelException while trying to communicate with Broker" +
							"[{}] to find Leader for [{}, {}]. Trying other replicas.", address, topic, partition);
				} else {
					throw new RuntimeException("Error communicating with Broker [" + address + "] to find Leader for [" + topic + ", " + partition + "]", e);
				}
			} finally {
				if (consumer != null) {
					consumer.close();
				}
			}
		}
		if (returnMetaData != null) {
			replicaBrokers.clear();
			for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
				replicaBrokers.add(replica.host() + ":" + replica.port());
			}
		}
		return returnMetaData;
	}

	private Broker findNewLeader(Broker oldLeader, String topic, int a_partition) {
		for (int i = 0; i < 3; i++) {
			if (LOG.isInfoEnabled()) {
				LOG.info("Trying to find a new leader after Broker failure.");
			}
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeader(replicaBrokers, topic, a_partition);
			if (metadata == null) {
				goToSleep = true;
			} else if (metadata.leader() == null) {
				goToSleep = true;
			} else if (oldLeader.host().equalsIgnoreCase(metadata.leader().host()) && i == 0) {
				// first time through if the leader hasn't changed give ZooKeeper a second to recover
				// second time, assume the broker did recover before failover, or it was a non-Broker issue
				//
				goToSleep = true;
			} else {
				return metadata.leader();
			}
			if (goToSleep) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException ie) {
				}
			}
		}
		throw new RuntimeException("Unable to find new leader after Broker failure.");
	}

	public int getId() {
		return this.partition;
	}

	@Override
	public String toString() {
		return "SinglePartitionIterator{partition="+partition+" readOffset="+readOffset+"}";
	}
}
