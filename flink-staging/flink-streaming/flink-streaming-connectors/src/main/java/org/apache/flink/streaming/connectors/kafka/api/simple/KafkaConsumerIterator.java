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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

/**
 * Iterates the records received from a partition of a Kafka topic as byte arrays.
 */
public class KafkaConsumerIterator implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final long DEFAULT_WAIT_ON_EMPTY_FETCH = 1000L;

	private List<String> hosts;
	private String topic;
	private int port;
	private int partition;
	private long readOffset;
	private long waitOnEmptyFetch;
	private transient SimpleConsumer consumer;
	private List<String> replicaBrokers;
	private String clientName;

	private transient Iterator<MessageAndOffset> iter;
	private transient FetchResponse fetchResponse;

	/**
	 * Constructor with configurable wait time on empty fetch. For connecting to the Kafka service
	 * we use the so called simple or low level Kafka API thus directly connecting to one of the brokers.
	 *
	 * @param hostName Hostname of a known Kafka broker
	 * @param port Port of the known Kafka broker
	 * @param topic Name of the topic to listen to
	 * @param partition Partition in the chosen topic
	 * @param waitOnEmptyFetch wait time on empty fetch in millis
	 */
	public KafkaConsumerIterator(String hostName, int port, String topic, int partition,
			long waitOnEmptyFetch) {

		this.hosts = new ArrayList<String>();
		hosts.add(hostName);
		this.port = port;

		this.topic = topic;
		this.partition = partition;
		this.waitOnEmptyFetch = waitOnEmptyFetch;

		replicaBrokers = new ArrayList<String>();
	}

	/**
	 * Constructor without configurable wait time on empty fetch. For connecting to the Kafka service
	 * we use the so called simple or low level Kafka API thus directly connecting to one of the brokers.
	 *
	 * @param hostName Hostname of a known Kafka broker
	 * @param port Port of the known Kafka broker
	 * @param topic Name of the topic to listen to
	 * @param partition Partition in the chosen topic
	 */
	public KafkaConsumerIterator(String hostName, int port, String topic, int partition){
		this(hostName, port, topic, partition, DEFAULT_WAIT_ON_EMPTY_FETCH);
	}

	// --------------------------------------------------------------------------------------------
	//  Initializing a connection
	// --------------------------------------------------------------------------------------------

	/**
	 * Initializes the connection by detecting the leading broker of
	 * the topic and establishing a connection to it.
	 */
	private void initialize() throws InterruptedException {
		PartitionMetadata metadata;
		do {
			metadata = findLeader(hosts, port, topic, partition);
			if (metadata == null) {
				try {
					Thread.sleep(waitOnEmptyFetch);
				} catch (InterruptedException e) {
					throw new InterruptedException("Establishing connection to Kafka failed");
				}
			}
		} while (metadata == null);

		if (metadata.leader() == null) {
			throw new RuntimeException("Can't find Leader for Topic and Partition. (at " + hosts.get(0)
					+ ":" + port);
		}

		String leadBroker = metadata.leader().host();
		clientName = "Client_" + topic + "_" + partition;

		consumer = new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
	}

	/**
	 * Initializes a connection from the earliest available offset.
	 */
	public void initializeFromBeginning() throws InterruptedException {
		initialize();
		readOffset = getLastOffset(consumer, topic, partition,
				kafka.api.OffsetRequest.EarliestTime(), clientName);

		resetFetchResponse(readOffset);
	}

	/**
	 * Initializes a connection from the latest available offset.
	 */
	public void initializeFromCurrent() throws InterruptedException {
		initialize();
		readOffset = getLastOffset(consumer, topic, partition,
				kafka.api.OffsetRequest.LatestTime(), clientName);

		resetFetchResponse(readOffset);
	}

	/**
	 * Initializes a connection from the specified offset.
	 *
	 * @param offset Desired Kafka offset
	 */
	public void initializeFromOffset(long offset) throws InterruptedException {
		initialize();
		readOffset = offset;
		resetFetchResponse(readOffset);
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
	public byte[] next() throws InterruptedException {
		return nextWithOffset().getMessage();
	}

	/**
	 * Returns the next message and its offset received from
	 * Kafka encapsulated in a POJO.
	 *
	 * @return next message and its offset.
	 */
	public MessageWithOffset nextWithOffset() throws InterruptedException {

		synchronized (fetchResponse) {
			while (!iter.hasNext()) {
				resetFetchResponse(readOffset);
				try {
					Thread.sleep(waitOnEmptyFetch);
				} catch (InterruptedException e) {
					throw new InterruptedException("Fetching from Kafka was interrupted");
				}
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

			return new MessageWithOffset(messageAndOffset.offset(), bytes);
		}
	}

	/**
	 * Resets the iterator to a given offset.
	 *
	 * @param offset Desired Kafka offset.
	 */
	public void reset(long offset) {
		synchronized (fetchResponse) {
			readOffset = offset;
			resetFetchResponse(offset);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Internal utilities
	// --------------------------------------------------------------------------------------------

	private void resetFetchResponse(long offset) {
		FetchRequest req = new FetchRequestBuilder().clientId(clientName)
				.addFetch(topic, partition, offset, 100000).build();
		fetchResponse = consumer.fetch(req);

		//TODO deal with broker failures

		iter = fetchResponse.messageSet(topic, partition).iterator();
	}

	private PartitionMetadata findLeader(List<String> a_hosts, int a_port, String a_topic,
			int a_partition) {
		PartitionMetadata returnMetaData = null;
		loop: for (String seed : a_hosts) {
			SimpleConsumer consumer = null;
			try {
				consumer = new SimpleConsumer(seed, a_port, 100000, 64 * 1024, "leaderLookup");
				List<String> topics = Collections.singletonList(a_topic);
				TopicMetadataRequest req = new TopicMetadataRequest(topics);
				kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

				List<TopicMetadata> metaData = resp.topicsMetadata();
				for (TopicMetadata item : metaData) {
					for (PartitionMetadata part : item.partitionsMetadata()) {
						if (part.partitionId() == a_partition) {
							returnMetaData = part;
							break loop;
						}
					}
				}
			} catch (Exception e) {
				throw new RuntimeException("Error communicating with Broker [" + seed
						+ "] to find Leader for [" + a_topic + ", " + a_partition + "] Reason: "
						+ e);
			} finally {
				if (consumer != null) {
					consumer.close();
				}
			}
		}
		if (returnMetaData != null) {
			replicaBrokers.clear();
			for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
				replicaBrokers.add(replica.host());
			}
		}
		return returnMetaData;
	}

	private static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
			long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo,
				kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);

		if (response.hasError()) {
			throw new RuntimeException("Error fetching data from Kafka broker. Reason: "
					+ response.errorCode(topic, partition));
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}
}
