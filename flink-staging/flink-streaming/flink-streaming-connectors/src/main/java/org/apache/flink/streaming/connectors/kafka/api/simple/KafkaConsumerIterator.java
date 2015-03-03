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

public class KafkaConsumerIterator {

	private static final long serialVersionUID = 1L;

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

	public KafkaConsumerIterator(String host, int port, String topic, int partition,
			long waitOnEmptyFetch) {

		this.hosts = new ArrayList<String>();
		hosts.add(host);
		this.port = port;

		this.topic = topic;
		this.partition = partition;
		this.waitOnEmptyFetch = waitOnEmptyFetch;

		replicaBrokers = new ArrayList<String>();
	}

	private void initialize() {
		PartitionMetadata metadata;
		do {
			metadata = findLeader(hosts, port, topic, partition);
			try {
				Thread.sleep(1000L);
			} catch (InterruptedException e) {
				e.printStackTrace();
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

	public void initializeFromBeginning() {
		initialize();
		readOffset = getLastOffset(consumer, topic, partition,
				kafka.api.OffsetRequest.EarliestTime(), clientName);

		resetFetchResponse(readOffset);
	}

	public void initializeFromCurrent() {
		initialize();
		readOffset = getLastOffset(consumer, topic, partition,
				kafka.api.OffsetRequest.LatestTime(), clientName);

		resetFetchResponse(readOffset);
	}

	public void initializeFromOffset(long offset) {
		initialize();
		readOffset = offset;
		resetFetchResponse(readOffset);
	}

	public boolean hasNext() {
		return true;
	}

	public byte[] next() {
		return nextWithOffset().getMessage();
	}

	private void resetFetchResponse(long offset) {
		FetchRequest req = new FetchRequestBuilder().clientId(clientName)
				.addFetch(topic, partition, offset, 100000).build();
		fetchResponse = consumer.fetch(req);
		iter = fetchResponse.messageSet(topic, partition).iterator();
	}

	public MessageWithOffset nextWithOffset() {

		synchronized (fetchResponse) {
			while (!iter.hasNext()) {
				resetFetchResponse(readOffset);
				try {
					Thread.sleep(waitOnEmptyFetch);
				} catch (InterruptedException e) {
					e.printStackTrace();
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

	public void reset(long offset) {
		synchronized (fetchResponse) {
			readOffset = offset;
			resetFetchResponse(offset);
		}
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
			throw new RuntimeException("Error fetching data Offset Data the Broker. Reason: "
					+ response.errorCode(topic, partition));
		}
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}
}
