/**
 *
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
 *
 */

package org.apache.flink.streaming.connectors.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.function.source.RichSourceFunction;
import org.apache.flink.util.Collector;

public abstract class KafkaSource<OUT> extends RichSourceFunction<OUT> {
	private static final long serialVersionUID = 1L;

	private final String zkQuorum;
	private final String groupId;
	private final String topicId;
	private final int numThreads;
	private ConsumerConnector consumer;
	private boolean closeWithoutSend = false;
	private boolean sendAndClose = false;

	OUT outTuple;

	public KafkaSource(String zkQuorum, String groupId, String topicId, int numThreads) {
		this.zkQuorum = zkQuorum;
		this.groupId = groupId;
		this.topicId = topicId;
		this.numThreads = numThreads;
	}

	/**
	 * Initializes the connection to Kafka.
	 */
	private void initializeConnection() {
		Properties props = new Properties();
		props.put("zookeeper.connect", zkQuorum);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
	}

	/**
	 * Called to forward the data from the source to the {@link DataStream}.
	 * 
	 * @param collector
	 *            The Collector for sending data to the dataStream
	 */
	@Override
	public void invoke(Collector<OUT> collector) throws Exception {
		initializeConnection();

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topicId, numThreads);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topicId).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();

		while (it.hasNext()) {
			OUT out = deserialize(it.next().message());
			if (closeWithoutSend) {
				break;
			}
			collector.collect(out);
			if (sendAndClose) {
				break;
			}
		}
		consumer.shutdown();
	}

	/**
	 * Deserializes the incoming data.
	 * 
	 * @param message
	 *            The incoming message in a byte array
	 * @return The deserialized message in the required format.
	 */
	public abstract OUT deserialize(byte[] message);

	/**
	 * Closes the connection immediately and no further data will be sent.
	 */
	public void closeWithoutSend() {
		closeWithoutSend = true;
	}

	/**
	 * Closes the connection only when the next message is sent after this call.
	 */
	public void sendAndClose() {
		sendAndClose = true;
	}
}
