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

package org.apache.flink.streaming.connectors.kafka;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.ConnectorSource;
import org.apache.flink.streaming.connectors.util.DeserializationSchema;
import org.apache.flink.util.Collector;

public class KafkaSource<OUT> extends ConnectorSource<OUT> {
	private static final long serialVersionUID = 1L;

	private final String zkQuorum;
	private final String groupId;
	private final String topicId;
	private ConsumerConnector consumer;

	OUT outTuple;

	public KafkaSource(String zkQuorum, String groupId, String topicId,
			DeserializationSchema<OUT> deserializationSchema) {
		super(deserializationSchema);
		this.zkQuorum = zkQuorum;
		this.groupId = groupId;
		this.topicId = topicId;
	}

	/**
	 * Initializes the connection to Kafka.
	 */
	private void initializeConnection() {
		Properties props = new Properties();
		props.put("zookeeper.connect", zkQuorum);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "2000");
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

		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(Collections.singletonMap(topicId, 1));

		KafkaStream<byte[], byte[]> stream = consumerMap.get(topicId).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();

		while (it.hasNext()) {
			OUT out = schema.deserialize(it.next().message());
			if (schema.isEndOfStream(out)) {
				break;
			}
			collector.collect(out);
		}
		consumer.shutdown();
	}

	@Override
	public void open(Configuration config) {
		initializeConnection();
	}
}
