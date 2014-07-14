/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.streamrecord.ArrayStreamRecord;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

/**
 * Source for reading messages from a Kafka queue. The source currently only
 * support string messages. Other types will be added soon.
 * 
 */
public class KafkaSource extends UserSourceInvokable {
	private static final long serialVersionUID = 1L;

	private final String zkQuorum;
	private final String groupId;
	private final String topicId;
	private final int numThreads;
	private ConsumerConnector consumer;

	StreamRecord record = new ArrayStreamRecord(1);

	public KafkaSource(String zkQuorum, String groupId, String topicId,
			int numThreads) {
		this.zkQuorum = zkQuorum;
		this.groupId = groupId;
		this.topicId = topicId;
		this.numThreads = numThreads;
	}

	private void initializeConnection() {
		Properties props = new Properties();
		props.put("zookeeper.connect", zkQuorum);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");
		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(new ConsumerConfig(props));
	}

	@Override
	public void invoke() {
		initializeConnection();

		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topicId, numThreads);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);
		KafkaStream<byte[], byte[]> stream = consumerMap.get(topicId).get(0);
		ConsumerIterator<byte[], byte[]> it = stream.iterator();

		while (it.hasNext()) {
			String message = new String(it.next().message());
			if (message.equals("q")) {
				break;
			}
			record.getTuple(0).setField(message, 0);
			emit(record);
		}

	}
}
