/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.api.simple;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.ConnectorSource;
import org.apache.flink.streaming.connectors.util.DeserializationSchema;
import org.apache.flink.util.Collector;

/**
 * Source that listens to a Kafka topic using the low level or simple Kafka API.
 *
 * @param <OUT>
 *            Type of the messages on the topic.
 */
public class SimpleKafkaSource<OUT> extends ConnectorSource<OUT> {
	private static final long serialVersionUID = 1L;

	private String topicId;
	private final String hostName;
	private final int port;
	private final int partition;
	protected KafkaConsumerIterator iterator;

	public SimpleKafkaSource(String topic, String hostName, int port, int partition,
								DeserializationSchema<OUT> deserializationSchema) {
		super(deserializationSchema);
		this.topicId = topic;
		this.hostName = hostName;
		this.port = port;
		this.partition = partition;
	}

	private void initializeConnection() {
		iterator = new KafkaConsumerIterator(hostName, port, topicId, partition);
	}

	protected void setInitialOffset(Configuration config) throws InterruptedException {
		iterator.initializeFromCurrent();
	}

	@Override
	public void run(Collector<OUT> collector) throws Exception {
		while (iterator.hasNext()) {
			MessageWithOffset msg = iterator.nextWithOffset();
			OUT out = schema.deserialize(msg.getMessage());
			collector.collect(out);
		}
	}

	@Override
	public void cancel() {
	}


	@Override
	public void open(Configuration config) throws InterruptedException {
		initializeConnection();
		setInitialOffset(config);
	}
}
