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

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.flink.streaming.api.function.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.util.SerializationSchema;

public class KafkaSink<IN, OUT> extends RichSinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	private kafka.javaapi.producer.Producer<Integer, OUT> producer;
	private Properties props;
	private String topicId;
	private String brokerAddr;
	private boolean initDone = false;
	private SerializationSchema<IN, OUT> scheme;

	public KafkaSink(String topicId, String brokerAddr,
			SerializationSchema<IN, OUT> serializationSchema) {
		this.topicId = topicId;
		this.brokerAddr = brokerAddr;
		this.scheme = serializationSchema;

	}

	/**
	 * Initializes the connection to Kafka.
	 */
	public void initialize() {
		props = new Properties();

		props.put("metadata.broker.list", brokerAddr);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");

		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<Integer, OUT>(config);
		initDone = true;
	}

	/**
	 * Called when new data arrives to the sink, and forwards it to Kafka.
	 * 
	 * @param next
	 *            The incoming data
	 */
	@Override
	public void invoke(IN next) {
		if (!initDone) {
			initialize();
		}

		producer.send(new KeyedMessage<Integer, OUT>(topicId, scheme.serialize(next)));

	}

	@Override
	public void close() {
		producer.close();
	}

}
