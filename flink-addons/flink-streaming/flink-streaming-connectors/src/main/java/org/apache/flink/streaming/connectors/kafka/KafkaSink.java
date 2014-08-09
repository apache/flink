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

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.flink.streaming.api.function.sink.SinkFunction;

public abstract class KafkaSink<IN, OUT> implements SinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	private kafka.javaapi.producer.Producer<Integer, OUT> producer;
	private Properties props;
	private String topicId;
	private String brokerAddr;
	private boolean sendAndClose = false;
	private boolean closeWithoutSend = false;
	private boolean initDone = false;

	public KafkaSink(String topicId, String brokerAddr) {
		this.topicId = topicId;
		this.brokerAddr = brokerAddr;

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
	 * @param value
	 *            The incoming data
	 */
	@Override
	public void invoke(IN value) {
		if (!initDone) {
			initialize();
		}

		OUT out = serialize(value);
		KeyedMessage<Integer, OUT> data = new KeyedMessage<Integer, OUT>(topicId, out);

		if (!closeWithoutSend) {
			producer.send(data);
		}

		if (sendAndClose) {
			producer.close();
		}
	}

	/**
	 * Serializes tuples into byte arrays.
	 * 
	 * @param value
	 *            The tuple used for the serialization
	 * @return The serialized byte array.
	 */
	public abstract OUT serialize(IN value);

	/**
	 * Closes the connection immediately and no further data will be sent.
	 */
	public void closeWithoutSend() {
		producer.close();
		closeWithoutSend = true;
	}

	/**
	 * Closes the connection only when the next message is sent after this call.
	 */
	public void sendAndClose() {
		sendAndClose = true;
	}

}
