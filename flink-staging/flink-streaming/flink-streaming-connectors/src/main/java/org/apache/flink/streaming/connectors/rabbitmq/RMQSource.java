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

package org.apache.flink.streaming.connectors.rabbitmq;

import java.io.IOException;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.ConnectorSource;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class RMQSource<OUT> extends ConnectorSource<OUT> {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RMQSource.class);

	private final String QUEUE_NAME;
	private final String HOST_NAME;

	private transient ConnectionFactory factory;
	private transient Connection connection;
	private transient Channel channel;
	private transient QueueingConsumer consumer;
	private transient QueueingConsumer.Delivery delivery;

	private volatile boolean isRunning = false;

	OUT out;

	public RMQSource(String HOST_NAME, String QUEUE_NAME,
			DeserializationSchema<OUT> deserializationSchema) {
		super(deserializationSchema);
		this.HOST_NAME = HOST_NAME;
		this.QUEUE_NAME = QUEUE_NAME;
	}

	/**
	 * Initializes the connection to RMQ.
	 */
	private void initializeConnection() {
		factory = new ConnectionFactory();
		factory.setHost(HOST_NAME);
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			consumer = new QueueingConsumer(channel);
			channel.basicConsume(QUEUE_NAME, true, consumer);
		} catch (IOException e) {
			throw new RuntimeException("Cannot create RMQ connection with " + QUEUE_NAME + " at "
					+ HOST_NAME, e);
		}
	}

	/**
	 * Called to forward the data from the source to the {@link DataStream}.
	 * 
	 * @param collector
	 *            The Collector for sending data to the dataStream
	 */
	@Override
	public void run(Collector<OUT> collector) throws Exception {
		isRunning = true;
		try {
			while (isRunning) {

				try {
					delivery = consumer.nextDelivery();
				} catch (Exception e) {
					if (LOG.isErrorEnabled()) {
						LOG.error("Cannot recieve RMQ message {} at {}", QUEUE_NAME, HOST_NAME);
					}
				}

				out = schema.deserialize(delivery.getBody());
				if (schema.isEndOfStream(out)) {
					break;
				} else {
					collector.collect(out);
				}
			}
		} finally {
			connection.close();
		}

	}

	@Override
	public void open(Configuration config) throws Exception {
		initializeConnection();
	}

	@Override
	public void cancel() {
		isRunning = false;
		try {
			connection.close();
		} catch (IOException e) {
			throw new RuntimeException("Error while closing RMQ connection with " + QUEUE_NAME
					+ " at " + HOST_NAME, e);
		}
	}

}
