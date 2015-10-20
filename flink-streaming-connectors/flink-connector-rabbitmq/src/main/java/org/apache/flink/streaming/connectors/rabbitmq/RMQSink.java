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
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RMQSink<IN> extends RichSinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RMQSink.class);

	private String QUEUE_NAME;
	private String HOST_NAME;
	private transient ConnectionFactory factory;
	private transient Connection connection;
	private transient Channel channel;
	private SerializationSchema<IN, byte[]> schema;

	public RMQSink(String HOST_NAME, String QUEUE_NAME, SerializationSchema<IN, byte[]> schema) {
		this.HOST_NAME = HOST_NAME;
		this.QUEUE_NAME = QUEUE_NAME;
		this.schema = schema;
	}

	/**
	 * Initializes the connection to RMQ.
	 */
	public void initializeConnection() {
		factory = new ConnectionFactory();
		factory.setHost(HOST_NAME);
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Called when new data arrives to the sink, and forwards it to RMQ.
	 * 
	 * @param value
	 *            The incoming data
	 */
	@Override
	public void invoke(IN value) {
		try {
			byte[] msg = schema.serialize(value);

			channel.basicPublish("", QUEUE_NAME, null, msg);

		} catch (IOException e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Cannot send RMQ message {} at {}", QUEUE_NAME, HOST_NAME);
			}
		}

	}

	/**
	 * Closes the connection.
	 */
	private void closeChannel() {
		try {
			channel.close();
			connection.close();
		} catch (IOException e) {
			throw new RuntimeException("Error while closing RMQ connection with " + QUEUE_NAME
					+ " at " + HOST_NAME, e);
		}

	}

	@Override
	public void open(Configuration config) {
		initializeConnection();
	}

	@Override
	public void close() {
		closeChannel();
	}

}
