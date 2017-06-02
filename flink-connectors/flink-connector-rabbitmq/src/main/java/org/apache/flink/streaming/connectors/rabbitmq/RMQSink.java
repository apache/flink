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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A Sink for publishing data into RabbitMQ.
 * @param <IN>
 */
public class RMQSink<IN> extends RichSinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RMQSink.class);

	protected final String queueName;
	private final RMQConnectionConfig rmqConnectionConfig;
	protected transient Connection connection;
	protected transient Channel channel;
	protected SerializationSchema<IN> schema;
	private boolean logFailuresOnly = false;

	/**
	 * @param rmqConnectionConfig The RabbitMQ connection configuration {@link RMQConnectionConfig}.
	 * @param queueName The queue to publish messages to.
	 * @param schema A {@link SerializationSchema} for turning the Java objects received into bytes
     */
	public RMQSink(RMQConnectionConfig rmqConnectionConfig, String queueName, SerializationSchema<IN> schema) {
		this.rmqConnectionConfig = rmqConnectionConfig;
		this.queueName = queueName;
		this.schema = schema;
	}

	/**
	 * Sets up the queue. The default implementation just declares the queue. The user may override
	 * this method to have a custom setup for the queue (i.e. binding the queue to an exchange or
	 * defining custom queue parameters)
	 */
	protected void setupQueue() throws IOException {
		channel.queueDeclare(queueName, false, false, false, null);
	}

	/**
	 * Defines whether the producer should fail on errors, or only log them.
	 * If this is set to true, then exceptions will be only logged, if set to false,
	 * exceptions will be eventually thrown and cause the streaming program to
	 * fail (and enter recovery).
	 *
	 * @param logFailuresOnly The flag to indicate logging-only on exceptions.
	 */
	public void setLogFailuresOnly(boolean logFailuresOnly) {
		this.logFailuresOnly = logFailuresOnly;
	}

	@Override
	public void open(Configuration config) throws Exception {
		ConnectionFactory factory = rmqConnectionConfig.getConnectionFactory();
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();
			if (channel == null) {
				throw new RuntimeException("None of RabbitMQ channels are available");
			}
			setupQueue();
		} catch (IOException e) {
			throw new RuntimeException("Error while creating the channel", e);
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

			channel.basicPublish("", queueName, null, msg);
		} catch (IOException e) {
			if (logFailuresOnly) {
				LOG.error("Cannot send RMQ message {} at {}", queueName, rmqConnectionConfig.getHost(), e);
			} else {
				throw new RuntimeException("Cannot send RMQ message " + queueName + " at " + rmqConnectionConfig.getHost(), e);
			}
		}

	}

	@Override
	public void close() {
		IOException t = null;
		try {
			channel.close();
		} catch (IOException e) {
			t = e;
		}

		try {
			connection.close();
		} catch (IOException e) {
			if (t != null) {
				LOG.warn("Both channel and connection closing failed. Logging channel exception and failing with connection exception", t);
			}
			t = e;
		}
		if (t != null) {
			throw new RuntimeException("Error while closing RMQ connection with " + queueName
					+ " at " + rmqConnectionConfig.getHost(), t);
		}
	}

}
