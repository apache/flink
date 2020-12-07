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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.util.Preconditions;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * A Sink for publishing data into RabbitMQ.
 * @param <IN>
 */
public class RMQSink<IN> extends RichSinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RMQSink.class);

	@Nullable
	protected final String queueName;

	private final RMQConnectionConfig rmqConnectionConfig;
	protected transient Connection connection;
	protected transient Channel channel;
	protected SerializationSchema<IN> schema;
	private boolean logFailuresOnly = false;

	@Nullable
	private final RMQSinkPublishOptions<IN> publishOptions;

	@Nullable
	private final SerializableReturnListener returnListener;

	/**
	 * @param rmqConnectionConfig The RabbitMQ connection configuration {@link RMQConnectionConfig}.
	 * @param queueName The queue to publish messages to.
	 * @param schema A {@link SerializationSchema} for turning the Java objects received into bytes
	 * @param publishOptions A {@link RMQSinkPublishOptions} for providing message's routing key and/or properties
	 * @param returnListener A SerializableReturnListener implementation object to handle returned message event
     */
	private RMQSink(
			RMQConnectionConfig rmqConnectionConfig,
			@Nullable String queueName,
			SerializationSchema<IN> schema,
			@Nullable RMQSinkPublishOptions<IN> publishOptions,
			@Nullable SerializableReturnListener returnListener) {
		this.rmqConnectionConfig = rmqConnectionConfig;
		this.queueName = queueName;
		this.schema = schema;
		this.publishOptions = publishOptions;
		this.returnListener = returnListener;
	}

	/**
	 * @param rmqConnectionConfig The RabbitMQ connection configuration {@link RMQConnectionConfig}.
	 * @param queueName The queue to publish messages to.
	 * @param schema A {@link SerializationSchema} for turning the Java objects received into bytes
     */
	@PublicEvolving
	public RMQSink(RMQConnectionConfig rmqConnectionConfig, String queueName, SerializationSchema<IN> schema) {
		this(rmqConnectionConfig, queueName, schema, null, null);
	}

	/**
	 * @param rmqConnectionConfig The RabbitMQ connection configuration {@link RMQConnectionConfig}.
	 * @param schema A {@link SerializationSchema} for turning the Java objects received into bytes
	 * @param publishOptions A {@link RMQSinkPublishOptions} for providing message's routing key and/or properties
	 * In this case the computeMandatoy or computeImmediate MUST return false otherwise an
	 * IllegalStateException is raised during runtime.
     */
	@PublicEvolving
	public RMQSink(RMQConnectionConfig rmqConnectionConfig, SerializationSchema<IN> schema,
			RMQSinkPublishOptions<IN> publishOptions) {
		this(rmqConnectionConfig, null, schema, publishOptions, null);
	}

	/**
	 * @param rmqConnectionConfig The RabbitMQ connection configuration {@link RMQConnectionConfig}.
	 * @param schema A {@link SerializationSchema} for turning the Java objects received into bytes
	 * @param publishOptions A {@link RMQSinkPublishOptions} for providing message's routing key and/or properties
	 * @param returnListener A SerializableReturnListener implementation object to handle returned message event
     */
	@PublicEvolving
	public RMQSink(RMQConnectionConfig rmqConnectionConfig, SerializationSchema<IN> schema,
			RMQSinkPublishOptions<IN> publishOptions, SerializableReturnListener returnListener) {
		this(rmqConnectionConfig, null, schema, publishOptions, returnListener);
	}

	/**
	 * Sets up the queue. The default implementation just declares the queue. The user may override
	 * this method to have a custom setup for the queue (i.e. binding the queue to an exchange or
	 * defining custom queue parameters)
	 */
	protected void setupQueue() throws IOException {
		if (queueName != null) {
			Util.declareQueueDefaults(channel, queueName);
		}
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

	/**
	 * Initializes the connection to RMQ with a default connection factory. The user may override
	 * this method to setup and configure their own {@link ConnectionFactory}.
	 */
	protected ConnectionFactory setupConnectionFactory() throws Exception {
		return rmqConnectionConfig.getConnectionFactory();
	}

	/**
	 * Initializes the connection to RMQ using the default connection factory from {@link #setupConnectionFactory()}.
	 * The user may override this method to setup and configure their own {@link Connection}.
	 */
	protected Connection setupConnection() throws Exception {
		return setupConnectionFactory().newConnection();
	}

	@Override
	public void open(Configuration config) throws Exception {
		schema.open(RuntimeContextInitializationContextAdapters.serializationAdapter(
				getRuntimeContext(),
				metricGroup -> metricGroup.addGroup("user")
		));

		try {
			connection = setupConnection();
			channel = connection.createChannel();
			if (channel == null) {
				throw new RuntimeException("None of RabbitMQ channels are available");
			}
			setupQueue();
			if (returnListener != null) {
				channel.addReturnListener(returnListener);
			}
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

			if (publishOptions == null) {
				channel.basicPublish("", queueName, null, msg);
			} else {
				boolean mandatory = publishOptions.computeMandatory(value);
				boolean immediate = publishOptions.computeImmediate(value);

				Preconditions.checkState(!(returnListener == null && (mandatory || immediate)),
					"Setting mandatory and/or immediate flags to true requires a ReturnListener.");

				String rk = publishOptions.computeRoutingKey(value);
				String exchange = publishOptions.computeExchange(value);

				channel.basicPublish(exchange, rk, mandatory, immediate,
					publishOptions.computeProperties(value), msg);
			}
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
		Exception t = null;
		try {
			if (channel != null) {
				channel.close();
			}
		} catch (IOException | TimeoutException e) {
			t = e;
		}

		try {
			if (connection != null) {
				connection.close();
			}
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
