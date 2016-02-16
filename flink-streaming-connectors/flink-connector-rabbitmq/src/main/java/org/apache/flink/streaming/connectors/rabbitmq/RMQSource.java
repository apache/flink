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
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.MessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.functions.source.MultipleIdsMessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RabbitMQ source (consumer) which reads from a queue and acknowledges messages on checkpoints.
 * When checkpointing is enabled, it guarantees exactly-once processing semantics.
 *
 * RabbitMQ requires messages to be acknowledged. On failures, RabbitMQ will re-resend all messages
 * which have not been acknowledged previously. When a failure occurs directly after a completed
 * checkpoint, all messages part of this checkpoint might be processed again because they couldn't
 * be acknowledged before failure. This case is handled by the {@link MessageAcknowledgingSourceBase}
 * base class which deduplicates the messages using the correlation id.
 *
 * RabbitMQ's Delivery Tags do NOT represent unique ids / offsets. That's why the source uses the
 * Correlation ID in the message properties to check for duplicate messages. Note that the
 * correlation id has to be set at the producer. If the correlation id is not set, messages may
 * be produced more than once in corner cases.
 *
 * This source can be operated in three different modes:
 *
 * 1) Exactly-once (when checkpointed) with RabbitMQ transactions and messages with
 *    unique correlation IDs.
 * 2) At-least-once (when checkpointed) with RabbitMQ transactions but no deduplication mechanism
 *    (correlation id is not set).
 * 3) No strong delivery guarantees (without checkpointing) with RabbitMQ auto-commit mode.
 *
 * Users may overwrite the setupConnectionFactory() method to pass their setup their own
 * ConnectionFactory in case the constructor parameters are not sufficient.
 *
 * @param <OUT> The type of the data read from RabbitMQ.
 */
public class RMQSource<OUT> extends MultipleIdsMessageAcknowledgingSourceBase<OUT, String, Long>
		implements ResultTypeQueryable<OUT> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RMQSource.class);

	private final String hostName;
	private final Integer port;
	private final String username;
	private final String password;
	private final String queueName;
	private final boolean usesCorrelationId;
	protected DeserializationSchema<OUT> schema;

	protected transient Connection connection;
	protected transient Channel channel;
	protected transient QueueingConsumer consumer;

	protected transient boolean autoAck;

	private transient volatile boolean running;

	/**
	 * Creates a new RabbitMQ source with at-least-once message processing guarantee when
	 * checkpointing is enabled. No strong delivery guarantees when checkpointing is disabled.
	 * For exactly-once, please use the constructor
	 * {@link RMQSource#RMQSource(String, String, boolean usesCorrelationId, DeserializationSchema)},
	 * set {@param usesCorrelationId} to true and enable checkpointing.
	 * @param hostName The RabbiMQ broker's address to connect to.
	 * @param queueName  The queue to receive messages from.
	 * @param deserializationSchema A {@link DeserializationSchema} for turning the bytes received
	 *               				into Java objects.
	 */
	public RMQSource(String hostName, String queueName,
				DeserializationSchema<OUT> deserializationSchema) {
		this(hostName, null, null, null, queueName, false, deserializationSchema);
	}

	/**
	 * Creates a new RabbitMQ source. For exactly-once, you must set the correlation ids of messages
	 * at the producer. The correlation id must be unique. Otherwise the behavior of the source is
	 * undefined. In doubt, set {@param usesCorrelationId} to false. When correlation ids are not
	 * used, this source has at-least-once processing semantics when checkpointing is enabled.
	 * @param hostName The RabbitMQ broker's address to connect to.
	 * @param queueName The queue to receive messages from.
	 * @param usesCorrelationId Whether the messages received are supplied with a <b>unique</b>
	 *                          id to deduplicate messages (in case of failed acknowledgments).
	 *                          Only used when checkpointing is enabled.
	 * @param deserializationSchema A {@link DeserializationSchema} for turning the bytes received
	 *                              into Java objects.
	 */
	public RMQSource(String hostName, String queueName, boolean usesCorrelationId,
				DeserializationSchema<OUT> deserializationSchema) {
		this(hostName, null, null, null, queueName, usesCorrelationId, deserializationSchema);
	}

	/**
	 * Creates a new RabbitMQ source. For exactly-once, you must set the correlation ids of messages
	 * at the producer. The correlation id must be unique. Otherwise the behavior of the source is
	 * undefined. In doubt, set {@param usesCorrelationId} to false. When correlation ids are not
	 * used, this source has at-least-once processing semantics when checkpointing is enabled.
	 * @param hostName The RabbitMQ broker's address to connect to.
	 * @param port The RabbitMQ broker's port.
	 * @param queueName The queue to receive messages from.
	 * @param usesCorrelationId Whether the messages received are supplied with a <b>unique</b>
	 *                          id to deduplicate messages (in case of failed acknowledgments).
	 *                          Only used when checkpointing is enabled.
	 * @param deserializationSchema A {@link DeserializationSchema} for turning the bytes received
	 *                              into Java objects.
	 */
	public RMQSource(String hostName, Integer port,
				String queueName, boolean usesCorrelationId,
				DeserializationSchema<OUT> deserializationSchema) {
		this(hostName, port, null, null, queueName, usesCorrelationId, deserializationSchema);
	}

	/**
	 * Creates a new RabbitMQ source. For exactly-once, you must set the correlation ids of messages
	 * at the producer. The correlation id must be unique. Otherwise the behavior of the source is
	 * undefined. In doubt, set {@param usesCorrelationId} to false. When correlation ids are not
	 * used, this source has at-least-once processing semantics when checkpointing is enabled.
	 * @param hostName The RabbitMQ broker's address to connect to.
	 * @param port The RabbitMQ broker's port.
	 * @param queueName The queue to receive messages from.
	 * @param usesCorrelationId Whether the messages received are supplied with a <b>unique</b>
	 *                          id to deduplicate messages (in case of failed acknowledgments).
	 *                          Only used when checkpointing is enabled.
	 * @param deserializationSchema A {@link DeserializationSchema} for turning the bytes received
	 *                              into Java objects.
	 */
	public RMQSource(String hostName, Integer port, String username, String password,
				String queueName, boolean usesCorrelationId,
				DeserializationSchema<OUT> deserializationSchema) {
		super(String.class);
		this.hostName = hostName;
		this.port = port;
		this.username = username;
		this.password = password;
		this.queueName = queueName;
		this.usesCorrelationId = usesCorrelationId;
		this.schema = deserializationSchema;
	}

	/**
	 * Initializes the connection to RMQ with a default connection factory. The user may override
	 * this method to setup and configure their own ConnectionFactory.
	 */
	protected ConnectionFactory setupConnectionFactory() {
		return new ConnectionFactory();
	}

	/**
	 * Initializes the connection to RMQ.
	 */
	private void initializeConnection() {
		ConnectionFactory factory = setupConnectionFactory();
		factory.setHost(hostName);
		if (port != null) {
			factory.setPort(port);
		}
		if (username != null) {
			factory.setUsername(username);
		}
		if (password != null) {
			factory.setPassword(password);
		}
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.queueDeclare(queueName, true, false, false, null);
			consumer = new QueueingConsumer(channel);

			RuntimeContext runtimeContext = getRuntimeContext();
			if (runtimeContext instanceof StreamingRuntimeContext
				&& ((StreamingRuntimeContext) runtimeContext).isCheckpointingEnabled()) {
				autoAck = false;
				// enables transaction mode
				channel.txSelect();
			} else {
				autoAck = true;
			}

			LOG.debug("Starting RabbitMQ source with autoAck status: " + autoAck);
			channel.basicConsume(queueName, autoAck, consumer);

		} catch (IOException e) {
			throw new RuntimeException("Cannot create RMQ connection with " + queueName + " at "
					+ hostName, e);
		}
	}

	@Override
	public void open(Configuration config) throws Exception {
		super.open(config);
		initializeConnection();
		running = true;
	}

	@Override
	public void close() throws Exception {
		super.close();
		try {
			connection.close();
		} catch (IOException e) {
			throw new RuntimeException("Error while closing RMQ connection with " + queueName
					+ " at " + hostName, e);
		}
	}


	@Override
	public void run(SourceContext<OUT> ctx) throws Exception {
		while (running) {
			QueueingConsumer.Delivery delivery = consumer.nextDelivery();

			synchronized (ctx.getCheckpointLock()) {

				OUT result = schema.deserialize(delivery.getBody());

				if (schema.isEndOfStream(result)) {
					break;
				}

				if (!autoAck) {
					final long deliveryTag = delivery.getEnvelope().getDeliveryTag();
					if (usesCorrelationId) {
						final String correlationId = delivery.getProperties().getCorrelationId();
						Preconditions.checkNotNull(correlationId, "RabbitMQ source was instantiated " +
							"with usesCorrelationId set to true but a message was received with " +
							"correlation id set to null!");
						if (!addId(correlationId)) {
							// we have already processed this message
							continue;
						}
					}
					sessionIds.add(deliveryTag);
				}

				ctx.collect(result);
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
	}

	@Override
	protected void acknowledgeSessionIDs(List<Long> sessionIds) {
		try {
			for (long id : sessionIds) {
				channel.basicAck(id, false);
			}
			channel.txCommit();
		} catch (IOException e) {
			throw new RuntimeException("Messages could not be acknowledged during checkpoint creation.", e);
		}
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return schema.getProducedType();
	}
}
