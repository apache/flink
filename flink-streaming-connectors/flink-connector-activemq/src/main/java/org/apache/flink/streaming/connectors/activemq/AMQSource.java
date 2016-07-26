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

package org.apache.flink.streaming.connectors.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.MessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.functions.source.MultipleIdsMessageAcknowledgingSourceBase;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import java.util.List;

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
public class AMQSource<OUT> extends MultipleIdsMessageAcknowledgingSourceBase<OUT, String, Long>
	implements ResultTypeQueryable<OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(AMQSource.class);

	private final ActiveMQConnectionFactory connectionFactory;
	private final String queueName;
	private final DeserializationSchema<OUT> deserializationSchema;
	private transient volatile boolean running = false;
	private transient Connection connection;
	private transient Session session;
	private transient MessageConsumer consumer;

	public AMQSource(ActiveMQConnectionFactory connectionFactory, String queueName, DeserializationSchema<OUT> deserializationSchema) {
		super(String.class);
		this.connectionFactory = connectionFactory;
		this.queueName = queueName;
		this.deserializationSchema = deserializationSchema;
	}

	@Override
	public void open(Configuration config) throws Exception {
		super.open(config);
		// Create a Connection
		connection = connectionFactory.createConnection();
		connection.start();

		connection.setExceptionListener(new ExceptionListener() {
			@Override
			public void onException(JMSException e) {

			}
		});

		// Create a Session
		session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		// Create the destination (Topic or Queue)
		Destination destination = session.createQueue(queueName);

		// Create a MessageConsumer from the Session to the Topic or
		// Queue
		consumer = session.createConsumer(destination);
		running = true;
	}

	@Override
	public void close() throws Exception {
		super.close();
		try {
			consumer.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
		try {
			session.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
		try {
			connection.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void acknowledgeSessionIDs(List<Long> longs) {

	}

	@Override
	public void run(SourceContext<OUT> ctx) throws Exception {
		while (running) {
			synchronized (ctx.getCheckpointLock()) {
				// Wait for a message
				Message message = consumer.receive(1000);

				if (message instanceof BytesMessage) {
					BytesMessage bytesMessage = (BytesMessage) message;
					byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
					bytesMessage.readBytes(bytes);
					OUT value = deserializationSchema.deserialize(bytes);
					ctx.collect(value);
				} else {
					System.out.println("Received: " + message);
				}
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return deserializationSchema.getProducedType();
	}
}
