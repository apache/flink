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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

public class AMQSink<IN> extends RichSinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(AMQSink.class);

	private final ActiveMQConnectionFactory connectionFactory;
	private final String queueName;
	private final SerializationSchema<IN> serializationSchema;
	private boolean logFailuresOnly = false;
	private transient MessageProducer producer;
	private transient Session session;
	private transient Connection connection;

	public AMQSink(ActiveMQConnectionFactory connectionFactory, String queueName, SerializationSchema<IN> serializationSchema) {
		this.connectionFactory = connectionFactory;
		this.queueName = queueName;
		this.serializationSchema = serializationSchema;
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
		super.open(config);
		// Create a Connection
		connection = connectionFactory.createConnection();
		connection.start();

		// Create a Session
		session = connection.createSession(false,
			Session.AUTO_ACKNOWLEDGE);

		// Create the destination (Topic or Queue)
		Destination destination = session.createQueue(queueName);

		// Create a MessageProducer from the Session to the Topic or
		// Queue
		producer = session.createProducer(destination);
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
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
			byte[] bytes = serializationSchema.serialize(value);
			BytesMessage message = session.createBytesMessage();
			message.writeBytes(bytes);
			producer.send(message);
		} catch (JMSException e) {
			if (logFailuresOnly) {
				LOG.error("Failed to send message to ActiveMQ", e);
			} else {
				throw new RuntimeException("Failed to send message to ActiveMQ", e);
			}
		}
	}

	@Override
	public void close() {
		RuntimeException t = null;
		try {
			session.close();
		} catch (JMSException e) {
			if (logFailuresOnly) {
				LOG.error("Failed to close ActiveMQ session", e);
			} else {
				t = new RuntimeException("Failed to close ActiveMQ session", e);
			}
		}

		try {
			connection.close();
		} catch (JMSException e) {
			if (logFailuresOnly) {
				LOG.error("Failed to close ActiveMQ connection", e);
			} else {
				t = t == null ? new RuntimeException("Failed to close ActiveMQ session", e)
					          : t;
			}
		}

		if (t != null) {
			throw t;
		}
	}

}
