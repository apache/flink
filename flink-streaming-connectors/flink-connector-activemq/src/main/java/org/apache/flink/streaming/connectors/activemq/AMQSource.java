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

public class AMQSource<OUT> extends MultipleIdsMessageAcknowledgingSourceBase<OUT, String, Long>
	implements ResultTypeQueryable<OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(AMQSource.class);

	private final ActiveMQConnectionFactory connectionFactory;
	private final String queueName;
	private final DeserializationSchema<OUT> deserializationSchema;
	private boolean logFailuresOnly = false;
	private transient RunningChecker runningChecker;
	private transient Connection connection;
	private transient Session session;
	private transient MessageConsumer consumer;

	public AMQSource(ActiveMQConnectionFactory connectionFactory, String queueName, DeserializationSchema<OUT> deserializationSchema) {
		this(connectionFactory, queueName, deserializationSchema, new RunningCheckerImpl());
	}

	AMQSource(ActiveMQConnectionFactory connectionFactory, String queueName, DeserializationSchema<OUT> deserializationSchema, RunningChecker runningChecker) {
		super(String.class);
		this.connectionFactory = connectionFactory;
		this.queueName = queueName;
		this.deserializationSchema = deserializationSchema;
		this.runningChecker = runningChecker;
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
		runningChecker.setIsRunning(true);
	}

	@Override
	public void close() throws Exception {
		super.close();
		RuntimeException exception = null;
		try {
			consumer.close();
		} catch (JMSException e) {
			if (logFailuresOnly) {
				LOG.error("Failed to close ActiveMQ session", e);
			} else {
				exception = new RuntimeException("Failed to close ActiveMQ consumer", e);
			}
		}
		try {
			session.close();
		} catch (JMSException e) {
			if (logFailuresOnly) {
				LOG.error("Failed to close ActiveMQ session", e);
			} else {
				exception = exception == null ? new RuntimeException("Failed to close ActiveMQ session", e)
					                          : exception;
			}

		}
		try {
			connection.close();
		} catch (JMSException e) {
			if (logFailuresOnly) {
				LOG.error("Failed to close ActiveMQ session", e);
			} else {
				exception = exception == null ? new RuntimeException("Failed to close ActiveMQ connection", e)
					                          : exception;
			}
		}

		if (exception != null) {
			throw exception;
		}
	}

	@Override
	protected void acknowledgeSessionIDs(List<Long> longs) {

	}

	@Override
	public void run(SourceContext<OUT> ctx) throws Exception {
		while (runningChecker.isRunning()) {
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
		runningChecker.setIsRunning(false);
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return deserializationSchema.getProducedType();
	}

	interface RunningChecker {
		boolean isRunning();
		void setIsRunning(boolean isRunning);
	}

	private static class RunningCheckerImpl implements RunningChecker {

		private volatile boolean isRunning = false;

		@Override
		public boolean isRunning() {
			return isRunning;
		}

		public void setIsRunning(boolean isRunning) {
			this.isRunning = isRunning;
		}
	}
}
