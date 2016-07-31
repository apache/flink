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
import org.apache.activemq.ActiveMQSession;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.MessageAcknowledgingSourceBase;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
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
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

/**
 * Source for reading messages from an ActiveMQ queue.
 * <p>
 * To create an instance of AMQSink class one should initialize and configure an
 * instance of a connection factory that will be used to create a connection.
 * This source is waiting for incoming messages from ActiveMQ and converts them from
 * an array of bytes into an instance of the output type. If an incoming
 * message is not a message with an array of bytes, this message is ignored
 * and warning message is logged.
 *
 * @param <OUT> type of output messages
 */
public class AMQSource<OUT> extends MessageAcknowledgingSourceBase<OUT, String>
	implements ResultTypeQueryable<OUT> {

	private static final Logger LOG = LoggerFactory.getLogger(AMQSource.class);

	private final ActiveMQConnectionFactory connectionFactory;
	private final String queueName;
	private final DeserializationSchema<OUT> deserializationSchema;
	private boolean logFailuresOnly = false;
	private RunningChecker runningChecker;
	private transient Connection connection;
	private transient Session session;
	private transient MessageConsumer consumer;
	private boolean autoAck;
	private HashMap<String, Message> unaknowledgedMessages = new HashMap<>();

	/**
	 * Create AMQSource.
	 *
	 * @param connectionFactory factory that will be used to create a connection with ActiveMQ
	 * @param queueName name of an ActiveMQ queue to read from
	 * @param deserializationSchema schema to deserialize incoming messages
	 */
	public AMQSource(ActiveMQConnectionFactory connectionFactory, String queueName, DeserializationSchema<OUT> deserializationSchema) {
		this(connectionFactory, queueName, deserializationSchema, new RunningCheckerImpl());
	}

	/**
	 * Create AMQSource.
	 *
	 * @param connectionFactory factory that will be used to create a connection with ActiveMQ
	 * @param queueName name of an ActiveMQ queue to read from
	 * @param deserializationSchema schema to deserialize incoming messages
	 * @param runningChecker running checker that is used to decide if the source is still running
	 */
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
				LOG.error("Received ActiveMQ exception", e);
			}
		});

		RuntimeContext runtimeContext = getRuntimeContext();
		int acknowledgeType;
		if (runtimeContext instanceof StreamingRuntimeContext
			&& ((StreamingRuntimeContext) runtimeContext).isCheckpointingEnabled()) {
			autoAck = false;
			acknowledgeType = ActiveMQSession.INDIVIDUAL_ACKNOWLEDGE;
		} else {
			autoAck = true;
			acknowledgeType = ActiveMQSession.AUTO_ACKNOWLEDGE;
		}
		// Create a Session
		session = connection.createSession(false, acknowledgeType);

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
	protected void acknowledgeIDs(long checkpointId, List<String> UIds) {
		try {
			for (String messageId : UIds) {
				Message unacknowledgedMessage = unaknowledgedMessages.get(messageId);
				if (unacknowledgedMessage != null) {
					unacknowledgedMessage.acknowledge();
					unaknowledgedMessages.remove(messageId);
				} else {
					LOG.warn("Tried to acknowledge unknown ActiveMQ message id: {}", messageId);
				}
			}
		} catch (JMSException e) {
			if (logFailuresOnly) {
				LOG.error("Failed to acknowledge ActiveMQ message");
			} else {
				throw new RuntimeException("Failed to acknowledge ActiveMQ message");
			}
		}
	}

	@Override
	public void run(SourceContext<OUT> ctx) throws Exception {
		while (runningChecker.isRunning()) {
			Message message = consumer.receive(1000);
			synchronized (ctx.getCheckpointLock()) {
				if (message instanceof BytesMessage) {
					BytesMessage bytesMessage = (BytesMessage) message;
					byte[] bytes = new byte[(int) bytesMessage.getBodyLength()];
					bytesMessage.readBytes(bytes);
					OUT value = deserializationSchema.deserialize(bytes);
					ctx.collect(value);
					if (!autoAck) {
						addId(bytesMessage.getJMSMessageID());
						unaknowledgedMessages.put(bytesMessage.getJMSMessageID(), bytesMessage);
					}
				} else {
					LOG.warn("Active MQ source received non bytes message: {}");
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

	private static class RunningCheckerImpl implements RunningChecker, Serializable {

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
