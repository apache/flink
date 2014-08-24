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

package org.apache.flink.streaming.connectors.rabbitmq;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.streaming.api.function.sink.SinkFunction;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public abstract class RMQSink<IN> implements SinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	private static final Log LOG = LogFactory.getLog(RMQSink.class);

	private boolean sendAndClose = false;
	private boolean closeWithoutSend = false;

	private String QUEUE_NAME;
	private String HOST_NAME;
	private transient ConnectionFactory factory;
	private transient Connection connection;
	private transient Channel channel;
	private boolean initDone = false;

	public RMQSink(String HOST_NAME, String QUEUE_NAME) {
		this.HOST_NAME = HOST_NAME;
		this.QUEUE_NAME = QUEUE_NAME;
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

		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		initDone = true;
	}

	/**
	 * Called when new data arrives to the sink, and forwards it to RMQ.
	 * 
	 * @param value
	 *            The incoming data
	 */
	@Override
	public void invoke(IN value) {
		if (!initDone) {
			initializeConnection();
		}

		try {
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			byte[] msg = serialize(value);
			if (!closeWithoutSend) {
				channel.basicPublish("", QUEUE_NAME, null, msg);
			}
		} catch (IOException e) {
			if (LOG.isErrorEnabled()) {
				LOG.error("Cannot send RMQ message " + QUEUE_NAME + " at " + HOST_NAME);
			}
		}

		if (sendAndClose) {
			closeChannel();
		}
	}

	/**
	 * Serializes tuples into byte arrays.
	 * 
	 * @param value
	 *            The tuple used for the serialization
	 * @return The serialized byte array.
	 */
	public abstract byte[] serialize(IN value);

	/**
	 * Closes the connection.
	 */
	private void closeChannel() {
		try {
			channel.close();
			connection.close();
		} catch (IOException e) {
			throw new RuntimeException("Error while closing RMQ connection with " + QUEUE_NAME + " at "
					+ HOST_NAME, e);
		}

	}

	/**
	 * Closes the connection immediately and no further data will be sent.
	 */
	public void closeWithoutSend() {
		closeChannel();
		closeWithoutSend = true;
	}

	/**
	 * Closes the connection only when the next message is sent after this call.
	 */
	public void sendAndClose() {
		sendAndClose = true;
	}

}
