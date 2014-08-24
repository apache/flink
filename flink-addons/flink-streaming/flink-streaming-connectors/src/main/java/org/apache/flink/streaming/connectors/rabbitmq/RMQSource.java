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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.function.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public abstract class RMQSource<OUT> extends RichSourceFunction<OUT> {
	private static final long serialVersionUID = 1L;

	private static final Log LOG = LogFactory.getLog(RMQSource.class);

	private final String QUEUE_NAME;
	private final String HOST_NAME;
	private boolean closeWithoutSend = false;
	private boolean sendAndClose = false;

	private transient ConnectionFactory factory;
	private transient Connection connection;
	private transient Channel channel;
	private transient QueueingConsumer consumer;
	private transient QueueingConsumer.Delivery delivery;

	OUT outTuple;

	public RMQSource(String HOST_NAME, String QUEUE_NAME) {
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
	public void invoke(Collector<OUT> collector) throws Exception {
		initializeConnection();

		while (true) {

			try {
				delivery = consumer.nextDelivery();
			} catch (Exception e) {
				if (LOG.isErrorEnabled()) {
					LOG.error("Cannot receive RMQ message " + QUEUE_NAME + " at " + HOST_NAME);
				}
			}

			outTuple = deserialize(delivery.getBody());
			if (closeWithoutSend) {
				break;
			} else {
				collector.collect(outTuple);
			}
			if (sendAndClose) {
				break;
			}
		}

		try {
			connection.close();
		} catch (IOException e) {
			throw new RuntimeException("Error while closing RMQ connection with " + QUEUE_NAME + " at "
					+ HOST_NAME, e);
		}

	}

	/**
	 * Deserializes the incoming data.
	 * 
	 * @param message
	 *            The incoming message in a byte array
	 * @return The deserialized message in the required format.
	 */
	public abstract OUT deserialize(byte[] message);

	/**
	 * Closes the connection immediately and no further data will be sent.
	 */
	public void closeWithoutSend() {
		closeWithoutSend = true;
	}

	/**
	 * Closes the connection only when the next message is sent after this call.
	 */
	public void sendAndClose() {
		sendAndClose = true;
	}

}
