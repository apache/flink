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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

public abstract class RMQSource<IN extends Tuple> extends SourceFunction<IN> {
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

	IN outTuple;

	public RMQSource(String HOST_NAME, String QUEUE_NAME) {
		this.HOST_NAME = HOST_NAME;
		this.QUEUE_NAME = QUEUE_NAME;
	}

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
			new RuntimeException("Cannot create RMQ connection with " + QUEUE_NAME + " at "
					+ HOST_NAME, e);
		}
	}

	@Override
	public void invoke(Collector<IN> collector) throws Exception {
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
			new RuntimeException("Error while closing RMQ connection with " + QUEUE_NAME + " at "
					+ HOST_NAME, e);
		}

	}

	public abstract IN deserialize(byte[] t);

	public void closeWithoutSend() {
		closeWithoutSend = true;
	}

	public void sendAndClose() {
		sendAndClose = true;
	}

}
