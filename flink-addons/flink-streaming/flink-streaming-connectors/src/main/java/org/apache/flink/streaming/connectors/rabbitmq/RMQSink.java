/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package org.apache.flink.streaming.connectors.rabbitmq;

import java.io.IOException;

import org.apache.flink.streaming.api.function.SinkFunction;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import org.apache.flink.api.java.tuple.Tuple;

/**
 * Source for sending messages to a RabbitMQ queue. The source currently only
 * support string messages. Other types will be added soon.
 * 
 */
public abstract class RMQSink<IN extends Tuple> extends SinkFunction<IN> {
	private static final long serialVersionUID = 1L;
	private boolean close = false;

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

	public void initializeConnection() {
		factory = new ConnectionFactory();
		factory.setHost(HOST_NAME);
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		initDone = true;
	}

	@Override
	public void invoke(IN tuple) {
		if (!initDone)
			initializeConnection();

		try {
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			byte[] msg = serialize(tuple);
			channel.basicPublish("", QUEUE_NAME, null, msg);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		if (close) {
			try {
				channel.close();
				connection.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public abstract byte[] serialize(Tuple t);

	public void close() {
		close = true;
	}

}
