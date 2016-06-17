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

package org.apache.flink.streaming.connectors.rabbitmq.common;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;

/**
 * Common utils for RabbitMQ streaming connector.
 */
public class Utils {
	private Utils() {}

	/**
	 *	Create RabbitMQ and check for null reference.
	 *
	 * @param connection RabbitMQ connection
	 * @return RabbitMQ channel
	 * @throws IOException if failed to open RabbitMQ channel
	 * @throws RuntimeException if connection object returned null channel
	 */
	public static Channel createChannel(Connection connection) throws IOException {
		Channel channel = connection.createChannel();
		if (channel == null) {
			throw new RuntimeException("RabbitMQ connection returned null channel");
		}

		return channel;
	}
}
