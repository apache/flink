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

// Changes made to the source, taken from com.rabbitmq:amqp-client:4.2.0:
//	- brought this class out of com.rabbitmq.client.QueueingConsumer

package org.apache.flink.streaming.connectors.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

/**
 * Encapsulates an arbitrary message - simple "bean" holder structure.
 * TODO: replace this with `com.rabbitmq.client.Delivery` in RMQ v5.x
 */
public class Delivery {
	private final Envelope envelope;
	private final AMQP.BasicProperties properties;
	private final byte[] body;

	public Delivery(Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
		this.envelope = envelope;
		this.properties = properties;
		this.body = body;
	}

	/**
	 * Retrieve the message envelope.
	 *
	 * @return the message envelope
	 */
	public Envelope getEnvelope() {
		return envelope;
	}

	/**
	 * Retrieve the message properties.
	 *
	 * @return the message properties
	 */
	public AMQP.BasicProperties getProperties() {
		return properties;
	}

	/**
	 * Retrieve the message body.
	 *
	 * @return the message body
	 */
	public byte[] getBody() {
		return body;
	}
}
