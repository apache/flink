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

package org.apache.flink.streaming.connectors.activemq.internal;

import org.apache.flink.streaming.connectors.activemq.DestinationType;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;

/**
 * Utilities for AMQ connector
 */
public class AMQUtil {
	private AMQUtil() {}

	/**
	 * Create ActiveMQ destination (queue or topic).
	 *
	 * @param session AMQ session
	 * @param destinationType destination type to create
	 * @param destinationName name of the destination
	 * @return created destination
	 * @throws JMSException
	 */
	public static Destination getDestination(Session session, DestinationType destinationType,
											String destinationName) throws JMSException {
		switch (destinationType) {
			case QUEUE:
				return session.createQueue(destinationName);
			case TOPIC:
				return session.createTopic(destinationName);
			default:
				throw new IllegalArgumentException("Unknown destination type: " + destinationType);
		}
	}

}
