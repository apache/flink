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
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.junit.Before;
import org.junit.Test;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AMQSinkTest {

	private final String DESTINATION_NAME = "queue";

	private ActiveMQConnectionFactory connectionFactory;
	private MessageProducer producer;
	private Session session;
	private Connection connection;
	private Destination destination;
	private BytesMessage message;

	private AMQSink<String> amqSink;
	private SerializationSchema<String> serializationSchema;

	@Before
	public void before() throws Exception {
		connectionFactory = mock(ActiveMQConnectionFactory.class);
		producer = mock(MessageProducer.class);
		session = mock(Session.class);
		connection = mock(Connection.class);
		destination = mock(Destination.class);
		message = mock(BytesMessage.class);

		when(connectionFactory.createConnection()).thenReturn(connection);
		when(connection.createSession(anyBoolean(), anyInt())).thenReturn(session);
//		when(session.createQueue(DESTINATION_NAME)).thenReturn((Queue) destination);
		when(session.createProducer(null)).thenReturn(producer);
		when(session.createBytesMessage()).thenReturn(message);
		serializationSchema = new SimpleStringSchema();

		AMQSinkConfig<String> config = new AMQSinkConfig.AMQSinkConfigBuilder<String>()
			.setConnectionFactory(connectionFactory)
			.setDestinationName(DESTINATION_NAME)
			.setSerializationSchema(serializationSchema)
			.build();
		amqSink = new AMQSink<>(config);
		amqSink.open(new Configuration());
	}

	@Test
	public void messageSentToProducer() throws Exception {
		byte[] expectedMessage = serializationSchema.serialize("msg");
		amqSink.invoke("msg");

		verify(producer).send(message);
		verify(message).writeBytes(expectedMessage);
	}

	@Test
	public void setPersistentDeliveryMode() throws Exception {
		AMQSinkConfig<String> config = new AMQSinkConfig.AMQSinkConfigBuilder<String>()
			.setConnectionFactory(connectionFactory)
			.setDestinationName(DESTINATION_NAME)
			.setSerializationSchema(serializationSchema)
			.setPersistentDelivery(true)
			.build();
		amqSink = new AMQSink<>(config);
		amqSink.open(new Configuration());
		verify(producer).setDeliveryMode(DeliveryMode.PERSISTENT);
	}

	@Test
	public void writeToTopic() throws Exception {
		AMQSinkConfig<String> config = new AMQSinkConfig.AMQSinkConfigBuilder<String>()
			.setConnectionFactory(connectionFactory)
			.setDestinationName(DESTINATION_NAME)
			.setSerializationSchema(serializationSchema)
			.setDestinationType(DestinationType.TOPIC)
			.build();
		amqSink = new AMQSink<>(config);
		amqSink.open(new Configuration());
		verify(session).createTopic(DESTINATION_NAME);
	}

	@Test
	public void exceptionOnSendAreNotThrown() throws Exception {
		when(session.createBytesMessage()).thenThrow(JMSException.class);
		amqSink.setLogFailuresOnly(true);

		amqSink.invoke("msg");
	}

	@Test(expected = RuntimeException.class)
	public void exceptionOnSendAreThrownByDefault() throws Exception {
		when(session.createBytesMessage()).thenThrow(JMSException.class);

		amqSink.invoke("msg");
	}

	@Test
	public void sessionAndConnectionAreClosed() throws Exception {
		amqSink.close();
		verify(session).close();
		verify(connection).close();
	}

	@Test
	public void connectionCloseExceptionIsIgnored() throws Exception {
		doThrow(new JMSException("session")).when(session).close();
		doThrow(new JMSException("connection")).when(connection).close();

		try {
			amqSink.close();
			fail("Should throw an exception");
		} catch (RuntimeException ex) {
			assertEquals("session", ex.getCause().getMessage());
		}
	}

	@Test
	public void connectionCloseExceptionIsPassed() throws Exception {
		doThrow(new JMSException("connection")).when(connection).close();

		try {
			amqSink.close();
			fail("Should throw an exception");
		} catch (RuntimeException ex) {
			assertEquals("connection", ex.getCause().getMessage());
		}
	}

	@Test
	public void exceptionDuringCloseAsIgnored() throws Exception {
		doThrow(new JMSException("session")).when(session).close();
		doThrow(new JMSException("connection")).when(connection).close();

		amqSink.setLogFailuresOnly(true);
		amqSink.close();
	}
}
