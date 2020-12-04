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

import com.rabbitmq.client.ConnectionFactory;
import org.junit.Test;

import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link RMQConnectionConfig}.
 */
public class RMQConnectionConfigTest {

	@Test(expected = NullPointerException.class)
	public void shouldThrowNullPointExceptionIfHostIsNull() throws NoSuchAlgorithmException,
		KeyManagementException, URISyntaxException {
		RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
			.setPort(1000).setUserName("guest")
			.setPassword("guest").setVirtualHost("/").build();
		connectionConfig.getConnectionFactory();
	}

	@Test(expected = NullPointerException.class)
	public void shouldThrowNullPointExceptionIfPortIsNull() throws NoSuchAlgorithmException,
		KeyManagementException, URISyntaxException {
		RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
			.setHost("localhost").setUserName("guest")
			.setPassword("guest").setVirtualHost("/").build();
		connectionConfig.getConnectionFactory();
	}

	@Test(expected = NullPointerException.class)
	public void shouldSetDefaultValueIfConnectionTimeoutNotGiven() throws NoSuchAlgorithmException,
		KeyManagementException, URISyntaxException {
		RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
			.setHost("localhost").setUserName("guest")
			.setPassword("guest").setVirtualHost("/").build();
		ConnectionFactory factory = connectionConfig.getConnectionFactory();
		assertEquals(ConnectionFactory.DEFAULT_CONNECTION_TIMEOUT, factory.getConnectionTimeout());
	}

	@Test
	public void shouldSetProvidedValueIfConnectionTimeoutNotGiven() throws NoSuchAlgorithmException,
		KeyManagementException, URISyntaxException {
		RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
			.setHost("localhost").setPort(5000).setUserName("guest")
			.setPassword("guest").setVirtualHost("/")
			.setConnectionTimeout(5000).build();
		ConnectionFactory factory = connectionConfig.getConnectionFactory();
		assertEquals(5000, factory.getConnectionTimeout());
	}

	@Test
	public void shouldSetOptionalPrefetchCount() {
		RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
			.setHost("localhost")
			.setPort(5000)
			.setUserName("guest")
			.setPassword("guest")
			.setVirtualHost("/")
			.setPrefetchCount(500)
			.build();
		Optional<Integer> prefetch = connectionConfig.getPrefetchCount();
		assertTrue(prefetch.isPresent());
		assertEquals(500, (int) prefetch.get());
	}

	@Test
	public void shouldReturnEmptyOptionalPrefetchCount() {
		RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
			.setHost("localhost")
			.setPort(5000)
			.setUserName("guest")
			.setPassword("guest")
			.setVirtualHost("/")
			.build();
		Optional<Integer> prefetch = connectionConfig.getPrefetchCount();
		assertFalse(prefetch.isPresent());
	}
}
