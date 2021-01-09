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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for the {@link RMQConnectionConfig}. */
public class RMQConnectionConfigTest {

    @Test
    public void shouldThrowNullPointExceptionIfHostIsNull()
            throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        assertThrows(
                NullPointerException.class,
                () -> {
                    RMQConnectionConfig connectionConfig =
                            new RMQConnectionConfig.Builder()
                                    .setPort(1000)
                                    .setUserName("guest")
                                    .setPassword("guest")
                                    .setVirtualHost("/")
                                    .build();
                    connectionConfig.getConnectionFactory();
                });
    }

    @Test
    public void shouldThrowNullPointExceptionIfPortIsNull()
            throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        assertThrows(
                NullPointerException.class,
                () -> {
                    RMQConnectionConfig connectionConfig =
                            new RMQConnectionConfig.Builder()
                                    .setHost("localhost")
                                    .setUserName("guest")
                                    .setPassword("guest")
                                    .setVirtualHost("/")
                                    .build();
                    connectionConfig.getConnectionFactory();
                });
    }

    @Test
    public void shouldSetDefaultValueIfConnectionTimeoutNotGiven()
            throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        assertThrows(
                NullPointerException.class,
                () -> {
                    RMQConnectionConfig connectionConfig =
                            new RMQConnectionConfig.Builder()
                                    .setHost("localhost")
                                    .setUserName("guest")
                                    .setPassword("guest")
                                    .setVirtualHost("/")
                                    .build();
                    ConnectionFactory factory = connectionConfig.getConnectionFactory();
                    assertEquals(
                            ConnectionFactory.DEFAULT_CONNECTION_TIMEOUT,
                            factory.getConnectionTimeout());
                });
    }

    @Test
    public void shouldSetProvidedValueIfConnectionTimeoutNotGiven()
            throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException {
        RMQConnectionConfig connectionConfig =
                new RMQConnectionConfig.Builder()
                        .setHost("localhost")
                        .setPort(5000)
                        .setUserName("guest")
                        .setPassword("guest")
                        .setVirtualHost("/")
                        .setConnectionTimeout(5000)
                        .build();
        ConnectionFactory factory = connectionConfig.getConnectionFactory();
        assertEquals(5000, factory.getConnectionTimeout());
    }

    @Test
    public void shouldSetOptionalPrefetchCount() {
        RMQConnectionConfig connectionConfig =
                new RMQConnectionConfig.Builder()
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
        RMQConnectionConfig connectionConfig =
                new RMQConnectionConfig.Builder()
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
