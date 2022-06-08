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

import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Tests for the {@link RMQConnectionConfig}. */
class RMQConnectionConfigTest {

    @Test
    void shouldThrowNullPointExceptionIfHostIsNull() {
        assertThatThrownBy(
                        () -> {
                            RMQConnectionConfig connectionConfig =
                                    new RMQConnectionConfig.Builder()
                                            .setPort(1000)
                                            .setUserName("guest")
                                            .setPassword("guest")
                                            .setVirtualHost("/")
                                            .build();
                            connectionConfig.getConnectionFactory();
                        })
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldThrowNullPointExceptionIfPortIsNull() {
        assertThatThrownBy(
                        () -> {
                            RMQConnectionConfig connectionConfig =
                                    new RMQConnectionConfig.Builder()
                                            .setHost("localhost")
                                            .setUserName("guest")
                                            .setPassword("guest")
                                            .setVirtualHost("/")
                                            .build();
                            connectionConfig.getConnectionFactory();
                        })
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldSetDefaultValueIfConnectionTimeoutNotGiven() {
        assertThatThrownBy(
                        () -> {
                            RMQConnectionConfig connectionConfig =
                                    new RMQConnectionConfig.Builder()
                                            .setHost("localhost")
                                            .setUserName("guest")
                                            .setPassword("guest")
                                            .setVirtualHost("/")
                                            .build();
                            ConnectionFactory factory = connectionConfig.getConnectionFactory();
                            assertThat(factory.getConnectionTimeout())
                                    .isEqualTo(ConnectionFactory.DEFAULT_CONNECTION_TIMEOUT);
                        })
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldSetProvidedValueIfConnectionTimeoutNotGiven()
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
        assertThat(factory.getConnectionTimeout()).isEqualTo(5000);
    }

    @Test
    void shouldSetOptionalPrefetchCount() {
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
        assertThat(prefetch).isPresent();
        assertThat((int) prefetch.get()).isEqualTo(500);
    }

    @Test
    void shouldReturnEmptyOptionalPrefetchCount() {
        RMQConnectionConfig connectionConfig =
                new RMQConnectionConfig.Builder()
                        .setHost("localhost")
                        .setPort(5000)
                        .setUserName("guest")
                        .setPassword("guest")
                        .setVirtualHost("/")
                        .build();
        Optional<Integer> prefetch = connectionConfig.getPrefetchCount();
        assertThat(prefetch).isNotPresent();
    }

    @Test
    void shouldSetDeliveryTimeout() {
        RMQConnectionConfig.Builder builder =
                new RMQConnectionConfig.Builder()
                        .setHost("localhost")
                        .setPort(5000)
                        .setUserName("guest")
                        .setPassword("guest")
                        .setVirtualHost("/");
        RMQConnectionConfig connectionConfig = builder.setDeliveryTimeout(10000).build();
        assertThat(connectionConfig.getDeliveryTimeout()).isEqualTo(10000);

        connectionConfig = builder.setDeliveryTimeout(10, TimeUnit.SECONDS).build();
        assertThat(connectionConfig.getDeliveryTimeout()).isEqualTo(10000);
    }

    @Test
    void shouldReturnDefaultDeliveryTimeout() {
        RMQConnectionConfig connectionConfig =
                new RMQConnectionConfig.Builder()
                        .setHost("localhost")
                        .setPort(5000)
                        .setUserName("guest")
                        .setPassword("guest")
                        .setVirtualHost("/")
                        .build();
        assertThat(connectionConfig.getDeliveryTimeout()).isEqualTo(30000);
    }

    @Test
    void shouldThrowIllegalArgumentExceptionIfDeliveryTimeoutIsNegative() {
        assertThatThrownBy(
                        () ->
                                new RMQConnectionConfig.Builder()
                                        .setHost("localhost")
                                        .setPort(1000)
                                        .setUserName("guest")
                                        .setPassword("guest")
                                        .setVirtualHost("/")
                                        .setDeliveryTimeout(-1)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowIllegalArgumentExceptionIfDeliveryTimeoutWithUnitIsNegative() {
        assertThatThrownBy(
                        () ->
                                new RMQConnectionConfig.Builder()
                                        .setHost("localhost")
                                        .setPort(1000)
                                        .setUserName("guest")
                                        .setPassword("guest")
                                        .setVirtualHost("/")
                                        .setDeliveryTimeout(-1, TimeUnit.SECONDS)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
    }
}
