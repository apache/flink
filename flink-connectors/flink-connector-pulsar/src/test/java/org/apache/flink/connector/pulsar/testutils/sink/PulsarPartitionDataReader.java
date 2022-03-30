/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.testutils.sink;

import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntimeOperator;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

/** The data reader for a specified topic partition from Pulsar. */
public class PulsarPartitionDataReader<T> implements ExternalSystemDataReader<T>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(PulsarPartitionDataReader.class);

    private final Consumer<T> consumer;

    public PulsarPartitionDataReader(
            PulsarRuntimeOperator operator, String fullTopicName, Schema<T> schema)
            throws PulsarClientException {
        // Create client for supporting the use in E2E test.
        String subscriptionName = randomAlphanumeric(12);
        this.consumer =
                operator.client()
                        .newConsumer(schema)
                        .topic(fullTopicName)
                        .subscriptionName(subscriptionName)
                        .subscriptionType(SubscriptionType.Exclusive)
                        .subscriptionMode(SubscriptionMode.Durable)
                        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                        .subscribe();
    }

    @Override
    public List<T> poll(Duration timeout) {
        List<T> results = new ArrayList<>();

        while (true) {
            try {
                int millis = Math.toIntExact(timeout.toMillis());
                Message<T> message = consumer.receive(millis, MILLISECONDS);

                if (message != null) {
                    consumer.acknowledgeCumulative(message);
                    results.add(message.getValue());
                } else {
                    break;
                }
            } catch (Exception e) {
                LOG.error("", e);
                break;
            }
        }

        return results;
    }

    @Override
    public void close() throws PulsarClientException {
        consumer.close();
    }
}
