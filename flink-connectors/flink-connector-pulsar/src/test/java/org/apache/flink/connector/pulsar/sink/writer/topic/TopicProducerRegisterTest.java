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

package org.apache.flink.connector.pulsar.sink.writer.topic;

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.committer.PulsarCommittable;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.List;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.base.DeliveryGuarantee.EXACTLY_ONCE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for {@link TopicProducerRegister}. */
class TopicProducerRegisterTest extends PulsarTestSuiteBase {

    @ParameterizedTest
    @EnumSource(DeliveryGuarantee.class)
    void createMessageBuilderForSendingMessage(DeliveryGuarantee deliveryGuarantee)
            throws IOException {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 8);

        SinkConfiguration configuration = sinkConfiguration(deliveryGuarantee);
        TopicProducerRegister register = new TopicProducerRegister(configuration);

        String message = randomAlphabetic(10);
        register.createMessageBuilder(topic, Schema.STRING).value(message).send();

        if (deliveryGuarantee == EXACTLY_ONCE) {
            List<PulsarCommittable> committables = register.prepareCommit();
            for (PulsarCommittable committable : committables) {
                TxnID txnID = committable.getTxnID();
                TransactionCoordinatorClient coordinatorClient = operator().coordinatorClient();
                coordinatorClient.commit(txnID);
            }
        }

        Message<String> receiveMessage = operator().receiveMessage(topic, Schema.STRING);
        assertEquals(receiveMessage.getValue(), message);
    }

    @ParameterizedTest
    @EnumSource(
            value = DeliveryGuarantee.class,
            names = {"AT_LEAST_ONCE", "NONE"})
    void noneAndAtLeastOnceWouldNotCreateTransaction(DeliveryGuarantee deliveryGuarantee) {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 8);

        SinkConfiguration configuration = sinkConfiguration(deliveryGuarantee);
        TopicProducerRegister register = new TopicProducerRegister(configuration);

        String message = randomAlphabetic(10);
        register.createMessageBuilder(topic, Schema.STRING).value(message).sendAsync();

        List<PulsarCommittable> committables = register.prepareCommit();
        assertThat(committables).isEmpty();
    }

    private SinkConfiguration sinkConfiguration(DeliveryGuarantee deliveryGuarantee) {
        return new SinkConfiguration(operator().sinkConfig(deliveryGuarantee));
    }
}
