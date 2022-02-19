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

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.connector.sink2.mocks.MockCommitRequest;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KafkaCommitter}. */
@ExtendWith({TestLoggerExtension.class})
public class KafkaCommitterTest {

    private static final int PRODUCER_ID = 0;
    private static final short EPOCH = 0;
    private static final String TRANSACTIONAL_ID = "transactionalId";

    /** Causes a network error by inactive broker and tests that a retry will happen. */
    @Test
    public void testRetryCommittableOnRetriableError() throws IOException, InterruptedException {
        Properties properties = getProperties();
        try (final KafkaCommitter committer = new KafkaCommitter(properties);
                FlinkKafkaInternalProducer<Object, Object> producer =
                        new FlinkKafkaInternalProducer<>(properties, TRANSACTIONAL_ID);
                Recyclable<FlinkKafkaInternalProducer<Object, Object>> recyclable =
                        new Recyclable<>(producer, p -> {})) {
            final MockCommitRequest<KafkaCommittable> request =
                    new MockCommitRequest<>(
                            new KafkaCommittable(PRODUCER_ID, EPOCH, TRANSACTIONAL_ID, recyclable));

            producer.resumeTransaction(PRODUCER_ID, EPOCH);
            committer.commit(Collections.singletonList(request));

            assertThat(request.getNumberOfRetries()).isEqualTo(1);
            assertThat(recyclable.isRecycled()).isFalse();
            // FLINK-25531: force the producer to close immediately, else it would take 1 hour
            producer.close(Duration.ZERO);
        }
    }

    @Test
    public void testFailJobOnUnknownFatalError() throws IOException, InterruptedException {
        Properties properties = getProperties();
        try (final KafkaCommitter committer = new KafkaCommitter(properties);
                FlinkKafkaInternalProducer<Object, Object> producer =
                        new FlinkKafkaInternalProducer<>(properties, TRANSACTIONAL_ID);
                Recyclable<FlinkKafkaInternalProducer<Object, Object>> recyclable =
                        new Recyclable<>(producer, p -> {})) {
            // will fail because transaction not started
            final MockCommitRequest<KafkaCommittable> request =
                    new MockCommitRequest<>(
                            new KafkaCommittable(PRODUCER_ID, EPOCH, TRANSACTIONAL_ID, recyclable));
            committer.commit(Collections.singletonList(request));
            assertThat(request.getFailedWithUnknownReason())
                    .isInstanceOf(IllegalStateException.class);
            assertThat(request.getFailedWithUnknownReason().getMessage())
                    .contains("Transaction was not started");
            assertThat(recyclable.isRecycled()).isTrue();
        }
    }

    Properties getProperties() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:1");
        // Low timeout will fail commitTransaction quicker
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "100");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return properties;
    }
}
