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

import org.apache.flink.api.connector.sink.Committer;

import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Committer implementation for {@link KafkaSink}
 *
 * <p>The committer is responsible to finalize the Kafka transactions by committing them.
 */
class KafkaCommitter implements Committer<KafkaCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCommitter.class);

    private final Properties kafkaProducerConfig;

    KafkaCommitter(Properties kafkaProducerConfig) {
        this.kafkaProducerConfig = kafkaProducerConfig;
    }

    @Override
    public List<KafkaCommittable> commit(List<KafkaCommittable> committables) throws IOException {
        List<KafkaCommittable> retryableCommittables = new ArrayList<>();
        for (KafkaCommittable committable : committables) {
            final String transactionalId = committable.getTransactionalId();
            LOG.debug("Committing Kafka transaction {}", transactionalId);
            try (FlinkKafkaInternalProducer<?, ?> producer =
                    committable.getProducer().orElseGet(() -> createProducer(committable))) {
                producer.commitTransaction();
            } catch (ProducerFencedException | InvalidTxnStateException e) {
                // That means we have committed this transaction before.
                LOG.warn(
                        "Encountered error {} while recovering transaction {}. "
                                + "Presumably this transaction has been already committed before",
                        e,
                        committable);
            } catch (Throwable e) {
                LOG.warn("Cannot commit Kafka transaction, retrying.", e);
                retryableCommittables.add(committable);
            }
        }
        return retryableCommittables;
    }

    @Override
    public void close() throws Exception {}

    /**
     * Creates a producer that can commit into the same transaction as the upstream producer that
     * was serialized into {@link KafkaCommittable}.
     */
    private FlinkKafkaInternalProducer<?, ?> createProducer(KafkaCommittable committable) {
        FlinkKafkaInternalProducer<?, ?> producer =
                new FlinkKafkaInternalProducer<>(
                        kafkaProducerConfig, committable.getTransactionalId());
        producer.resumeTransaction(committable.getProducerId(), committable.getEpoch());
        return producer;
    }
}
