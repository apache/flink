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
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.InvalidTxnStateException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;

/**
 * Committer implementation for {@link KafkaSink}
 *
 * <p>The committer is responsible to finalize the Kafka transactions by committing them.
 */
class KafkaCommitter implements Committer<KafkaCommittable>, Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaCommitter.class);
    public static final String UNKNOWN_PRODUCER_ID_ERROR_MESSAGE =
            "because of a bug in the Kafka broker (KAFKA-9310). Please upgrade to Kafka 2.5+. If you are running with concurrent checkpoints, you also may want to try without them.\n"
                    + "To avoid data loss, the application will restart.";

    private final Properties kafkaProducerConfig;

    @Nullable private FlinkKafkaInternalProducer<?, ?> recoveryProducer;

    KafkaCommitter(Properties kafkaProducerConfig) {
        this.kafkaProducerConfig = kafkaProducerConfig;
    }

    @Override
    public List<KafkaCommittable> commit(List<KafkaCommittable> committables) throws IOException {
        List<KafkaCommittable> retryableCommittables = new ArrayList<>();
        Exception collected = null;
        for (KafkaCommittable committable : committables) {
            final String transactionalId = committable.getTransactionalId();
            LOG.debug("Committing Kafka transaction {}", transactionalId);
            Optional<Recyclable<? extends FlinkKafkaInternalProducer<?, ?>>> recyclable =
                    committable.getProducer();
            FlinkKafkaInternalProducer<?, ?> producer;
            try {
                producer =
                        recyclable
                                .<FlinkKafkaInternalProducer<?, ?>>map(Recyclable::getObject)
                                .orElseGet(() -> getRecoveryProducer(committable));
                producer.commitTransaction();
                producer.flush();
            } catch (RetriableException e) {
                LOG.warn(
                        "Encountered retriable exception while committing {}.", transactionalId, e);
                retryableCommittables.add(committable);
                continue;
            } catch (ProducerFencedException e) {
                // initTransaction has been called on this transaction before
                LOG.error(
                        "Unable to commit transaction ({}) because its producer is already fenced."
                                + " This means that you either have a different producer with the same '{}' (this is"
                                + " unlikely with the '{}' as all generated ids are unique and shouldn't be reused)"
                                + " or recovery took longer than '{}' ({}ms). In both cases this most likely signals data loss,"
                                + " please consult the Flink documentation for more details.",
                        committable,
                        ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                        KafkaSink.class.getSimpleName(),
                        ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                        kafkaProducerConfig.getProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG),
                        e);
            } catch (InvalidTxnStateException e) {
                // This exception only occurs when aborting after a commit or vice versa.
                // It does not appear on double commits or double aborts.
                LOG.error(
                        "Unable to commit transaction ({}) because it's in an invalid state. "
                                + "Most likely the transaction has been aborted for some reason. Please check the Kafka logs for more details.",
                        committable,
                        e);
            } catch (UnknownProducerIdException e) {
                LOG.error(
                        "Unable to commit transaction ({}) " + UNKNOWN_PRODUCER_ID_ERROR_MESSAGE,
                        committable,
                        e);
            } catch (Exception e) {
                LOG.error(
                        "Transaction ({}) encountered error and data has been potentially lost.",
                        committable,
                        e);
                collected = firstOrSuppressed(e, collected);
            }
            recyclable.ifPresent(Recyclable::close);
            if (collected != null) {
                throw new FlinkRuntimeException(
                        "Some committables were not committed and committing failed with:",
                        collected);
            }
        }
        return retryableCommittables;
    }

    @Override
    public void close() {
        if (recoveryProducer != null) {
            recoveryProducer.close();
        }
    }

    /**
     * Creates a producer that can commit into the same transaction as the upstream producer that
     * was serialized into {@link KafkaCommittable}.
     */
    private FlinkKafkaInternalProducer<?, ?> getRecoveryProducer(KafkaCommittable committable) {
        if (recoveryProducer == null) {
            recoveryProducer =
                    new FlinkKafkaInternalProducer<>(
                            kafkaProducerConfig, committable.getTransactionalId());
        } else {
            recoveryProducer.setTransactionId(committable.getTransactionalId());
        }
        recoveryProducer.resumeTransaction(committable.getProducerId(), committable.getEpoch());
        return recoveryProducer;
    }
}
