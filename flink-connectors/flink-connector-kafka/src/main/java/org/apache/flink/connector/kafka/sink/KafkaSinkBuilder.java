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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.connector.base.DeliveryGuarantee;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Builder to construct {@link KafkaSink}.
 *
 * <p>The following example shows the minimum setup to create a KafkaSink that writes String values
 * to a Kafka topic.
 *
 * <pre>{@code
 * KafkaSink<String> sink = KafkaSink
 *     .<String>builder
 *     .setBootstrapServers(MY_BOOTSTRAP_SERVERS)
 *     .setRecordSerializer(MY_RECORD_SERIALIZER)
 *     .build();
 * }</pre>
 *
 * <p>One can also configure different {@link DeliveryGuarantee} by using {@link
 * #setDeliverGuarantee(DeliveryGuarantee)} but keep in mind when using {@link
 * DeliveryGuarantee#EXACTLY_ONCE} one must set the transactionalIdPrefix {@link
 * #setTransactionalIdPrefix(String)}.
 *
 * @see KafkaSink for a more detailed explanation of the different guarantees.
 * @param <IN> type of the records written to Kafka
 */
@PublicEvolving
public class KafkaSinkBuilder<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSinkBuilder.class);
    private static final Duration DEFAULT_KAFKA_TRANSACTION_TIMEOUT = Duration.ofHours(1);
    private static final int MAXIMUM_PREFIX_BYTES = 64000;

    private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.NONE;
    private String transactionalIdPrefix = "kafka-sink";

    private Properties kafkaProducerConfig;
    private KafkaRecordSerializationSchema<IN> recordSerializer;
    private String bootstrapServers;

    KafkaSinkBuilder() {}

    /**
     * Sets the wanted the {@link DeliveryGuarantee}. The default delivery guarantee is {@link
     * #deliveryGuarantee}.
     *
     * @param deliveryGuarantee
     * @return {@link KafkaSinkBuilder}
     */
    public KafkaSinkBuilder<IN> setDeliverGuarantee(DeliveryGuarantee deliveryGuarantee) {
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee, "deliveryGuarantee");
        return this;
    }

    /**
     * Sets the configuration which used to instantiate all used {@link
     * org.apache.kafka.clients.producer.KafkaProducer}.
     *
     * @param kafkaProducerConfig
     * @return {@link KafkaSinkBuilder}
     */
    public KafkaSinkBuilder<IN> setKafkaProducerConfig(Properties kafkaProducerConfig) {
        this.kafkaProducerConfig = checkNotNull(kafkaProducerConfig, "kafkaProducerConfig");
        // set the producer configuration properties for kafka record key value serializers.
        if (!kafkaProducerConfig.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
            kafkaProducerConfig.put(
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                    ByteArraySerializer.class.getName());
        } else {
            LOG.warn(
                    "Overwriting the '{}' is not recommended",
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        }

        if (!kafkaProducerConfig.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
            kafkaProducerConfig.put(
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                    ByteArraySerializer.class.getName());
        } else {
            LOG.warn(
                    "Overwriting the '{}' is not recommended",
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        }

        if (!kafkaProducerConfig.containsKey(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG)) {
            final long timeout = DEFAULT_KAFKA_TRANSACTION_TIMEOUT.toMillis();
            checkState(
                    timeout < Integer.MAX_VALUE && timeout > 0,
                    "timeout does not fit into 32 bit integer");
            kafkaProducerConfig.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, (int) timeout);
            LOG.warn(
                    "Property [{}] not specified. Setting it to {}",
                    ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                    DEFAULT_KAFKA_TRANSACTION_TIMEOUT);
        }
        return this;
    }

    /**
     * Sets the {@link KafkaRecordSerializationSchema} that transforms incoming records to {@link
     * org.apache.kafka.clients.producer.ProducerRecord}s.
     *
     * @param recordSerializer
     * @return {@link KafkaSinkBuilder}
     */
    public KafkaSinkBuilder<IN> setRecordSerializer(
            KafkaRecordSerializationSchema<IN> recordSerializer) {
        this.recordSerializer = checkNotNull(recordSerializer, "recordSerializer");
        ClosureCleaner.clean(
                this.recordSerializer, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        return this;
    }

    /**
     * Sets the prefix for all created transactionalIds if {@link DeliveryGuarantee#EXACTLY_ONCE} is
     * configured.
     *
     * <p>It is mandatory to always set this value with {@link DeliveryGuarantee#EXACTLY_ONCE} to
     * prevent corrupted transactions if multiple jobs using the KafkaSink run against the same
     * Kafka Cluster. The default prefix is {@link #transactionalIdPrefix}.
     *
     * <p>The size of the prefix is capped by {@link #MAXIMUM_PREFIX_BYTES} formatted with UTF-8.
     *
     * <p>It is important to keep the prefix stable across application restarts. If the prefix
     * changes it might happen that lingering transactions are not correctly aborted and newly
     * written messages are not immediately consumable until the transactions timeout.
     *
     * @param transactionalIdPrefix
     * @return {@link KafkaSinkBuilder}
     */
    public KafkaSinkBuilder<IN> setTransactionalIdPrefix(String transactionalIdPrefix) {
        this.transactionalIdPrefix = checkNotNull(transactionalIdPrefix, "transactionalIdPrefix");
        checkState(
                transactionalIdPrefix.getBytes(StandardCharsets.UTF_8).length
                        <= MAXIMUM_PREFIX_BYTES,
                "The configured prefix is too long and the resulting transactionalId might exceed Kafka's transactionalIds size.");
        return this;
    }

    /**
     * Sets the Kafka bootstrap servers.
     *
     * @param bootstrapServers a comma separated list of valid URIs to reach the Kafka broker
     * @return {@link KafkaSinkBuilder}
     */
    public KafkaSinkBuilder<IN> setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = checkNotNull(bootstrapServers);
        return this;
    }

    private void sanityCheck() {
        if (kafkaProducerConfig == null) {
            setKafkaProducerConfig(new Properties());
        }
        if (bootstrapServers != null) {
            kafkaProducerConfig.setProperty(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        }
        checkNotNull(
                kafkaProducerConfig.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
                "bootstrapServers");
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            checkState(
                    transactionalIdPrefix != null,
                    "EXACTLY_ONCE delivery guarantee requires a transactionIdPrefix to be set to provide unique transaction names across multiple KafkaSinks writing to the same Kafka cluster.");
        }
        checkNotNull(recordSerializer, "recordSerializer");
    }

    /**
     * Constructs the {@link KafkaSink} with the configured properties.
     *
     * @return {@link KafkaSink}
     */
    public KafkaSink<IN> build() {
        sanityCheck();
        return new KafkaSink<>(
                deliveryGuarantee, kafkaProducerConfig, transactionalIdPrefix, recordSerializer);
    }
}
