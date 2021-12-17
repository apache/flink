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
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.InitContextInitializationContextAdapter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Supplier;

/**
 * Flink Sink to produce data into a Kafka topic. The sink supports all delivery guarantees
 * described by {@link DeliveryGuarantee}.
 * <li>{@link DeliveryGuarantee#NONE} does not provide any guarantees: messages may be lost in case
 *     of issues on the Kafka broker and messages may be duplicated in case of a Flink failure.
 * <li>{@link DeliveryGuarantee#AT_LEAST_ONCE} the sink will wait for all outstanding records in the
 *     Kafka buffers to be acknowledged by the Kafka producer on a checkpoint. No messages will be
 *     lost in case of any issue with the Kafka brokers but messages may be duplicated when Flink
 *     restarts.
 * <li>{@link DeliveryGuarantee#EXACTLY_ONCE}: In this mode the KafkaSink will write all messages in
 *     a Kafka transaction that will be committed to Kafka on a checkpoint. Thus, if the consumer
 *     reads only committed data (see Kafka consumer config isolation.level), no duplicates will be
 *     seen in case of a Flink restart. However, this delays record writing effectively until a
 *     checkpoint is written, so adjust the checkpoint duration accordingly. Please ensure that you
 *     use unique {@link #transactionalIdPrefix}s across your applications running on the same Kafka
 *     cluster such that multiple running jobs do not interfere in their transactions! Additionally,
 *     it is highly recommended to tweak Kafka transaction timeout (link) >> maximum checkpoint
 *     duration + maximum restart duration or data loss may happen when Kafka expires an uncommitted
 *     transaction.
 *
 * @param <IN> type of the records written to Kafka
 * @see KafkaSinkBuilder on how to construct a KafkaSink
 */
@PublicEvolving
public class KafkaSink<IN> implements Sink<IN, KafkaCommittable, KafkaWriterState, Void> {

    private final DeliveryGuarantee deliveryGuarantee;

    private final KafkaRecordSerializationSchema<IN> recordSerializer;
    private final Properties kafkaProducerConfig;
    private final String transactionalIdPrefix;

    KafkaSink(
            DeliveryGuarantee deliveryGuarantee,
            Properties kafkaProducerConfig,
            String transactionalIdPrefix,
            KafkaRecordSerializationSchema<IN> recordSerializer) {
        this.deliveryGuarantee = deliveryGuarantee;
        this.kafkaProducerConfig = kafkaProducerConfig;
        this.transactionalIdPrefix = transactionalIdPrefix;
        this.recordSerializer = recordSerializer;
    }

    /**
     * Create a {@link KafkaSinkBuilder} to construct a new {@link KafkaSink}.
     *
     * @param <IN> type of incoming records
     * @return {@link KafkaSinkBuilder}
     */
    public static <IN> KafkaSinkBuilder<IN> builder() {
        return new KafkaSinkBuilder<>();
    }

    @Override
    public SinkWriter<IN, KafkaCommittable, KafkaWriterState> createWriter(
            InitContext context, List<KafkaWriterState> states) throws IOException {
        final Supplier<MetricGroup> metricGroupSupplier =
                () -> context.metricGroup().addGroup("user");
        return new KafkaWriter<>(
                deliveryGuarantee,
                kafkaProducerConfig,
                transactionalIdPrefix,
                context,
                recordSerializer,
                new InitContextInitializationContextAdapter(
                        context.getUserCodeClassLoader(), metricGroupSupplier),
                states);
    }

    @Override
    public Optional<Committer<KafkaCommittable>> createCommitter() throws IOException {
        return Optional.of(new KafkaCommitter(kafkaProducerConfig));
    }

    @Override
    public Optional<GlobalCommitter<KafkaCommittable, Void>> createGlobalCommitter()
            throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<KafkaCommittable>> getCommittableSerializer() {
        return Optional.of(new KafkaCommittableSerializer());
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<KafkaWriterState>> getWriterStateSerializer() {
        return Optional.of(new KafkaWriterStateSerializer());
    }
}
