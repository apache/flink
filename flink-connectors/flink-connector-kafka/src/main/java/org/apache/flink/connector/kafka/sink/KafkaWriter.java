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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.StatefulSink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.MetricUtil;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaMetricMutableWrapper;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava30.com.google.common.io.Closer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.UnknownProducerIdException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class is responsible to write records in a Kafka topic and to handle the different delivery
 * {@link DeliveryGuarantee}s.
 *
 * @param <IN> The type of the input elements.
 */
class KafkaWriter<IN>
        implements StatefulSink.StatefulSinkWriter<IN, KafkaWriterState>,
                TwoPhaseCommittingSink.PrecommittingSinkWriter<IN, KafkaCommittable> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaWriter.class);
    private static final String KAFKA_PRODUCER_METRIC_NAME = "KafkaProducer";
    private static final long METRIC_UPDATE_INTERVAL_MILLIS = 500;

    private static final String KEY_DISABLE_METRICS = "flink.disable-metrics";
    private static final String KEY_REGISTER_METRICS = "register.producer.metrics";
    private static final String KAFKA_PRODUCER_METRICS = "producer-metrics";

    private final DeliveryGuarantee deliveryGuarantee;
    private final Properties kafkaProducerConfig;
    private final String transactionalIdPrefix;
    private final KafkaRecordSerializationSchema<IN> recordSerializer;
    private final Callback deliveryCallback;
    private final KafkaRecordSerializationSchema.KafkaSinkContext kafkaSinkContext;

    private final Map<String, KafkaMetricMutableWrapper> previouslyCreatedMetrics = new HashMap<>();
    private final SinkWriterMetricGroup metricGroup;
    private final boolean disabledMetrics;
    private final Counter numRecordsSendCounter;
    private final Counter numBytesSendCounter;
    // deprecated, use numRecordsSendErrorsCounter instead.
    @Deprecated private final Counter numRecordsOutErrorsCounter;
    private final Counter numRecordsSendErrorsCounter;
    private final ProcessingTimeService timeService;

    // Number of outgoing bytes at the latest metric sync
    private long latestOutgoingByteTotal;
    private Metric byteOutMetric;
    private FlinkKafkaInternalProducer<byte[], byte[]> currentProducer;
    private final KafkaWriterState kafkaWriterState;
    // producer pool only used for exactly once
    private final Deque<FlinkKafkaInternalProducer<byte[], byte[]>> producerPool =
            new ArrayDeque<>();
    private final Closer closer = Closer.create();
    private long lastCheckpointId;

    private boolean closed = false;
    private long lastSync = System.currentTimeMillis();

    /**
     * Constructor creating a kafka writer.
     *
     * <p>It will throw a {@link RuntimeException} if {@link
     * KafkaRecordSerializationSchema#open(SerializationSchema.InitializationContext,
     * KafkaRecordSerializationSchema.KafkaSinkContext)} fails.
     *
     * @param deliveryGuarantee the Sink's delivery guarantee
     * @param kafkaProducerConfig the properties to configure the {@link FlinkKafkaInternalProducer}
     * @param transactionalIdPrefix used to create the transactionalIds
     * @param sinkInitContext context to provide information about the runtime environment
     * @param recordSerializer serialize to transform the incoming records to {@link ProducerRecord}
     * @param schemaContext context used to initialize the {@link KafkaRecordSerializationSchema}
     * @param recoveredStates state from an previous execution which was covered
     */
    KafkaWriter(
            DeliveryGuarantee deliveryGuarantee,
            Properties kafkaProducerConfig,
            String transactionalIdPrefix,
            Sink.InitContext sinkInitContext,
            KafkaRecordSerializationSchema<IN> recordSerializer,
            SerializationSchema.InitializationContext schemaContext,
            Collection<KafkaWriterState> recoveredStates) {
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee, "deliveryGuarantee");
        this.kafkaProducerConfig = checkNotNull(kafkaProducerConfig, "kafkaProducerConfig");
        this.transactionalIdPrefix = checkNotNull(transactionalIdPrefix, "transactionalIdPrefix");
        this.recordSerializer = checkNotNull(recordSerializer, "recordSerializer");
        this.deliveryCallback =
                new WriterCallback(
                        sinkInitContext.getMailboxExecutor(),
                        sinkInitContext.<RecordMetadata>metadataConsumer().orElse(null));
        this.disabledMetrics =
                kafkaProducerConfig.containsKey(KEY_DISABLE_METRICS)
                                && Boolean.parseBoolean(
                                        kafkaProducerConfig.get(KEY_DISABLE_METRICS).toString())
                        || kafkaProducerConfig.containsKey(KEY_REGISTER_METRICS)
                                && !Boolean.parseBoolean(
                                        kafkaProducerConfig.get(KEY_REGISTER_METRICS).toString());
        checkNotNull(sinkInitContext, "sinkInitContext");
        this.timeService = sinkInitContext.getProcessingTimeService();
        this.metricGroup = sinkInitContext.metricGroup();
        this.numBytesSendCounter = metricGroup.getNumBytesSendCounter();
        this.numRecordsSendCounter = metricGroup.getNumRecordsSendCounter();
        this.numRecordsOutErrorsCounter = metricGroup.getNumRecordsOutErrorsCounter();
        this.numRecordsSendErrorsCounter = metricGroup.getNumRecordsSendErrorsCounter();
        this.kafkaSinkContext =
                new DefaultKafkaSinkContext(
                        sinkInitContext.getSubtaskId(),
                        sinkInitContext.getNumberOfParallelSubtasks(),
                        kafkaProducerConfig);
        try {
            recordSerializer.open(schemaContext, kafkaSinkContext);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Cannot initialize schema.", e);
        }

        this.kafkaWriterState = new KafkaWriterState(transactionalIdPrefix);
        this.lastCheckpointId =
                sinkInitContext
                        .getRestoredCheckpointId()
                        .orElse(CheckpointIDCounter.INITIAL_CHECKPOINT_ID - 1);
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            abortLingeringTransactions(
                    checkNotNull(recoveredStates, "recoveredStates"), lastCheckpointId + 1);
            this.currentProducer = getTransactionalProducer(lastCheckpointId + 1);
            this.currentProducer.beginTransaction();
        } else if (deliveryGuarantee == DeliveryGuarantee.AT_LEAST_ONCE
                || deliveryGuarantee == DeliveryGuarantee.NONE) {
            this.currentProducer = new FlinkKafkaInternalProducer<>(this.kafkaProducerConfig, null);
            closer.register(this.currentProducer);
            initKafkaMetrics(this.currentProducer);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported Kafka writer semantic " + this.deliveryGuarantee);
        }

        initFlinkMetrics();
    }

    @Override
    public void write(IN element, Context context) throws IOException {
        final ProducerRecord<byte[], byte[]> record =
                recordSerializer.serialize(element, kafkaSinkContext, context.timestamp());
        currentProducer.send(record, deliveryCallback);
        numRecordsSendCounter.inc();
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        if (deliveryGuarantee != DeliveryGuarantee.NONE || endOfInput) {
            LOG.debug("final flush={}", endOfInput);
            currentProducer.flush();
        }
    }

    @Override
    public Collection<KafkaCommittable> prepareCommit() {
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            final List<KafkaCommittable> committables =
                    Collections.singletonList(
                            KafkaCommittable.of(currentProducer, producerPool::add));
            LOG.debug("Committing {} committables.", committables);
            return committables;
        }
        return Collections.emptyList();
    }

    @Override
    public List<KafkaWriterState> snapshotState(long checkpointId) throws IOException {
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            currentProducer = getTransactionalProducer(checkpointId + 1);
            currentProducer.beginTransaction();
        }
        return ImmutableList.of(kafkaWriterState);
    }

    @Override
    public void close() throws Exception {
        closed = true;
        LOG.debug("Closing writer with {}", currentProducer);
        closeAll(
                this::abortCurrentProducer,
                closer,
                producerPool::clear,
                () -> {
                    checkState(currentProducer.isClosed());
                    currentProducer = null;
                });
    }

    private void abortCurrentProducer() {
        if (currentProducer.isInTransaction()) {
            try {
                currentProducer.abortTransaction();
            } catch (ProducerFencedException e) {
                LOG.debug(
                        "Producer {} fenced while aborting", currentProducer.getTransactionalId());
            }
        }
    }

    @VisibleForTesting
    Deque<FlinkKafkaInternalProducer<byte[], byte[]>> getProducerPool() {
        return producerPool;
    }

    @VisibleForTesting
    FlinkKafkaInternalProducer<byte[], byte[]> getCurrentProducer() {
        return currentProducer;
    }

    void abortLingeringTransactions(
            Collection<KafkaWriterState> recoveredStates, long startCheckpointId) {
        List<String> prefixesToAbort = Lists.newArrayList(transactionalIdPrefix);

        final Optional<KafkaWriterState> lastStateOpt = recoveredStates.stream().findFirst();
        if (lastStateOpt.isPresent()) {
            KafkaWriterState lastState = lastStateOpt.get();
            if (!lastState.getTransactionalIdPrefix().equals(transactionalIdPrefix)) {
                prefixesToAbort.add(lastState.getTransactionalIdPrefix());
                LOG.warn(
                        "Transactional id prefix from previous execution {} has changed to {}.",
                        lastState.getTransactionalIdPrefix(),
                        transactionalIdPrefix);
            }
        }

        try (TransactionAborter transactionAborter =
                new TransactionAborter(
                        kafkaSinkContext.getParallelInstanceId(),
                        kafkaSinkContext.getNumberOfParallelInstances(),
                        this::getOrCreateTransactionalProducer,
                        producerPool::add)) {
            transactionAborter.abortLingeringTransactions(prefixesToAbort, startCheckpointId);
        }
    }

    /**
     * For each checkpoint we create new {@link FlinkKafkaInternalProducer} so that new transactions
     * will not clash with transactions created during previous checkpoints ({@code
     * producer.initTransactions()} assures that we obtain new producerId and epoch counters).
     *
     * <p>Ensures that all transaction ids in between lastCheckpointId and checkpointId are
     * initialized.
     */
    private FlinkKafkaInternalProducer<byte[], byte[]> getTransactionalProducer(long checkpointId) {
        checkState(
                checkpointId > lastCheckpointId,
                "Expected %s > %s",
                checkpointId,
                lastCheckpointId);
        FlinkKafkaInternalProducer<byte[], byte[]> producer = null;
        // in case checkpoints have been aborted, Flink would create non-consecutive transaction ids
        // this loop ensures that all gaps are filled with initialized (empty) transactions
        for (long id = lastCheckpointId + 1; id <= checkpointId; id++) {
            String transactionalId =
                    TransactionalIdFactory.buildTransactionalId(
                            transactionalIdPrefix, kafkaSinkContext.getParallelInstanceId(), id);
            producer = getOrCreateTransactionalProducer(transactionalId);
        }
        this.lastCheckpointId = checkpointId;
        assert producer != null;
        LOG.info("Created new transactional producer {}", producer.getTransactionalId());
        return producer;
    }

    private FlinkKafkaInternalProducer<byte[], byte[]> getOrCreateTransactionalProducer(
            String transactionalId) {
        FlinkKafkaInternalProducer<byte[], byte[]> producer = producerPool.poll();
        if (producer == null) {
            producer = new FlinkKafkaInternalProducer<>(kafkaProducerConfig, transactionalId);
            closer.register(producer);
            producer.initTransactions();
            initKafkaMetrics(producer);
        } else {
            producer.initTransactionId(transactionalId);
        }
        return producer;
    }

    private void initFlinkMetrics() {
        metricGroup.setCurrentSendTimeGauge(this::computeSendTime);
        registerMetricSync();
    }

    private void initKafkaMetrics(FlinkKafkaInternalProducer<byte[], byte[]> producer) {
        byteOutMetric =
                MetricUtil.getKafkaMetric(
                        producer.metrics(), KAFKA_PRODUCER_METRICS, "outgoing-byte-total");
        if (disabledMetrics) {
            return;
        }
        final MetricGroup kafkaMetricGroup = metricGroup.addGroup(KAFKA_PRODUCER_METRIC_NAME);
        producer.metrics().entrySet().forEach(initMetric(kafkaMetricGroup));
    }

    private Consumer<Map.Entry<MetricName, ? extends Metric>> initMetric(
            MetricGroup kafkaMetricGroup) {
        return (entry) -> {
            final String name = entry.getKey().name();
            final Metric metric = entry.getValue();
            if (previouslyCreatedMetrics.containsKey(name)) {
                final KafkaMetricMutableWrapper wrapper = previouslyCreatedMetrics.get(name);
                wrapper.setKafkaMetric(metric);
            } else {
                final KafkaMetricMutableWrapper wrapper = new KafkaMetricMutableWrapper(metric);
                previouslyCreatedMetrics.put(name, wrapper);
                kafkaMetricGroup.gauge(name, wrapper);
            }
        };
    }

    private long computeSendTime() {
        FlinkKafkaInternalProducer<byte[], byte[]> producer = this.currentProducer;
        if (producer == null) {
            return -1L;
        }
        final Metric sendTime =
                MetricUtil.getKafkaMetric(
                        producer.metrics(), KAFKA_PRODUCER_METRICS, "request-latency-avg");
        final Metric queueTime =
                MetricUtil.getKafkaMetric(
                        producer.metrics(), KAFKA_PRODUCER_METRICS, "record-queue-time-avg");
        return ((Number) sendTime.metricValue()).longValue()
                + ((Number) queueTime.metricValue()).longValue();
    }

    private void registerMetricSync() {
        timeService.registerTimer(
                lastSync + METRIC_UPDATE_INTERVAL_MILLIS,
                (time) -> {
                    if (closed) {
                        return;
                    }
                    long outgoingBytesUntilNow = ((Number) byteOutMetric.metricValue()).longValue();
                    long outgoingBytesSinceLastUpdate =
                            outgoingBytesUntilNow - latestOutgoingByteTotal;
                    numBytesSendCounter.inc(outgoingBytesSinceLastUpdate);
                    latestOutgoingByteTotal = outgoingBytesUntilNow;
                    lastSync = time;
                    registerMetricSync();
                });
    }

    private class WriterCallback implements Callback {
        private final MailboxExecutor mailboxExecutor;
        @Nullable private final Consumer<RecordMetadata> metadataConsumer;

        public WriterCallback(
                MailboxExecutor mailboxExecutor,
                @Nullable Consumer<RecordMetadata> metadataConsumer) {
            this.mailboxExecutor = mailboxExecutor;
            this.metadataConsumer = metadataConsumer;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                FlinkKafkaInternalProducer<byte[], byte[]> producer =
                        KafkaWriter.this.currentProducer;
                mailboxExecutor.execute(
                        () -> {
                            numRecordsOutErrorsCounter.inc();
                            numRecordsSendErrorsCounter.inc();
                            throwException(metadata, exception, producer);
                        },
                        "Failed to send data to Kafka");
            }

            if (metadataConsumer != null) {
                metadataConsumer.accept(metadata);
            }
        }

        private void throwException(
                RecordMetadata metadata,
                Exception exception,
                FlinkKafkaInternalProducer<byte[], byte[]> producer) {
            String message =
                    String.format("Failed to send data to Kafka %s with %s ", metadata, producer);
            if (exception instanceof UnknownProducerIdException) {
                message += KafkaCommitter.UNKNOWN_PRODUCER_ID_ERROR_MESSAGE;
            }
            throw new FlinkRuntimeException(message, exception);
        }
    }
}
