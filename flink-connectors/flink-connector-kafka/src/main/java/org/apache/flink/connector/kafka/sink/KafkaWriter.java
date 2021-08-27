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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.MetricUtil;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaMetricMutableWrapper;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava30.com.google.common.io.Closer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class is responsible to write records in a Kafka topic and to handle the different delivery
 * {@link DeliveryGuarantee}s.
 *
 * @param <IN> The type of the input elements.
 */
class KafkaWriter<IN> implements SinkWriter<IN, KafkaCommittable, KafkaWriterState> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaWriter.class);
    private static final String KAFKA_PRODUCER_METRIC_NAME = "KafkaProducer";
    private static final long METRIC_UPDATE_INTERVAL_MILLIS = 500;

    private static final String KEY_DISABLE_METRICS = "flink.disable-metrics";
    private static final String KEY_REGISTER_METRICS = "register.producer.metrics";

    private final DeliveryGuarantee deliveryGuarantee;
    private final Properties kafkaProducerConfig;
    private final String transactionalIdPrefix;
    private final KafkaRecordSerializationSchema<IN> recordSerializer;
    private final Callback deliveryCallback;
    private final AtomicLong pendingRecords = new AtomicLong();
    private final KafkaRecordSerializationSchema.KafkaSinkContext kafkaSinkContext;
    private final Map<String, KafkaMetricMutableWrapper> previouslyCreatedMetrics = new HashMap<>();
    private final SinkWriterMetricGroup metricGroup;
    private final Counter numBytesOutCounter;
    private final Sink.ProcessingTimeService timeService;
    private final boolean disabledMetrics;

    private Metric byteOutMetric;
    private FlinkKafkaInternalProducer<byte[], byte[]> currentProducer;
    private KafkaWriterState kafkaWriterState;
    private final Closer closer = Closer.create();
    @Nullable private volatile Exception producerAsyncException;
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
            List<KafkaWriterState> recoveredStates) {
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee, "deliveryGuarantee");
        this.kafkaProducerConfig = checkNotNull(kafkaProducerConfig, "kafkaProducerConfig");
        this.transactionalIdPrefix = checkNotNull(transactionalIdPrefix, "transactionalIdPrefix");
        this.recordSerializer = checkNotNull(recordSerializer, "recordSerializer");
        this.deliveryCallback =
                (metadata, exception) -> {
                    if (exception != null && producerAsyncException == null) {
                        producerAsyncException = exception;
                    }
                    acknowledgeMessage();
                };
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
        this.numBytesOutCounter = metricGroup.getIOMetricGroup().getNumBytesOutCounter();
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
        lastCheckpointId = sinkInitContext.getRestoredCheckpointId().orElse(0);
        abortLingeringTransactions(
                checkNotNull(recoveredStates, "recoveredStates"), lastCheckpointId + 1);
        this.kafkaWriterState = new KafkaWriterState(transactionalIdPrefix);
        this.currentProducer = createProducer(lastCheckpointId + 1);
        registerMetricSync();
    }

    @Override
    public void write(IN element, Context context) throws IOException {
        checkErroneous();
        final ProducerRecord<byte[], byte[]> record =
                recordSerializer.serialize(element, kafkaSinkContext, context.timestamp());
        pendingRecords.incrementAndGet();
        currentProducer.send(record, deliveryCallback);
    }

    @Override
    public List<KafkaCommittable> prepareCommit(boolean flush) {
        flushRecords(flush);
        return precommit();
    }

    @Override
    public List<KafkaWriterState> snapshotState(long checkpointId) throws IOException {
        currentProducer = createProducer(checkpointId + 1);
        return ImmutableList.of(kafkaWriterState);
    }

    @Override
    public void close() throws Exception {

        if (currentProducer.isInTransaction()) {
            currentProducer.abortTransaction();
        }
        closed = true;
        closer.close();
        checkState(currentProducer.isClosed());
    }

    private void abortLingeringTransactions(
            List<KafkaWriterState> recoveredStates, long startCheckpointId) {
        List<String> prefixesToAbort = Lists.newArrayList(transactionalIdPrefix);

        if (!recoveredStates.isEmpty()) {
            KafkaWriterState lastState = recoveredStates.get(0);
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
                        FlinkKafkaInternalProducer::close)) {
            transactionAborter.abortLingeringTransactions(prefixesToAbort, startCheckpointId);
        }
    }

    private void acknowledgeMessage() {
        pendingRecords.decrementAndGet();
    }

    private void checkErroneous() {
        Exception e = producerAsyncException;
        if (e != null) {
            // prevent double throwing
            producerAsyncException = null;
            throw new RuntimeException("Failed to send data to Kafka: " + e.getMessage(), e);
        }
    }

    private FlinkKafkaInternalProducer<byte[], byte[]> createProducer(long checkpointId) {
        switch (deliveryGuarantee) {
            case EXACTLY_ONCE:
                final FlinkKafkaInternalProducer<byte[], byte[]> transactionalProducer =
                        getTransactionalProducer(checkpointId);
                initMetrics(transactionalProducer);
                transactionalProducer.beginTransaction();
                return transactionalProducer;
            case AT_LEAST_ONCE:
            case NONE:
                if (currentProducer != null) {
                    LOG.debug("Reusing existing KafkaProducer");
                    return currentProducer;
                }
                final FlinkKafkaInternalProducer<byte[], byte[]> producer =
                        new FlinkKafkaInternalProducer<>(kafkaProducerConfig, null);
                closer.register(producer);
                initMetrics(producer);
                return producer;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Kafka writer semantic " + deliveryGuarantee);
        }
    }

    private void flushRecords(boolean finalFlush) {
        switch (deliveryGuarantee) {
            case EXACTLY_ONCE:
            case AT_LEAST_ONCE:
                currentProducer.flush();
                final long pendingRecordsCount = pendingRecords.get();
                if (pendingRecordsCount != 0) {
                    throw new IllegalStateException(
                            "Pending record count must be zero at this point: "
                                    + pendingRecordsCount);
                }
                break;
            case NONE:
                if (finalFlush) {
                    currentProducer.flush();
                }
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Kafka writer semantic " + deliveryGuarantee);
        }
        // if the flushed requests has errors, we should propagate it also and fail the checkpoint
        checkErroneous();
    }

    private List<KafkaCommittable> precommit() {
        final List<KafkaCommittable> committables;
        switch (deliveryGuarantee) {
            case EXACTLY_ONCE:
                committables = Collections.singletonList(KafkaCommittable.of(currentProducer));
                break;
            case AT_LEAST_ONCE:
            case NONE:
                committables = Collections.emptyList();
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Kafka writer semantic " + deliveryGuarantee);
        }
        LOG.info("Committing {} committables.", committables);
        return committables;
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
        FlinkKafkaInternalProducer<byte[], byte[]> producer =
                new FlinkKafkaInternalProducer<>(kafkaProducerConfig, transactionalId);
        closer.register(producer);
        producer.initTransactions();
        initMetrics(producer);
        return producer;
    }

    private void initMetrics(FlinkKafkaInternalProducer<byte[], byte[]> producer) {
        byteOutMetric =
                MetricUtil.getKafkaMetric(
                        producer.metrics(), "producer-metrics", "outgoing-byte-total");
        metricGroup.setCurrentSendTimeGauge(() -> computeSendTime(producer));
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

    private static long computeSendTime(Producer<?, ?> producer) {
        final Metric sendTime =
                MetricUtil.getKafkaMetric(
                        producer.metrics(), "producer-metrics", "request-latency-avg");
        final Metric queueTime =
                MetricUtil.getKafkaMetric(
                        producer.metrics(), "producer-metrics", "record-queue-time-avg");
        return ((Number) sendTime.metricValue()).longValue()
                + ((Number) queueTime.metricValue()).longValue();
    }

    private void registerMetricSync() {
        timeService.registerProcessingTimer(
                lastSync + METRIC_UPDATE_INTERVAL_MILLIS,
                (time) -> {
                    if (closed) {
                        return;
                    }
                    MetricUtil.sync(byteOutMetric, numBytesOutCounter);
                    lastSync = time;
                    registerMetricSync();
                });
    }
}
