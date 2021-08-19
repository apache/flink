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
import org.apache.flink.shaded.guava30.com.google.common.io.Closer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

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
        this.kafkaWriterState =
                recoverAndInitializeState(checkNotNull(recoveredStates, "recoveredStates"));
        this.currentProducer = createProducer();
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
        List<KafkaCommittable> committables = precommit();
        currentProducer = createProducer();
        return committables;
    }

    @Override
    public List<KafkaWriterState> snapshotState(long checkpointId) throws IOException {
        return ImmutableList.of(kafkaWriterState);
    }

    @Override
    public void close() throws Exception {
        if (currentProducer.isInTransaction()) {
            currentProducer.abortTransaction();
        }
        currentProducer.flush();
        closed = true;
        closer.close();
    }

    private KafkaWriterState recoverAndInitializeState(List<KafkaWriterState> recoveredStates) {
        final int subtaskId = kafkaSinkContext.getParallelInstanceId();
        if (recoveredStates.isEmpty()) {
            final KafkaWriterState state =
                    new KafkaWriterState(transactionalIdPrefix, subtaskId, 0);
            abortTransactions(getTransactionsToAbort(state, new ArrayList<>()));
            return state;
        }
        final Map<Integer, KafkaWriterState> taskOffsetMapping =
                recoveredStates.stream()
                        .collect(
                                Collectors.toMap(
                                        KafkaWriterState::getSubtaskId, Function.identity()));
        checkState(
                taskOffsetMapping.containsKey(subtaskId),
                "Internal error: It is expected that state from previous executions is distributed to the same subtask id.");
        final KafkaWriterState lastState = taskOffsetMapping.get(subtaskId);
        taskOffsetMapping.remove(subtaskId);
        abortTransactions(
                getTransactionsToAbort(lastState, new ArrayList<>(taskOffsetMapping.values())));
        if (!lastState.getTransactionalIdPrefix().equals(transactionalIdPrefix)) {
            LOG.warn(
                    "Transactional id prefix from previous execution {} has changed to {}.",
                    lastState.getTransactionalIdPrefix(),
                    transactionalIdPrefix);
            return new KafkaWriterState(transactionalIdPrefix, subtaskId, 0);
        }
        return new KafkaWriterState(
                transactionalIdPrefix, subtaskId, lastState.getTransactionalIdOffset());
    }

    private void abortTransactions(List<String> transactionsToAbort) {
        transactionsToAbort.forEach(
                transaction -> {
                    // don't mess with the original configuration or any other
                    // properties of the
                    // original object
                    // -> create an internal kafka producer on our own and do not rely
                    // on
                    //    initTransactionalProducer().
                    final Properties myConfig = new Properties();
                    myConfig.putAll(kafkaProducerConfig);
                    myConfig.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transaction);
                    LOG.info("Aborting Kafka transaction {}.", transaction);
                    FlinkKafkaInternalProducer<byte[], byte[]> kafkaProducer = null;
                    try {
                        kafkaProducer = new FlinkKafkaInternalProducer<>(myConfig);
                        // it suffices to call initTransactions - this will abort any
                        // lingering transactions
                        kafkaProducer.initTransactions();
                    } finally {
                        if (kafkaProducer != null) {
                            kafkaProducer.close(Duration.ofSeconds(0));
                        }
                    }
                });
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

    private FlinkKafkaInternalProducer<byte[], byte[]> createProducer() {
        switch (deliveryGuarantee) {
            case EXACTLY_ONCE:
                final FlinkKafkaInternalProducer<byte[], byte[]> transactionalProducer =
                        createTransactionalProducer();
                initMetrics(transactionalProducer);
                transactionalProducer.beginTransaction();
                closer.register(transactionalProducer);
                return transactionalProducer;
            case AT_LEAST_ONCE:
            case NONE:
                if (currentProducer != null) {
                    LOG.debug("Reusing existing KafkaProducer");
                    return currentProducer;
                }
                final FlinkKafkaInternalProducer<byte[], byte[]> producer =
                        new FlinkKafkaInternalProducer<>(kafkaProducerConfig);
                initMetrics(producer);
                closer.register(producer);
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
     */
    private FlinkKafkaInternalProducer<byte[], byte[]> createTransactionalProducer() {
        final long transactionalIdOffset = kafkaWriterState.getTransactionalIdOffset() + 1;
        final Properties copiedProducerConfig = new Properties();
        copiedProducerConfig.putAll(kafkaProducerConfig);
        initTransactionalProducerConfig(
                copiedProducerConfig,
                transactionalIdOffset,
                transactionalIdPrefix,
                kafkaSinkContext.getParallelInstanceId());
        final FlinkKafkaInternalProducer<byte[], byte[]> producer =
                new FlinkKafkaInternalProducer<>(copiedProducerConfig);
        producer.initTransactions();
        kafkaWriterState =
                new KafkaWriterState(
                        transactionalIdPrefix,
                        kafkaSinkContext.getParallelInstanceId(),
                        transactionalIdOffset);
        LOG.info(
                "Created new transactional producer {}",
                copiedProducerConfig.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG));
        return producer;
    }

    private static void initTransactionalProducerConfig(
            Properties producerConfig,
            long transactionalIdOffset,
            String transactionalIdPrefix,
            int subtaskId) {
        producerConfig.put(
                ProducerConfig.TRANSACTIONAL_ID_CONFIG,
                TransactionalIdFactory.buildTransactionalId(
                        transactionalIdPrefix, subtaskId, transactionalIdOffset));
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

    private List<String> getTransactionsToAbort(
            KafkaWriterState main, List<KafkaWriterState> others) {
        try (final KafkaTransactionLog log =
                new KafkaTransactionLog(
                        kafkaProducerConfig,
                        main,
                        others,
                        kafkaSinkContext.getNumberOfParallelInstances())) {
            return log.getTransactionsToAbort();
        } catch (KafkaException e) {
            LOG.warn(
                    "Cannot abort transactions before startup e.g. the job has no access to the "
                            + "__transaction_state topic. Lingering transactions may hold new "
                            + "data back from downstream consumers. Please abort these "
                            + "transactions manually.",
                    e);
            return Collections.emptyList();
        }
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
