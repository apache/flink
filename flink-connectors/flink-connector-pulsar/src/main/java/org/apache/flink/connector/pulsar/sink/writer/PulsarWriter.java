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

package org.apache.flink.connector.pulsar.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink.PrecommittingSinkWriter;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.committer.PulsarCommittable;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContext;
import org.apache.flink.connector.pulsar.sink.writer.context.PulsarSinkContextAdapter;
import org.apache.flink.connector.pulsar.sink.writer.metrics.PulsarSinkWriterMetrics;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRouter;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.connector.pulsar.sink.writer.topic.TopicMetadataListener;
import org.apache.flink.connector.pulsar.sink.writer.topic.TopicProducerRegister;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

import static java.util.Collections.emptyList;
import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is responsible to write records in a Pulsar topic and to handle the different delivery
 * {@link DeliveryGuarantee}s.
 *
 * @param <IN> The type of the input elements.
 */
@Internal
public class PulsarWriter<IN> implements PrecommittingSinkWriter<IN, PulsarCommittable> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarWriter.class);

    private final SinkConfiguration sinkConfiguration;
    private final PulsarSerializationSchema<IN> serializationSchema;
    private final TopicMetadataListener metadataListener;
    private final DeliveryGuarantee deliveryGuarantee;
    private final TopicRouter<IN> topicRouter;
    private final PulsarSinkContextAdapter sinkContextAdapter;
    private final MailboxExecutor mailboxExecutor;
    private final TopicProducerRegister producerRegister;
    private final Semaphore pendingMessages;
    private final PulsarSinkWriterMetrics pulsarSinkWriterMetrics;
    private final ProcessingTimeService timeService;
    private volatile long lastSendTime;

    /**
     * Constructor creating a Pulsar writer.
     *
     * <p>It will throw a {@link RuntimeException} if {@link
     * PulsarSerializationSchema#open(InitializationContext, PulsarSinkContext, SinkConfiguration)}
     * fails.
     *
     * @param sinkConfiguration the configuration to configure the Pulsar producer.
     * @param serializationSchema transform the incoming records into different message properties.
     * @param metadataListener the listener for querying topic metadata.
     * @param topicRouter topic router to choose topic by incoming records.
     * @param initContext context to provide information about the runtime environment.
     */
    public PulsarWriter(
            SinkConfiguration sinkConfiguration,
            PulsarSerializationSchema<IN> serializationSchema,
            TopicMetadataListener metadataListener,
            TopicRouter<IN> topicRouter,
            InitContext initContext) {
        this.sinkConfiguration = checkNotNull(sinkConfiguration);
        this.serializationSchema = checkNotNull(serializationSchema);
        this.metadataListener = checkNotNull(metadataListener);
        this.topicRouter = checkNotNull(topicRouter);
        checkNotNull(initContext);

        this.deliveryGuarantee = sinkConfiguration.getDeliveryGuarantee();
        this.sinkContextAdapter = new PulsarSinkContextAdapter(initContext, sinkConfiguration);
        this.mailboxExecutor = initContext.getMailboxExecutor();

        // Initialize topic metadata listener.
        LOG.debug("Initialize topic metadata after creating Pulsar writer.");
        this.timeService = initContext.getProcessingTimeService();
        this.metadataListener.open(sinkConfiguration, timeService);

        // Initialize topic router.
        this.topicRouter.open(sinkConfiguration);

        // Initialize the serialization schema.
        try {
            InitializationContext initializationContext =
                    initContext.asSerializationSchemaInitializationContext();
            this.serializationSchema.open(
                    initializationContext, sinkContextAdapter, sinkConfiguration);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Cannot initialize schema.", e);
        }

        // Initialize metrics
        this.pulsarSinkWriterMetrics = new PulsarSinkWriterMetrics(initContext.metricGroup());
        this.lastSendTime = 0;
        setupFlinkMetrics(pulsarSinkWriterMetrics);
        // Create this producer register after opening serialization schema!
        this.producerRegister =
                new TopicProducerRegister(sinkConfiguration, pulsarSinkWriterMetrics);
        this.pendingMessages = new Semaphore(sinkConfiguration.getMaxPendingMessages());
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        // Serialize the incoming element.
        sinkContextAdapter.updateTimestamp(context);

        // Choose the right topic to send.
        List<String> availableTopics = metadataListener.availableTopics();
        String topic = topicRouter.route(element, availableTopics, sinkContextAdapter);

        // Create message builder for sending message.
        TypedMessageBuilder<?> builder = createMessageBuilder(topic, element, sinkContextAdapter);

        // Perform message sending.
        if (deliveryGuarantee == DeliveryGuarantee.NONE) {
            // We would just ignore the sending exception. This may cause data loss.
            builder.sendAsync();
        } else {
            // Waiting for permits to write message.
            pendingMessages.acquire();
            long sendStartTime = timeService.getCurrentProcessingTime();
            CompletableFuture<MessageId> sender = builder.sendAsync();
            sender.whenComplete(
                    (id, ex) -> {
                        long sendEndTime = timeService.getCurrentProcessingTime();
                        this.lastSendTime = sendEndTime - sendStartTime;
                        pendingMessages.release();
                        if (ex != null) {
                            pulsarSinkWriterMetrics.recordNumRecordOutErrors();
                            mailboxExecutor.execute(
                                    () -> {
                                        throw new FlinkRuntimeException(
                                                "Failed to send data to Pulsar " + topic, ex);
                                    },
                                    "Failed to send message to Pulsar");
                        } else {
                            LOG.debug("Sent message to Pulsar {} with message id {}", topic, id);
                        }
                    });
        }
    }

    private TypedMessageBuilder<?> createMessageBuilder(
            String topic, IN element, PulsarSinkContextAdapter context) {
        TypedMessageBuilder<?> builder;

        if (sinkConfiguration.isEnableSchemaEvolution()) {
            Schema<IN> schema = serializationSchema.schema();
            builder = producerRegister.createMessageBuilder(topic, element, schema);
        } else {
            byte[] bytes = serializationSchema.serialize(element, context);
            builder = producerRegister.createMessageBuilder(topic, bytes, Schema.BYTES);
        }

        byte[] orderingKey = serializationSchema.orderingKey(element, context);
        if (orderingKey != null) {
            builder.orderingKey(orderingKey);
        }

        builder.key(serializationSchema.key(element, context));

        Map<String, String> properties = serializationSchema.properties(element, context);
        if (properties != null) {
            builder.properties(properties);
        }

        Long timestamp = serializationSchema.eventTime(element, context);
        if (timestamp != null) {
            builder.eventTime(timestamp);
        }

        List<String> clusters = serializationSchema.replicationClusters(element, context);
        if (clusters != null) {
            builder.replicationClusters(clusters);
        }

        if (serializationSchema.disableReplication(element, context)) {
            builder.disableReplication();
        }

        return builder;
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        if (endOfInput) {
            // Try flush only once when we meet the end of the input.
            producerRegister.flush();
        } else {
            while (pendingMessages.availablePermits() < sinkConfiguration.getMaxPendingMessages()) {
                producerRegister.flush();
            }
        }
    }

    @Override
    public Collection<PulsarCommittable> prepareCommit() throws IOException {
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            while (pendingMessages.availablePermits() < sinkConfiguration.getMaxPendingMessages()) {
                producerRegister.flush();
            }
            return producerRegister.prepareCommit();
        } else {
            return emptyList();
        }
    }

    @Override
    public void close() throws Exception {
        // Close all the resources and throw the exception at last.
        closeAll(metadataListener, producerRegister);
    }

    private void setupFlinkMetrics(PulsarSinkWriterMetrics pulsarSinkWriterMetrics) {
        pulsarSinkWriterMetrics.setCurrentSendTimeGauge(() -> lastSendTime);
    }
}
