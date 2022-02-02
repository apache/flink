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
import org.apache.flink.connector.pulsar.sink.writer.message.RawMessage;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRouter;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.connector.pulsar.sink.writer.topic.TopicMetadataListener;
import org.apache.flink.connector.pulsar.sink.writer.topic.TopicProducerRegister;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.SerializableFunction;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

import static java.util.Collections.emptyList;
import static org.apache.flink.util.IOUtils.closeAll;

/**
 * This class is responsible to write records in a Pulsar topic and to handle the different delivery
 * {@link DeliveryGuarantee}s.
 *
 * @param <IN> The type of the input elements.
 */
public class PulsarWriter<IN> implements PrecommittingSinkWriter<IN, PulsarCommittable> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarWriter.class);

    private final SinkConfiguration sinkConfiguration;
    private final DeliveryGuarantee deliveryGuarantee;
    private final PulsarSerializationSchema<IN> serializationSchema;
    private final TopicRouter<IN> topicRouter;
    private final PulsarSinkContextAdapter sinkContextAdapter;
    private final TopicMetadataListener metadataListener;
    private final MailboxExecutor mailboxExecutor;
    private final TopicProducerRegister producerRegister;
    private final Semaphore pendingMessages;

    /**
     * Constructor creating a Pulsar writer.
     *
     * <p>It will throw a {@link RuntimeException} if {@link
     * PulsarSerializationSchema#open(InitializationContext, PulsarSinkContext, SinkConfiguration)}
     * fails.
     *
     * @param sinkConfiguration the configuration to configure the Pulsar producer.
     * @param serializationSchema serialize to transform the incoming records to {@link RawMessage}.
     * @param metadataListener the listener for querying topic metadata.
     * @param topicRouterProvider create related topic router to choose topic by incoming records.
     * @param initContext context to provide information about the runtime environment.
     */
    public PulsarWriter(
            SinkConfiguration sinkConfiguration,
            PulsarSerializationSchema<IN> serializationSchema,
            TopicMetadataListener metadataListener,
            SerializableFunction<SinkConfiguration, TopicRouter<IN>> topicRouterProvider,
            InitContext initContext) {
        this.sinkConfiguration = sinkConfiguration;
        this.deliveryGuarantee = sinkConfiguration.getDeliveryGuarantee();
        this.serializationSchema = serializationSchema;
        this.topicRouter = topicRouterProvider.apply(sinkConfiguration);
        this.sinkContextAdapter = new PulsarSinkContextAdapter(initContext, sinkConfiguration);
        this.metadataListener = metadataListener;
        this.mailboxExecutor = initContext.getMailboxExecutor();

        // Initialize topic metadata listener.
        LOG.debug("Initialize topic metadata after creating Pulsar writer.");
        ProcessingTimeService timeService = initContext.getProcessingTimeService();
        metadataListener.open(sinkConfiguration, timeService);

        // Initialize topic router.
        topicRouter.open(sinkConfiguration);

        // Initialize the serialization schema.
        try {
            InitializationContext initializationContext =
                    initContext.asSerializationSchemaInitializationContext();
            serializationSchema.open(initializationContext, sinkContextAdapter, sinkConfiguration);
        } catch (Exception e) {
            throw new FlinkRuntimeException("Cannot initialize schema.", e);
        }

        // Create this producer register after opening serialization schema!
        this.producerRegister = new TopicProducerRegister(sinkConfiguration, serializationSchema);
        this.pendingMessages = new Semaphore(sinkConfiguration.getMaxPendingMessages());
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(IN element, Context context) throws IOException, InterruptedException {
        // Serialize the incoming element.
        sinkContextAdapter.updateTimestamp(context);
        RawMessage<byte[]> message = serializationSchema.serialize(element, sinkContextAdapter);

        // Choose the right topic to send.
        List<String> availableTopics = metadataListener.availableTopics();
        String topic = topicRouter.route(element, message, availableTopics, sinkContextAdapter);

        // Create message builder for sending message.
        TypedMessageBuilder<?> builder = createMessageBuilder(topic, deliveryGuarantee);
        if (sinkConfiguration.isEnableSchemaEvolution()) {
            ((TypedMessageBuilder<IN>) builder).value(element);
        } else {
            ((TypedMessageBuilder<byte[]>) builder).value(message.getValue());
        }
        message.supplement(builder);

        // Perform message sending.
        if (deliveryGuarantee == DeliveryGuarantee.NONE) {
            // We would just ignore the sending exception. This may cause data loss.
            builder.sendAsync();
        } else {
            // Waiting for permits to write message.
            pendingMessages.acquire();
            CompletableFuture<MessageId> sender = builder.sendAsync();
            sender.whenComplete(
                    (id, ex) -> {
                        pendingMessages.release();
                        if (ex != null) {
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
            String topic, DeliveryGuarantee deliveryGuarantee) {
        Producer<?> producer = producerRegister.getOrCreateProducer(topic);
        if (deliveryGuarantee == DeliveryGuarantee.EXACTLY_ONCE) {
            Transaction transaction = producerRegister.getOrCreateTransaction(topic);
            return producer.newMessage(transaction);
        } else {
            return producer.newMessage();
        }
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        while (pendingMessages.availablePermits() < sinkConfiguration.getMaxPendingMessages()) {
            producerRegister.flush();
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
}
