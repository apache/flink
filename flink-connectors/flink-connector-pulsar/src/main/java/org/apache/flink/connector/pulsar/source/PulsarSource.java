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

package org.apache.flink.connector.pulsar.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.pulsar.source.config.ConfigurationDataCustomizer;
import org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumState;
import org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumStateSerializer;
import org.apache.flink.connector.pulsar.source.enumerator.PulsarSourceEnumerator;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRangeGenerator;
import org.apache.flink.connector.pulsar.source.reader.PulsarPartitionSplitReader;
import org.apache.flink.connector.pulsar.source.reader.PulsarRecordEmitter;
import org.apache.flink.connector.pulsar.source.reader.PulsarSourceReader;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarSchemaInitializationContext;
import org.apache.flink.connector.pulsar.source.reader.message.PulsarMessage;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.flink.shaded.guava18.com.google.common.io.Closer;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;

import java.util.function.Supplier;

/**
 * The Source implementation of Pulsar. Please use a {@link PulsarSourceBuilder} to construct a
 * {@link PulsarSource}. The following example shows how to create a PulsarSource emitting records
 * of <code>String</code> type.
 *
 * <p>See {@link PulsarSourceBuilder} for more details.
 *
 * @param <IN> The input type of the pulsar {@link Message<?>}
 * @param <OUT> The output type of the source.
 */
@PublicEvolving
public final class PulsarSource<IN, OUT>
        implements Source<OUT, PulsarPartitionSplit, PulsarSourceEnumState>,
                ResultTypeQueryable<OUT> {
    private static final long serialVersionUID = 7773108631275567433L;

    /** The source configuration, we don't support the pulsar's configuration class directly. */
    private final Configuration configuration;

    private final PulsarSubscriber subscriber;

    private final TopicRangeGenerator rangeGenerator;

    private final Supplier<StartCursor> startCursorSupplier;

    private final Supplier<StopCursor> stopCursorSupplier;

    /**
     * Boundedness for source, we only support {@link Boundedness#CONTINUOUS_UNBOUNDED} currently.
     */
    private final Boundedness boundedness;

    /** The pulsar deserialization schema used for deserialize message, */
    private final PulsarDeserializationSchema<IN, OUT> deserializationSchema;

    /** Modify the flink generated {@link ClientConfigurationData}. */
    private final ConfigurationDataCustomizer<ClientConfigurationData>
            clientConfigurationCustomizer;

    /** Modify the flink generated {@link ConsumerConfigurationData}. */
    private final ConfigurationDataCustomizer<ConsumerConfigurationData<IN>>
            consumerConfigurationCustomizer;

    /**
     * The constructor for PulsarSource, it's package protected for forcing using {@link
     * PulsarSourceBuilder}.
     */
    PulsarSource( // NOSONAR This constructor is not public.
            Configuration configuration,
            PulsarSubscriber subscriber,
            TopicRangeGenerator rangeGenerator,
            Supplier<StartCursor> startCursorSupplier,
            Supplier<StopCursor> stopCursorSupplier,
            Boundedness boundedness,
            PulsarDeserializationSchema<IN, OUT> deserializationSchema,
            ConfigurationDataCustomizer<ClientConfigurationData> clientConfigurationCustomizer,
            ConfigurationDataCustomizer<ConsumerConfigurationData<IN>>
                    consumerConfigurationCustomizer) {
        this.configuration = configuration;
        this.subscriber = subscriber;
        this.rangeGenerator = rangeGenerator;
        this.startCursorSupplier = startCursorSupplier;
        this.stopCursorSupplier = stopCursorSupplier;
        this.boundedness = boundedness;
        this.deserializationSchema = deserializationSchema;
        this.clientConfigurationCustomizer = clientConfigurationCustomizer;
        this.consumerConfigurationCustomizer = consumerConfigurationCustomizer;
    }

    /**
     * Get a PulsarSourceBuilder to builder a {@link PulsarSource}.
     *
     * @return a Pulsar source builder.
     */
    public static <A, B> PulsarSourceBuilder<A, B> builder() {
        return new PulsarSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return boundedness;
    }

    @Override
    public SourceReader<OUT, PulsarPartitionSplit> createReader(SourceReaderContext readerContext)
            throws Exception {
        // Initialize the deserialization schema before creating the pulsar reader.
        deserializationSchema.open(new PulsarSchemaInitializationContext(readerContext));

        FutureCompletingBlockingQueue<RecordsWithSplitIds<PulsarMessage<OUT>>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        ExecutorProvider listenerExecutor = new ExecutorProvider(1, "Pulsar listener executor");
        Closer splitCloser = Closer.create(); // NOSONAR This would be closed in PulsarSourceReader.
        splitCloser.register(listenerExecutor::shutdownNow);

        Supplier<PulsarPartitionSplitReader<OUT>> splitReaderSupplier =
                () -> {
                    PulsarPartitionSplitReader<OUT> reader =
                            new PulsarPartitionSplitReader<>(
                                    configuration,
                                    clientConfigurationCustomizer,
                                    consumerConfigurationCustomizer,
                                    deserializationSchema,
                                    listenerExecutor);

                    splitCloser.register(reader);
                    return reader;
                };
        PulsarRecordEmitter<OUT> recordEmitter = new PulsarRecordEmitter<>();

        return new PulsarSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                recordEmitter,
                configuration,
                readerContext,
                splitCloser::close);
    }

    @Override
    public SplitEnumerator<PulsarPartitionSplit, PulsarSourceEnumState> createEnumerator(
            SplitEnumeratorContext<PulsarPartitionSplit> enumContext) {
        return new PulsarSourceEnumerator(
                subscriber,
                rangeGenerator,
                startCursorSupplier,
                stopCursorSupplier,
                configuration,
                enumContext);
    }

    @Override
    public SplitEnumerator<PulsarPartitionSplit, PulsarSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<PulsarPartitionSplit> enumContext,
            PulsarSourceEnumState checkpoint) {
        return new PulsarSourceEnumerator(
                subscriber,
                rangeGenerator,
                startCursorSupplier,
                stopCursorSupplier,
                configuration,
                enumContext,
                checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<PulsarPartitionSplit> getSplitSerializer() {
        return new PulsarPartitionSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<PulsarSourceEnumState> getEnumeratorCheckpointSerializer() {
        return new PulsarSourceEnumStateSerializer();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
