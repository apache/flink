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

package org.apache.flink.streaming.connectors.gcp.pubsub.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.connectors.gcp.pubsub.DeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;
import org.apache.flink.util.Preconditions;

import com.google.auth.Credentials;

import java.io.IOException;
import java.util.Properties;
import java.util.function.Supplier;

/** */
public class PubSubSource<OUT>
        implements Source<OUT, PubSubSplit, PubSubEnumeratorCheckpoint>, ResultTypeQueryable<OUT> {
    protected final PubSubDeserializationSchema<OUT> deserializationSchema;

    private final Boundedness boundedness;
    private final Properties props;
    private final Credentials credentials;
    private final String projectName;
    private final String subscriptionName;
    private final String hostAndPort;

    //	TODO: make private and provide builder
    public PubSubSource(
            PubSubDeserializationSchema<OUT> deserializationSchema,
            Boundedness boundedness,
            Properties props,
            Credentials credentials,
            String projectName,
            String subscriptionName,
            String hostAndPort) {
        this.deserializationSchema = deserializationSchema;
        this.boundedness = boundedness;
        this.props = props;
        this.credentials = credentials;
        this.projectName = projectName;
        this.subscriptionName = subscriptionName;
        this.hostAndPort = hostAndPort;
    }

    public PubSubSource(
            DeserializationSchema<OUT> deserializationSchema,
            Boundedness boundedness,
            Properties props,
            Credentials credentials,
            String projectName,
            String subscriptionName,
            String hostAndPort) {
        Preconditions.checkNotNull(deserializationSchema);
        this.deserializationSchema = new DeserializationSchemaWrapper<>(deserializationSchema);
        this.boundedness = boundedness;
        this.props = props;
        this.credentials = credentials;
        this.projectName = projectName;
        this.subscriptionName = subscriptionName;
        this.hostAndPort = hostAndPort;
    }

    @Override
    public Boundedness getBoundedness() {
        return this.boundedness;
    }

    @Override
    public SourceReader<OUT, PubSubSplit> createReader(SourceReaderContext readerContext) {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple2<OUT, Long>>> elementsQueue =
                new FutureCompletingBlockingQueue<>();
        Supplier<PubSubSplitReader<OUT>> splitReaderSupplier =
                () -> {
                    try {
                        return new PubSubSplitReader<>(
                                deserializationSchema, projectName, subscriptionName, hostAndPort);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return null;
                    }
                };
        PubSubRecordEmitter<OUT> recordEmitter = new PubSubRecordEmitter<>();

        return new PubSubSourceReader<>(
                elementsQueue,
                splitReaderSupplier,
                recordEmitter,
                toConfiguration(props),
                readerContext);
    }

    @Override
    public SplitEnumerator<PubSubSplit, PubSubEnumeratorCheckpoint> createEnumerator(
            SplitEnumeratorContext<PubSubSplit> enumContext) {
        return new PubSubSourceEnumerator(enumContext);
    }

    @Override
    public SplitEnumerator<PubSubSplit, PubSubEnumeratorCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<PubSubSplit> enumContext,
            PubSubEnumeratorCheckpoint checkpoint) {
        return new PubSubSourceEnumerator(enumContext);
    }

    @Override
    public SimpleVersionedSerializer<PubSubSplit> getSplitSerializer() {
        return new PubSubSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<PubSubEnumeratorCheckpoint>
            getEnumeratorCheckpointSerializer() {
        return null;
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    // ----------- private helper methods ---------------

    private Configuration toConfiguration(Properties props) {
        Configuration config = new Configuration();
        props.stringPropertyNames().forEach(key -> config.setString(key, props.getProperty(key)));
        return config;
    }
}
