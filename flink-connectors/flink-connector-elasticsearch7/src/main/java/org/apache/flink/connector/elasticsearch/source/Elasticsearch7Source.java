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

package org.apache.flink.connector.elasticsearch.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.elasticsearch.common.NetworkClientConfig;
import org.apache.flink.connector.elasticsearch.source.enumerator.Elasticsearch7SourceEnumState;
import org.apache.flink.connector.elasticsearch.source.enumerator.Elasticsearch7SourceEnumStateSerializer;
import org.apache.flink.connector.elasticsearch.source.enumerator.Elasticsearch7SourceEnumerator;
import org.apache.flink.connector.elasticsearch.source.reader.Elasticsearch7SearchHitDeserializationSchema;
import org.apache.flink.connector.elasticsearch.source.reader.Elasticsearch7SourceReader;
import org.apache.flink.connector.elasticsearch.source.split.Elasticsearch7Split;
import org.apache.flink.connector.elasticsearch.source.split.Elasticsearch7SplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

/**
 * The Source implementation for Elasticsearch. Please use a {@link Elasticsearch7SourceBuilder} to
 * construct a {@link Elasticsearch7Source}. The following example shows how to create a
 * ElasticsearchSource emitting records of <code>
 * String</code> type.
 *
 * <pre>{@code
 * TODO: add example
 * }</pre>
 *
 * <p>See {@link Elasticsearch7SourceBuilder} for more details.
 *
 * @param <OUT> the output type of the source.
 */
@PublicEvolving
public class Elasticsearch7Source<OUT>
        implements Source<OUT, Elasticsearch7Split, Elasticsearch7SourceEnumState>,
                ResultTypeQueryable<OUT> {

    private final Elasticsearch7SearchHitDeserializationSchema<OUT> deserializationSchema;
    private final Elasticsearch7SourceConfiguration sourceConfiguration;
    private final NetworkClientConfig networkClientConfig;

    Elasticsearch7Source(
            Elasticsearch7SearchHitDeserializationSchema<OUT> deserializationSchema,
            Elasticsearch7SourceConfiguration sourceConfiguration,
            NetworkClientConfig networkClientConfig) {
        this.deserializationSchema = deserializationSchema;
        this.sourceConfiguration = sourceConfiguration;
        this.networkClientConfig = networkClientConfig;
    }

    /**
     * Create an {@link Elasticsearch7SourceBuilder} to construct a new {@link
     * Elasticsearch7Source}.
     *
     * @param <OUT> the produced output type
     * @return {@link Elasticsearch7SourceBuilder}
     */
    public static <OUT> Elasticsearch7SourceBuilder<OUT> builder() {
        return new Elasticsearch7SourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<OUT, Elasticsearch7Split> createReader(SourceReaderContext readerContext)
            throws Exception {

        deserializationSchema.open(
                new DeserializationSchema.InitializationContext() {
                    @Override
                    public MetricGroup getMetricGroup() {
                        return readerContext.metricGroup().addGroup("deserializer");
                    }

                    @Override
                    public UserCodeClassLoader getUserCodeClassLoader() {
                        return readerContext.getUserCodeClassLoader();
                    }
                });

        return new Elasticsearch7SourceReader<>(
                readerContext.getConfiguration(),
                readerContext,
                sourceConfiguration,
                networkClientConfig,
                deserializationSchema);
    }

    @Override
    public SplitEnumerator<Elasticsearch7Split, Elasticsearch7SourceEnumState> createEnumerator(
            SplitEnumeratorContext<Elasticsearch7Split> enumContext) throws Exception {
        return new Elasticsearch7SourceEnumerator(
                sourceConfiguration, networkClientConfig, enumContext);
    }

    @Override
    public SplitEnumerator<Elasticsearch7Split, Elasticsearch7SourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<Elasticsearch7Split> enumContext,
            Elasticsearch7SourceEnumState checkpoint)
            throws Exception {
        return new Elasticsearch7SourceEnumerator(
                sourceConfiguration,
                networkClientConfig,
                enumContext,
                checkpoint.getAssignedSplits());
    }

    @Override
    public SimpleVersionedSerializer<Elasticsearch7Split> getSplitSerializer() {
        return new Elasticsearch7SplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Elasticsearch7SourceEnumState>
            getEnumeratorCheckpointSerializer() {
        return new Elasticsearch7SourceEnumStateSerializer();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
