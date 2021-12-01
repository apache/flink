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
import org.apache.flink.connector.elasticsearch.source.enumerator.ElasticsearchEnumState;
import org.apache.flink.connector.elasticsearch.source.enumerator.ElasticsearchEnumStateSerializer;
import org.apache.flink.connector.elasticsearch.source.enumerator.ElasticsearchEnumerator;
import org.apache.flink.connector.elasticsearch.source.reader.ElasticsearchSearchHitDeserializationSchema;
import org.apache.flink.connector.elasticsearch.source.reader.ElasticsearchSourceReader;
import org.apache.flink.connector.elasticsearch.source.split.ElasticsearchSplit;
import org.apache.flink.connector.elasticsearch.source.split.ElasticsearchSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

/**
 * The Source implementation for Elasticsearch. Please use a {@link ElasticsearchSourceBuilder} to
 * construct a {@link ElasticsearchSource}. The following example shows how to create a
 * ElasticsearchSource emitting records of <code>
 * String</code> type.
 *
 * <pre>{@code
 * TODO: add example
 * }</pre>
 *
 * <p>See {@link ElasticsearchSourceBuilder} for more details.
 *
 * @param <OUT> the output type of the source.
 */
@PublicEvolving
public class ElasticsearchSource<OUT>
        implements Source<OUT, ElasticsearchSplit, ElasticsearchEnumState>,
                ResultTypeQueryable<OUT> {

    private final ElasticsearchSearchHitDeserializationSchema<OUT> deserializationSchema;
    private final ElasticsearchSourceConfiguration sourceConfiguration;
    private final NetworkClientConfig networkClientConfig;

    ElasticsearchSource(
            ElasticsearchSearchHitDeserializationSchema<OUT> deserializationSchema,
            ElasticsearchSourceConfiguration sourceConfiguration,
            NetworkClientConfig networkClientConfig) {
        this.deserializationSchema = deserializationSchema;
        this.sourceConfiguration = sourceConfiguration;
        this.networkClientConfig = networkClientConfig;
    }

    /**
     * Create an {@link ElasticsearchSourceBuilder} to construct a new {@link ElasticsearchSource}.
     *
     * @param <OUT> the produced output type
     * @return {@link ElasticsearchSourceBuilder}
     */
    public static <OUT> ElasticsearchSourceBuilder<OUT> builder() {
        return new ElasticsearchSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<OUT, ElasticsearchSplit> createReader(SourceReaderContext readerContext)
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

        return new ElasticsearchSourceReader<>(
                readerContext.getConfiguration(),
                readerContext,
                sourceConfiguration,
                networkClientConfig,
                deserializationSchema);
    }

    @Override
    public SplitEnumerator<ElasticsearchSplit, ElasticsearchEnumState> createEnumerator(
            SplitEnumeratorContext<ElasticsearchSplit> enumContext) throws Exception {
        return new ElasticsearchEnumerator(sourceConfiguration, networkClientConfig, enumContext);
    }

    @Override
    public SplitEnumerator<ElasticsearchSplit, ElasticsearchEnumState> restoreEnumerator(
            SplitEnumeratorContext<ElasticsearchSplit> enumContext,
            ElasticsearchEnumState checkpoint)
            throws Exception {
        return new ElasticsearchEnumerator(
                sourceConfiguration,
                networkClientConfig,
                enumContext,
                checkpoint.getAssignedSplits());
    }

    @Override
    public SimpleVersionedSerializer<ElasticsearchSplit> getSplitSerializer() {
        return new ElasticsearchSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<ElasticsearchEnumState> getEnumeratorCheckpointSerializer() {
        return new ElasticsearchEnumStateSerializer();
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return deserializationSchema.getProducedType();
    }
}
