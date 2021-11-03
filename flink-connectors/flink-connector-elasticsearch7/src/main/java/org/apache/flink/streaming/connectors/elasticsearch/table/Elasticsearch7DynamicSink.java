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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.FlushBackoffType;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.StringUtils;

import org.apache.http.HttpHost;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSink} that describes how to create a {@link ElasticsearchSink} from a
 * logical description.
 */
@Internal
final class Elasticsearch7DynamicSink implements DynamicTableSink {

    private final EncodingFormat<SerializationSchema<RowData>> format;
    private final DataType physicalRowDataType;
    private final List<LogicalTypeWithIndex> primaryKeyLogicalTypesWithIndex;
    private final Elasticsearch7Configuration config;

    Elasticsearch7DynamicSink(
            EncodingFormat<SerializationSchema<RowData>> format,
            Elasticsearch7Configuration config,
            List<LogicalTypeWithIndex> primaryKeyLogicalTypesWithIndex,
            DataType physicalRowDataType) {
        this.format = checkNotNull(format);
        this.physicalRowDataType = checkNotNull(physicalRowDataType);
        this.primaryKeyLogicalTypesWithIndex = checkNotNull(primaryKeyLogicalTypesWithIndex);
        this.config = checkNotNull(config);
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        ChangelogMode.Builder builder = ChangelogMode.newBuilder();
        for (RowKind kind : requestedMode.getContainedKinds()) {
            if (kind != RowKind.UPDATE_BEFORE) {
                builder.addContainedKind(kind);
            }
        }
        return builder.build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        SerializationSchema<RowData> format =
                this.format.createRuntimeEncoder(context, physicalRowDataType);

        final RowElasticsearchEmitter rowElasticsearchEmitter =
                new RowElasticsearchEmitter(
                        IndexGeneratorFactory.createIndexGenerator(
                                config.getIndex(),
                                DataType.getFieldNames(physicalRowDataType),
                                DataType.getFieldDataTypes(physicalRowDataType)),
                        format,
                        XContentType.JSON,
                        KeyExtractor.createKeyExtractor(
                                primaryKeyLogicalTypesWithIndex, config.getKeyDelimiter()));

        final ElasticsearchSinkBuilder<RowData> builder = ElasticsearchSink.builder();
        builder.setEmitter(rowElasticsearchEmitter);
        builder.setHosts(config.getHosts().toArray(new HttpHost[0]));
        builder.setDeliveryGuarantee(config.getDeliveryGuarantee());
        builder.setBulkFlushMaxActions(config.getBulkFlushMaxActions());
        builder.setBulkFlushMaxSizeMb((int) (config.getBulkFlushMaxByteSize() >> 20));
        builder.setBulkFlushInterval(config.getBulkFlushInterval());

        if (config.getBulkFlushBackoffType().isPresent()) {
            FlushBackoffType backoffType = config.getBulkFlushBackoffType().get();
            int backoffMaxRetries = config.getBulkFlushBackoffRetries().get();
            long backoffDelayMs = config.getBulkFlushBackoffDelay().get();

            builder.setBulkFlushBackoffStrategy(backoffType, backoffMaxRetries, backoffDelayMs);
        }

        if (config.getUsername().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getUsername().get())) {
            builder.setConnectionUsername(config.getUsername().get());
        }

        if (config.getPassword().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getPassword().get())) {
            builder.setConnectionPassword(config.getPassword().get());
        }

        if (config.getPathPrefix().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getPathPrefix().get())) {
            builder.setConnectionPathPrefix(config.getPathPrefix().get());
        }

        return SinkProvider.of(builder.build());
    }

    @Override
    public DynamicTableSink copy() {
        return new Elasticsearch7DynamicSink(
                format, config, primaryKeyLogicalTypesWithIndex, physicalRowDataType);
    }

    @Override
    public String asSummaryString() {
        return "Elasticsearch7";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Elasticsearch7DynamicSink that = (Elasticsearch7DynamicSink) o;
        return Objects.equals(format, that.format)
                && Objects.equals(physicalRowDataType, that.physicalRowDataType)
                && Objects.equals(
                        primaryKeyLogicalTypesWithIndex, that.primaryKeyLogicalTypesWithIndex)
                && Objects.equals(config, that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(format, physicalRowDataType, primaryKeyLogicalTypesWithIndex, config);
    }
}
