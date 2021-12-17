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

package org.apache.flink.connector.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSinkBuilderBase;
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

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DynamicTableSink} that describes how to create a {@link ElasticsearchSink} from a
 * logical description.
 */
@Internal
class ElasticsearchDynamicSink implements DynamicTableSink {

    final EncodingFormat<SerializationSchema<RowData>> format;
    final DataType physicalRowDataType;
    final List<LogicalTypeWithIndex> primaryKeyLogicalTypesWithIndex;
    final ElasticsearchConfiguration config;

    final String summaryString;
    final ElasticsearchSinkBuilderSupplier<RowData> builderSupplier;
    @Nullable final String documentType;

    ElasticsearchDynamicSink(
            EncodingFormat<SerializationSchema<RowData>> format,
            ElasticsearchConfiguration config,
            List<LogicalTypeWithIndex> primaryKeyLogicalTypesWithIndex,
            DataType physicalRowDataType,
            String summaryString,
            ElasticsearchSinkBuilderSupplier<RowData> builderSupplier,
            @Nullable String documentType) {
        this.format = checkNotNull(format);
        this.physicalRowDataType = checkNotNull(physicalRowDataType);
        this.primaryKeyLogicalTypesWithIndex = checkNotNull(primaryKeyLogicalTypesWithIndex);
        this.config = checkNotNull(config);
        this.summaryString = checkNotNull(summaryString);
        this.builderSupplier = checkNotNull(builderSupplier);
        this.documentType = documentType;
    }

    Function<RowData, String> createKeyExtractor() {
        return KeyExtractor.createKeyExtractor(
                primaryKeyLogicalTypesWithIndex, config.getKeyDelimiter());
    }

    IndexGenerator createIndexGenerator() {
        return IndexGeneratorFactory.createIndexGenerator(
                config.getIndex(),
                DataType.getFieldNames(physicalRowDataType),
                DataType.getFieldDataTypes(physicalRowDataType));
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
                        createIndexGenerator(),
                        format,
                        XContentType.JSON,
                        documentType,
                        createKeyExtractor());

        ElasticsearchSinkBuilderBase<RowData, ? extends ElasticsearchSinkBuilderBase> builder =
                builderSupplier.get();
        builder.setEmitter(rowElasticsearchEmitter);
        builder.setHosts(config.getHosts().toArray(new HttpHost[0]));
        builder.setDeliveryGuarantee(config.getDeliveryGuarantee());
        builder.setBulkFlushMaxActions(config.getBulkFlushMaxActions());
        builder.setBulkFlushMaxSizeMb(config.getBulkFlushMaxByteSize().getMebiBytes());
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

        if (config.getConnectionRequestTimeout().isPresent()) {
            builder.setConnectionRequestTimeout(
                    (int) config.getConnectionRequestTimeout().get().getSeconds());
        }

        if (config.getConnectionTimeout().isPresent()) {
            builder.setConnectionTimeout((int) config.getConnectionTimeout().get().getSeconds());
        }

        if (config.getSocketTimeout().isPresent()) {
            builder.setSocketTimeout((int) config.getSocketTimeout().get().getSeconds());
        }

        return SinkProvider.of(builder.build());
    }

    @Override
    public DynamicTableSink copy() {
        return new ElasticsearchDynamicSink(
                format,
                config,
                primaryKeyLogicalTypesWithIndex,
                physicalRowDataType,
                summaryString,
                builderSupplier,
                documentType);
    }

    @Override
    public String asSummaryString() {
        return summaryString;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ElasticsearchDynamicSink that = (ElasticsearchDynamicSink) o;
        return Objects.equals(format, that.format)
                && Objects.equals(physicalRowDataType, that.physicalRowDataType)
                && Objects.equals(
                        primaryKeyLogicalTypesWithIndex, that.primaryKeyLogicalTypesWithIndex)
                && Objects.equals(config, that.config)
                && Objects.equals(summaryString, that.summaryString)
                && Objects.equals(builderSupplier, that.builderSupplier)
                && Objects.equals(documentType, that.documentType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                format,
                physicalRowDataType,
                primaryKeyLogicalTypesWithIndex,
                config,
                summaryString,
                builderSupplier,
                documentType);
    }
}
