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
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.elasticsearch.source.Elasticsearch7Source;
import org.apache.flink.connector.elasticsearch.source.Elasticsearch7SourceBuilder;
import org.apache.flink.connector.elasticsearch.source.reader.Elasticsearch7SearchHitDeserializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;

import org.apache.http.HttpHost;

import javax.annotation.Nullable;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** An Elasticsearch {@link ScanTableSource}. */
@Internal
public class Elasticsearch7DynamicTableSource
        implements ScanTableSource, SupportsWatermarkPushDown {

    /** Data type that describes the final output of the source. */
    private final DataType producedDataType;

    private final Elasticsearch7DynamicTableSourceConfiguration sourceConfig;

    private final String tableIdentifier;

    private final boolean failOnMissingFields;

    private final boolean ignoreParseErrors;

    private final TimestampFormat timestampFormat;

    protected @Nullable WatermarkStrategy<RowData> watermarkStrategy;

    public Elasticsearch7DynamicTableSource(
            DataType producedDataType,
            Elasticsearch7DynamicTableSourceConfiguration sourceConfig,
            String tableIdentifier,
            boolean failOnMissingFields,
            boolean ignoreParseErrors,
            TimestampFormat timestampFormat) {
        this.producedDataType = checkNotNull(producedDataType);
        this.sourceConfig = checkNotNull(sourceConfig);
        this.tableIdentifier = checkNotNull(tableIdentifier);
        this.failOnMissingFields = failOnMissingFields;
        this.ignoreParseErrors = ignoreParseErrors;
        this.timestampFormat = checkNotNull(timestampFormat);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        final TypeInformation<RowData> producedTypeInformation =
                context.createTypeInformation(producedDataType);

        final Elasticsearch7SearchHitDeserializationSchema<RowData> elasticsearchDeserializer =
                new RowDataElasticsearch7SearchHitDeserializationSchema(
                        (RowType) producedDataType.getLogicalType(),
                        producedTypeInformation,
                        failOnMissingFields,
                        ignoreParseErrors,
                        timestampFormat);

        final Elasticsearch7Source<RowData> elasticsearchSource =
                createElasticsearchSource(elasticsearchDeserializer);

        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                return execEnv.fromSource(
                        elasticsearchSource,
                        (watermarkStrategy != null)
                                ? watermarkStrategy
                                : WatermarkStrategy.noWatermarks(),
                        "ElasticsearchSource-" + tableIdentifier);
            }

            @Override
            public boolean isBounded() {
                return elasticsearchSource.getBoundedness() == Boundedness.BOUNDED;
            }
        };
    }

    Elasticsearch7Source<RowData> createElasticsearchSource(
            Elasticsearch7SearchHitDeserializationSchema<RowData> elasticsearchDeserializer) {

        final Elasticsearch7SourceBuilder<RowData> builder = Elasticsearch7Source.builder();

        builder.setDeserializationSchema(elasticsearchDeserializer);
        builder.setHosts(sourceConfig.getHosts().toArray(new HttpHost[0]));
        builder.setIndexName(sourceConfig.getIndex());
        builder.setNumberOfSearchSlices(sourceConfig.getNumberOfSlices());
        builder.setPitKeepAlive(sourceConfig.getPitKeepAlive());

        if (sourceConfig.getUsername().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(sourceConfig.getUsername().get())) {
            builder.setConnectionUsername(sourceConfig.getUsername().get());
        }

        if (sourceConfig.getPassword().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(sourceConfig.getPassword().get())) {
            builder.setConnectionPassword(sourceConfig.getPassword().get());
        }

        if (sourceConfig.getPathPrefix().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(sourceConfig.getPathPrefix().get())) {
            builder.setConnectionPathPrefix(sourceConfig.getPathPrefix().get());
        }

        if (sourceConfig.getConnectionRequestTimeout().isPresent()) {
            builder.setConnectionRequestTimeout(
                    (int) sourceConfig.getConnectionRequestTimeout().get().getSeconds());
        }

        if (sourceConfig.getConnectionTimeout().isPresent()) {
            builder.setConnectionTimeout(
                    (int) sourceConfig.getConnectionTimeout().get().getSeconds());
        }

        if (sourceConfig.getSocketTimeout().isPresent()) {
            builder.setSocketTimeout((int) sourceConfig.getSocketTimeout().get().getSeconds());
        }

        return builder.build();
    }

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public DynamicTableSource copy() {
        Elasticsearch7DynamicTableSource copy =
                new Elasticsearch7DynamicTableSource(
                        producedDataType,
                        sourceConfig,
                        tableIdentifier,
                        failOnMissingFields,
                        ignoreParseErrors,
                        timestampFormat);
        copy.watermarkStrategy = watermarkStrategy;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return "Elasticsearch table source";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Elasticsearch7DynamicTableSource that = (Elasticsearch7DynamicTableSource) o;
        return failOnMissingFields == that.failOnMissingFields
                && ignoreParseErrors == that.ignoreParseErrors
                && Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(sourceConfig, that.sourceConfig)
                && Objects.equals(tableIdentifier, that.tableIdentifier)
                && timestampFormat == that.timestampFormat
                && Objects.equals(watermarkStrategy, that.watermarkStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                producedDataType,
                sourceConfig,
                tableIdentifier,
                failOnMissingFields,
                ignoreParseErrors,
                timestampFormat,
                watermarkStrategy);
    }
}
