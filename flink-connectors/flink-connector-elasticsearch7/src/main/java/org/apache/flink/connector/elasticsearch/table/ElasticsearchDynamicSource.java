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
import org.apache.flink.connector.elasticsearch.source.ElasticsearchSource;
import org.apache.flink.connector.elasticsearch.source.ElasticsearchSourceBuilder;
import org.apache.flink.connector.elasticsearch.source.reader.ElasticsearchSearchHitDeserializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.http.HttpHost;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** A Elasticsearch {@link ScanTableSource}. */
@Internal
public class ElasticsearchDynamicSource implements ScanTableSource {

    /** Data type that describes the final output of the source. */
    private final DataType producedDataType;

    private final ElasticsearchSourceConfig sourceConfig;

    private final String tableIdentifier;

    private final boolean failOnMissingFields;

    private final boolean ignoreParseErrors;

    private final TimestampFormat timestampFormat;

    public ElasticsearchDynamicSource(
            DataType producedDataType,
            ElasticsearchSourceConfig sourceConfig,
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

        final ElasticsearchSearchHitDeserializationSchema<RowData> elasticsearchDeserializer =
                new DynamicElasticsearchDeserializationSchema(
                        (RowType) producedDataType.getLogicalType(),
                        producedTypeInformation,
                        failOnMissingFields,
                        ignoreParseErrors,
                        timestampFormat);

        final ElasticsearchSource<RowData> elasticsearchSource =
                createElasticsearchSource(elasticsearchDeserializer);

        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(StreamExecutionEnvironment execEnv) {
                return execEnv.fromSource(
                        elasticsearchSource,
                        WatermarkStrategy.noWatermarks(),
                        "ElasticsearchSource-" + tableIdentifier);
            }

            @Override
            public boolean isBounded() {
                return elasticsearchSource.getBoundedness() == Boundedness.BOUNDED;
            }
        };
    }

    ElasticsearchSource<RowData> createElasticsearchSource(
            ElasticsearchSearchHitDeserializationSchema<RowData> elasticsearchDeserializer) {

        final ElasticsearchSourceBuilder<RowData> builder = ElasticsearchSource.builder();

        builder.setDeserializationSchema(elasticsearchDeserializer);
        // TODO: add NetworkClientConfig options here
        builder.setHosts(sourceConfig.getHosts().toArray(new HttpHost[0]));
        builder.setIndexName(sourceConfig.getIndex());
        builder.setNumberOfSearchSlices(sourceConfig.getNumberOfSlices());
        builder.setPitKeepAlive(sourceConfig.getPitKeepAlive());

        return builder.build();
    }

    @Override
    public DynamicTableSource copy() {
        return new ElasticsearchDynamicSource(
                producedDataType,
                sourceConfig,
                tableIdentifier,
                failOnMissingFields,
                ignoreParseErrors,
                timestampFormat);
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
        ElasticsearchDynamicSource that = (ElasticsearchDynamicSource) o;
        return failOnMissingFields == that.failOnMissingFields
                && ignoreParseErrors == that.ignoreParseErrors
                && Objects.equals(producedDataType, that.producedDataType)
                && Objects.equals(sourceConfig, that.sourceConfig)
                && Objects.equals(tableIdentifier, that.tableIdentifier)
                && timestampFormat == that.timestampFormat;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                producedDataType,
                sourceConfig,
                tableIdentifier,
                failOnMissingFields,
                ignoreParseErrors,
                timestampFormat);
    }
}
