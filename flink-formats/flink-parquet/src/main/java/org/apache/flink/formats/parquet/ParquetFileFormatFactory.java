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

package org.apache.flink.formats.parquet;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.BulkDecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.vector.VectorizedColumnBatch;
import org.apache.flink.table.factories.BulkReaderFormatFactory;
import org.apache.flink.table.factories.BulkWriterFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.filesystem.FileSystemConnectorOptions;
import org.apache.flink.table.filesystem.PartitionFieldExtractor;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.conf.Configuration;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Parquet format factory for file system. */
public class ParquetFileFormatFactory implements BulkReaderFormatFactory, BulkWriterFormatFactory {

    public static final String IDENTIFIER = "parquet";

    public static final ConfigOption<Boolean> UTC_TIMEZONE =
            key("utc-timezone")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Use UTC timezone or local timezone to the conversion between epoch"
                                    + " time and LocalDateTime. Hive 0.x/1.x/2.x use local timezone. But Hive 3.x"
                                    + " use UTC timezone");

    @Override
    public BulkDecodingFormat<RowData> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        return new BulkDecodingFormat<RowData>() {
            @Override
            public BulkFormat<RowData, FileSourceSplit> createRuntimeDecoder(
                    DynamicTableSource.Context sourceContext, DataType producedDataType) {
                String defaultPartName =
                        context.getCatalogTable()
                                .getOptions()
                                .getOrDefault(
                                        FileSystemConnectorOptions.PARTITION_DEFAULT_NAME.key(),
                                        FileSystemConnectorOptions.PARTITION_DEFAULT_NAME
                                                .defaultValue());
                return ParquetColumnarRowInputFormat.createPartitionedFormat(
                        getParquetConfiguration(formatOptions),
                        (RowType) producedDataType.getLogicalType(),
                        context.getCatalogTable().getPartitionKeys(),
                        PartitionFieldExtractor.forFileSystem(defaultPartName),
                        VectorizedColumnBatch.DEFAULT_SIZE,
                        formatOptions.get(UTC_TIMEZONE),
                        true);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<BulkWriter.Factory<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        return new EncodingFormat<BulkWriter.Factory<RowData>>() {
            @Override
            public BulkWriter.Factory<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context sinkContext, DataType consumedDataType) {
                return ParquetRowDataBuilder.createWriterFactory(
                        (RowType) consumedDataType.getLogicalType(),
                        getParquetConfiguration(formatOptions),
                        formatOptions.get(UTC_TIMEZONE));
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    private static Configuration getParquetConfiguration(ReadableConfig options) {
        Configuration conf = new Configuration();
        Properties properties = new Properties();
        ((org.apache.flink.configuration.Configuration) options).addAllToProperties(properties);
        properties.forEach((k, v) -> conf.set(IDENTIFIER + "." + k, v.toString()));
        return conf;
    }

    @Override
    public String factoryIdentifier() {
        return "parquet";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }
}
