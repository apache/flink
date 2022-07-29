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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.table.factories.BulkReaderFormatFactory;
import org.apache.flink.connector.file.table.factories.BulkWriterFormatFactory;
import org.apache.flink.connector.file.table.format.BulkDecodingFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.row.ParquetRowDataBuilder;
import org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.FileBasedStatisticsReportableInputFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.DateTimeUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

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
        return new ParquetBulkDecodingFormat(formatOptions);
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

    /**
     * ParquetBulkDecodingFormat which implements {@link FileBasedStatisticsReportableInputFormat}.
     */
    @VisibleForTesting
    public static class ParquetBulkDecodingFormat
            implements ProjectableDecodingFormat<BulkFormat<RowData, FileSourceSplit>>,
                    BulkDecodingFormat<RowData>,
                    FileBasedStatisticsReportableInputFormat {

        private final ReadableConfig formatOptions;

        public ParquetBulkDecodingFormat(ReadableConfig formatOptions) {
            this.formatOptions = formatOptions;
        }

        @Override
        public BulkFormat<RowData, FileSourceSplit> createRuntimeDecoder(
                DynamicTableSource.Context sourceContext,
                DataType producedDataType,
                int[][] projections) {

            return ParquetColumnarRowInputFormat.createPartitionedFormat(
                    getParquetConfiguration(formatOptions),
                    (RowType) Projection.of(projections).project(producedDataType).getLogicalType(),
                    sourceContext.createTypeInformation(producedDataType),
                    Collections.emptyList(),
                    null,
                    VectorizedColumnBatch.DEFAULT_SIZE,
                    formatOptions.get(UTC_TIMEZONE),
                    true);
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }

        @Override
        public TableStats reportStatistics(List<Path> files, DataType producedDataType) {
            try {
                Configuration hadoopConfig = getParquetConfiguration(formatOptions);
                Map<String, Statistics<?>> columnStatisticsMap = new HashMap<>();
                RowType producedRowType = (RowType) producedDataType.getLogicalType();
                long rowCount = 0;
                for (Path file : files) {
                    rowCount += updateStatistics(hadoopConfig, file, columnStatisticsMap);
                }
                Map<String, ColumnStats> columnStatsMap =
                        convertToColumnStats(columnStatisticsMap, producedRowType);
                return new TableStats(rowCount, columnStatsMap);
            } catch (Exception e) {
                return TableStats.UNKNOWN;
            }
        }

        private Map<String, ColumnStats> convertToColumnStats(
                Map<String, Statistics<?>> columnStatisticsMap, RowType producedRowType) {
            Map<String, ColumnStats> columnStatMap = new HashMap<>();
            for (String column : producedRowType.getFieldNames()) {
                Statistics<?> statistics = columnStatisticsMap.get(column);
                if (statistics == null) {
                    continue;
                }
                ColumnStats columnStats =
                        convertToColumnStats(
                                producedRowType.getTypeAt(producedRowType.getFieldIndex(column)),
                                statistics);
                columnStatMap.put(column, columnStats);
            }
            return columnStatMap;
        }

        private ColumnStats convertToColumnStats(
                LogicalType logicalType, Statistics<?> statistics) {
            ColumnStats.Builder builder =
                    new ColumnStats.Builder().setNullCount(statistics.getNumNulls());

            // For complex types: ROW, ARRAY, MAP. The returned statistics have wrong null count
            // value, so now complex types stats return null.
            switch (logicalType.getTypeRoot()) {
                case BOOLEAN:
                case BINARY:
                case VARBINARY:
                    break;
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                    if (statistics instanceof IntStatistics) {
                        builder.setMin(((IntStatistics) statistics).getMin())
                                .setMax(((IntStatistics) statistics).getMax());
                    } else if (statistics instanceof LongStatistics) {
                        builder.setMin(((LongStatistics) statistics).getMin())
                                .setMax(((LongStatistics) statistics).getMax());
                    } else {
                        return null;
                    }
                    break;
                case DOUBLE:
                    if (statistics instanceof DoubleStatistics) {
                        builder.setMin(((DoubleStatistics) statistics).getMin())
                                .setMax(((DoubleStatistics) statistics).getMax());
                        break;
                    } else {
                        return null;
                    }
                case FLOAT:
                    if (statistics instanceof FloatStatistics) {
                        builder.setMin(((FloatStatistics) statistics).getMin())
                                .setMax(((FloatStatistics) statistics).getMax());
                        break;
                    } else {
                        return null;
                    }
                case DATE:
                    if (statistics instanceof IntStatistics) {
                        Date min =
                                Date.valueOf(
                                        DateTimeUtils.formatDate(
                                                ((IntStatistics) statistics).getMin()));
                        Date max =
                                Date.valueOf(
                                        DateTimeUtils.formatDate(
                                                ((IntStatistics) statistics).getMax()));
                        builder.setMin(min).setMax(max);
                        break;
                    } else {
                        return null;
                    }
                case TIME_WITHOUT_TIME_ZONE:
                    if (statistics instanceof IntStatistics) {
                        Time min =
                                Time.valueOf(
                                        DateTimeUtils.toLocalTime(
                                                ((IntStatistics) statistics).getMin()));
                        Time max =
                                Time.valueOf(
                                        DateTimeUtils.toLocalTime(
                                                ((IntStatistics) statistics).getMax()));
                        builder.setMin(min).setMax(max);
                        break;
                    } else {
                        return null;
                    }
                case CHAR:
                case VARCHAR:
                    if (statistics instanceof BinaryStatistics) {
                        Binary min = ((BinaryStatistics) statistics).genericGetMin();
                        Binary max = ((BinaryStatistics) statistics).genericGetMax();
                        if (min != null) {
                            builder.setMin(min.toStringUsingUTF8());
                        } else {
                            builder.setMin(null);
                        }
                        if (max != null) {
                            builder.setMax(max.toStringUsingUTF8());
                        } else {
                            builder.setMax(null);
                        }
                        break;
                    } else {
                        return null;
                    }
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case TIMESTAMP_WITH_TIME_ZONE:
                    if (statistics instanceof LongStatistics) {
                        builder.setMin(new Timestamp(((LongStatistics) statistics).getMin()))
                                .setMax(new Timestamp(((LongStatistics) statistics).getMax()));
                    } else if (statistics instanceof BinaryStatistics) {
                        Binary min = ((BinaryStatistics) statistics).genericGetMin();
                        Binary max = ((BinaryStatistics) statistics).genericGetMax();
                        if (min != null) {
                            builder.setMin(binaryToTimestamp(min, formatOptions.get(UTC_TIMEZONE)));
                        } else {
                            builder.setMin(null);
                        }
                        if (max != null) {
                            builder.setMax(binaryToTimestamp(max, formatOptions.get(UTC_TIMEZONE)));
                        } else {
                            builder.setMax(null);
                        }
                    } else {
                        return null;
                    }
                    break;
                case DECIMAL:
                    if (statistics instanceof IntStatistics) {
                        builder.setMin(BigDecimal.valueOf(((IntStatistics) statistics).getMin()))
                                .setMax(BigDecimal.valueOf(((IntStatistics) statistics).getMax()));
                    } else if (statistics instanceof LongStatistics) {
                        builder.setMin(BigDecimal.valueOf(((LongStatistics) statistics).getMin()))
                                .setMax(BigDecimal.valueOf(((LongStatistics) statistics).getMax()));
                    } else if (statistics instanceof BinaryStatistics) {
                        Binary min = ((BinaryStatistics) statistics).genericGetMin();
                        Binary max = ((BinaryStatistics) statistics).genericGetMax();
                        if (min != null) {
                            builder.setMin(
                                    binaryToDecimal(min, ((DecimalType) logicalType).getScale()));
                        } else {
                            builder.setMin(null);
                        }
                        if (max != null) {
                            builder.setMax(
                                    binaryToDecimal(max, ((DecimalType) logicalType).getScale()));
                        } else {
                            builder.setMax(null);
                        }
                    } else {
                        return null;
                    }
                    break;
                default:
                    return null;
            }
            return builder.build();
        }

        private long updateStatistics(
                Configuration hadoopConfig,
                Path file,
                Map<String, Statistics<?>> columnStatisticsMap)
                throws IOException {
            org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toUri());
            ParquetMetadata metadata = ParquetFileReader.readFooter(hadoopConfig, hadoopPath);
            MessageType schema = metadata.getFileMetaData().getSchema();
            List<String> columns =
                    schema.asGroupType().getFields().stream()
                            .map(Type::getName)
                            .collect(Collectors.toList());
            List<BlockMetaData> blocks = metadata.getBlocks();
            long rowCount = 0;
            for (BlockMetaData block : blocks) {
                rowCount += block.getRowCount();
                for (int i = 0; i < columns.size(); ++i) {
                    updateStatistics(
                            block.getColumns().get(i).getStatistics(),
                            columns.get(i),
                            columnStatisticsMap);
                }
            }
            return rowCount;
        }

        private void updateStatistics(
                Statistics<?> statistics,
                String column,
                Map<String, Statistics<?>> columnStatisticsMap) {
            Statistics<?> previousStatistics = columnStatisticsMap.get(column);
            if (previousStatistics == null) {
                columnStatisticsMap.put(column, statistics);
            } else {
                previousStatistics.mergeStatistics(statistics);
            }
        }

        private static BigDecimal binaryToDecimal(Binary decimal, int scale) {
            BigInteger bigInteger = new BigInteger(decimal.getBytesUnsafe());
            return new BigDecimal(bigInteger, scale);
        }

        private static Timestamp binaryToTimestamp(Binary timestamp, boolean utcTimestamp) {
            Preconditions.checkArgument(timestamp.length() == 12, "Must be 12 bytes");
            ByteBuffer buf = timestamp.toByteBuffer();
            buf.order(ByteOrder.LITTLE_ENDIAN);
            long timeOfDayNanos = buf.getLong();
            int julianDay = buf.getInt();

            TimestampData timestampData =
                    TimestampColumnReader.int96ToTimestamp(utcTimestamp, timeOfDayNanos, julianDay);
            return timestampData.toTimestamp();
        }
    }
}
