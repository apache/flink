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

package org.apache.flink.formats.parquet.utils;

import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/** Utils for Parquet format statistics report. */
public class ParquetFormatStatisticsReportUtil {

    private static final Logger LOG =
            LoggerFactory.getLogger(ParquetFormatStatisticsReportUtil.class);

    public static TableStats getTableStatistics(
            List<Path> files,
            DataType producedDataType,
            Configuration hadoopConfig,
            boolean isUtcTimestamp) {
        return getTableStatistics(
                files,
                producedDataType,
                hadoopConfig,
                isUtcTimestamp,
                Runtime.getRuntime().availableProcessors());
    }

    public static TableStats getTableStatistics(
            List<Path> files,
            DataType producedDataType,
            Configuration hadoopConfig,
            boolean isUtcTimestamp,
            int statisticsThreadNum) {
        ExecutorService executorService = null;
        try {
            Map<String, Statistics<?>> columnStatisticsMap = new HashMap<>();
            RowType producedRowType = (RowType) producedDataType.getLogicalType();
            executorService =
                    Executors.newFixedThreadPool(
                            statisticsThreadNum,
                            new ExecutorThreadFactory("parquet-get-table-statistic-worker"));
            long rowCount = 0;
            List<Future<FileParquetStatistics>> fileRowCountFutures = new ArrayList<>();
            for (Path file : files) {
                fileRowCountFutures.add(
                        executorService.submit(
                                new ParquetFileRowCountCalculator(
                                        hadoopConfig, file, columnStatisticsMap)));
            }
            for (Future<FileParquetStatistics> fileCountFuture : fileRowCountFutures) {
                FileParquetStatistics fileStatistics = fileCountFuture.get();
                List<String> columns = fileStatistics.getColumns();
                List<BlockMetaData> blocks = fileStatistics.blocks;
                for (BlockMetaData block : blocks) {
                    rowCount += block.getRowCount();
                    for (int i = 0; i < columns.size(); ++i) {
                        updateStatistics(
                                block.getColumns().get(i).getStatistics(),
                                columns.get(i),
                                columnStatisticsMap);
                    }
                }
            }
            Map<String, ColumnStats> columnStatsMap =
                    convertToColumnStats(columnStatisticsMap, producedRowType, isUtcTimestamp);
            return new TableStats(rowCount, columnStatsMap);
        } catch (Exception e) {
            LOG.warn("Reporting statistics failed for Parquet format", e);
            return TableStats.UNKNOWN;
        } finally {
            if (executorService != null) {
                executorService.shutdownNow();
            }
        }
    }

    private static void updateStatistics(
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

    private static Map<String, ColumnStats> convertToColumnStats(
            Map<String, Statistics<?>> columnStatisticsMap,
            RowType producedRowType,
            boolean isUtcTimestamp) {
        Map<String, ColumnStats> columnStatMap = new HashMap<>();
        for (String column : producedRowType.getFieldNames()) {
            Statistics<?> statistics = columnStatisticsMap.get(column);
            if (statistics == null) {
                continue;
            }
            ColumnStats columnStats =
                    convertToColumnStats(
                            producedRowType.getTypeAt(producedRowType.getFieldIndex(column)),
                            statistics,
                            isUtcTimestamp);
            columnStatMap.put(column, columnStats);
        }
        return columnStatMap;
    }

    private static ColumnStats convertToColumnStats(
            LogicalType logicalType, Statistics<?> statistics, boolean isUtcTimestamp) {
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
                        builder.setMin(binaryToTimestamp(min, isUtcTimestamp));
                    } else {
                        builder.setMin(null);
                    }
                    if (max != null) {
                        builder.setMax(binaryToTimestamp(max, isUtcTimestamp));
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

    private static class FileParquetStatistics {

        private final List<String> columns;

        private final List<BlockMetaData> blocks;

        public FileParquetStatistics(List<String> columns, List<BlockMetaData> blocks) {
            this.columns = columns;
            this.blocks = blocks;
        }

        public List<String> getColumns() {
            return columns;
        }

        public List<BlockMetaData> getBlocks() {
            return blocks;
        }
    }

    private static class ParquetFileRowCountCalculator implements Callable<FileParquetStatistics> {
        private final Configuration hadoopConfig;
        private final Path file;

        public ParquetFileRowCountCalculator(
                Configuration hadoopConfig,
                Path file,
                Map<String, Statistics<?>> columnStatisticsMap) {
            this.hadoopConfig = hadoopConfig;
            this.file = file;
        }

        @Override
        public FileParquetStatistics call() throws Exception {
            org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toUri());
            ParquetMetadata metadata = ParquetFileReader.readFooter(hadoopConfig, hadoopPath);
            MessageType schema = metadata.getFileMetaData().getSchema();
            List<String> columns =
                    schema.asGroupType().getFields().stream()
                            .map(Type::getName)
                            .collect(Collectors.toList());
            List<BlockMetaData> blocks = metadata.getBlocks();
            return new FileParquetStatistics(columns, blocks);
        }
    }
}
