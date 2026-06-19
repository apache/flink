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

package org.apache.flink.orc.util;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.DateColumnStatistics;
import org.apache.orc.DecimalColumnStatistics;
import org.apache.orc.DoubleColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.TimestampColumnStatistics;
import org.apache.orc.TypeDescription;
import org.apache.orc.impl.ColumnStatisticsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Utils for Orc format statistics report. */
public class OrcFormatStatisticsReportUtil {

    private static final Logger LOG = LoggerFactory.getLogger(OrcFormatStatisticsReportUtil.class);

    public static TableStats getTableStatistics(
            List<Path> files, DataType producedDataType, Configuration hadoopConfig) {
        return getTableStatistics(
                files, producedDataType, hadoopConfig, Runtime.getRuntime().availableProcessors());
    }

    public static TableStats getTableStatistics(
            List<Path> files,
            DataType producedDataType,
            Configuration hadoopConfig,
            int statisticsThreadNum) {
        ExecutorService executorService = null;
        try {
            long rowCount = 0;
            Map<String, ColumnStatistics> columnStatisticsMap = new HashMap<>();
            RowType producedRowType = (RowType) producedDataType.getLogicalType();
            executorService =
                    Executors.newFixedThreadPool(
                            statisticsThreadNum,
                            new ExecutorThreadFactory("orc-get-table-statistic-worker"));
            List<Future<FileOrcStatistics>> fileRowCountFutures = new ArrayList<>();
            for (Path file : files) {
                fileRowCountFutures.add(
                        executorService.submit(new OrcFileRowCountCalculator(hadoopConfig, file)));
            }
            for (Future<FileOrcStatistics> fileCountFuture : fileRowCountFutures) {
                FileOrcStatistics fileOrcStatistics = fileCountFuture.get();
                rowCount += fileOrcStatistics.getRowCount();
                for (String column : producedRowType.getFieldNames()) {
                    int fieldIdx = fileOrcStatistics.getFieldNames().indexOf(column);
                    if (fieldIdx >= 0) {
                        int colId = fileOrcStatistics.getColumnTypes().get(fieldIdx).getId();
                        ColumnStatistics statistic = fileOrcStatistics.getStatistics()[colId];
                        updateStatistics(statistic, column, columnStatisticsMap);
                    }
                }
            }
            Map<String, ColumnStats> columnStatsMap =
                    convertToColumnStats(rowCount, columnStatisticsMap, producedRowType);

            return new TableStats(rowCount, columnStatsMap);
        } catch (Exception e) {
            LOG.warn("Reporting statistics failed for Orc format: {}", e.getMessage());
            return TableStats.UNKNOWN;
        } finally {
            if (executorService != null) {
                executorService.shutdownNow();
            }
        }
    }

    private static void updateStatistics(
            ColumnStatistics statistic,
            String column,
            Map<String, ColumnStatistics> columnStatisticsMap) {
        ColumnStatistics previousStatistics = columnStatisticsMap.get(column);
        if (previousStatistics == null) {
            columnStatisticsMap.put(column, statistic);
        } else {
            if (previousStatistics instanceof ColumnStatisticsImpl) {
                ((ColumnStatisticsImpl) previousStatistics).merge((ColumnStatisticsImpl) statistic);
            }
        }
    }

    private static Map<String, ColumnStats> convertToColumnStats(
            long totalRowCount,
            Map<String, ColumnStatistics> columnStatisticsMap,
            RowType logicalType) {
        Map<String, ColumnStats> columnStatsMap = new HashMap<>();
        for (String column : logicalType.getFieldNames()) {
            ColumnStatistics columnStatistics = columnStatisticsMap.get(column);
            if (columnStatistics == null) {
                continue;
            }
            ColumnStats columnStats =
                    convertToColumnStats(
                            totalRowCount,
                            logicalType.getTypeAt(logicalType.getFieldIndex(column)),
                            columnStatistics);
            columnStatsMap.put(column, columnStats);
        }

        return columnStatsMap;
    }

    private static ColumnStats convertToColumnStats(
            long totalRowCount, LogicalType logicalType, ColumnStatistics columnStatistics) {
        ColumnStats.Builder builder =
                new ColumnStats.Builder().setNdv(null).setAvgLen(null).setMaxLen(null);
        if (!columnStatistics.hasNull()) {
            builder.setNullCount(0L);
        } else {
            builder.setNullCount(totalRowCount - columnStatistics.getNumberOfValues());
        }

        // For complex types: ROW, ARRAY, MAP. The returned statistics have wrong null count
        // value, so now complex types stats return null.
        switch (logicalType.getTypeRoot()) {
            case BOOLEAN:
                break;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                if (columnStatistics instanceof IntegerColumnStatistics) {
                    builder.setMax(((IntegerColumnStatistics) columnStatistics).getMaximum())
                            .setMin(((IntegerColumnStatistics) columnStatistics).getMinimum());
                    break;
                } else {
                    return null;
                }
            case FLOAT:
            case DOUBLE:
                if (columnStatistics instanceof DoubleColumnStatistics) {
                    builder.setMax(((DoubleColumnStatistics) columnStatistics).getMaximum())
                            .setMin(((DoubleColumnStatistics) columnStatistics).getMinimum());
                    break;
                } else {
                    return null;
                }
            case CHAR:
            case VARCHAR:
                if (columnStatistics instanceof StringColumnStatistics) {
                    builder.setMax(((StringColumnStatistics) columnStatistics).getMaximum())
                            .setMin(((StringColumnStatistics) columnStatistics).getMinimum());
                    break;
                } else {
                    return null;
                }
            case DATE:
                if (columnStatistics instanceof DateColumnStatistics) {
                    Date maximum = (Date) ((DateColumnStatistics) columnStatistics).getMaximum();
                    Date minimum = (Date) ((DateColumnStatistics) columnStatistics).getMinimum();
                    builder.setMax(maximum).setMin(minimum);
                    break;
                } else {
                    return null;
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
                if (columnStatistics instanceof TimestampColumnStatistics) {
                    builder.setMax(((TimestampColumnStatistics) columnStatistics).getMaximum())
                            .setMin(((TimestampColumnStatistics) columnStatistics).getMinimum());
                    break;
                } else {
                    return null;
                }
            case DECIMAL:
                if (columnStatistics instanceof DecimalColumnStatistics) {
                    builder.setMax(
                                    ((DecimalColumnStatistics) columnStatistics)
                                            .getMaximum()
                                            .bigDecimalValue())
                            .setMin(
                                    ((DecimalColumnStatistics) columnStatistics)
                                            .getMinimum()
                                            .bigDecimalValue());
                    break;
                } else {
                    return null;
                }
            default:
                return null;
        }
        return builder.build();
    }

    private static class FileOrcStatistics {
        private final Long rowCount;

        private final List<String> fieldNames;

        private final ColumnStatistics[] statistics;

        private final List<TypeDescription> columnTypes;

        public FileOrcStatistics(
                Long rowCount,
                List<String> fieldNames,
                ColumnStatistics[] statistics,
                List<TypeDescription> columnTypes) {
            this.rowCount = rowCount;
            this.fieldNames = fieldNames;
            this.statistics = statistics;
            this.columnTypes = columnTypes;
        }

        public Long getRowCount() {
            return rowCount;
        }

        public List<String> getFieldNames() {
            return fieldNames;
        }

        public ColumnStatistics[] getStatistics() {
            return statistics;
        }

        public List<TypeDescription> getColumnTypes() {
            return columnTypes;
        }
    }

    private static class OrcFileRowCountCalculator implements Callable<FileOrcStatistics> {

        private final Configuration hadoopConf;
        private final Path file;

        public OrcFileRowCountCalculator(Configuration hadoopConf, Path file) {
            this.hadoopConf = hadoopConf;
            this.file = file;
        }

        @Override
        public FileOrcStatistics call() throws IOException {
            org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(file.toUri());
            Reader reader =
                    OrcFile.createReader(
                            path,
                            OrcFile.readerOptions(hadoopConf)
                                    .maxLength(OrcConf.MAX_FILE_LENGTH.getLong(hadoopConf)));
            ColumnStatistics[] statistics = reader.getStatistics();
            TypeDescription schema = reader.getSchema();
            List<String> fieldNames = schema.getFieldNames();
            List<TypeDescription> columnTypes = schema.getChildren();

            return new FileOrcStatistics(
                    reader.getNumberOfRows(), fieldNames, statistics, columnTypes);
        }
    }
}
