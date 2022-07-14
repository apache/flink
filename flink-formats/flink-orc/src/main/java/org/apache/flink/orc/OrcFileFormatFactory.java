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

package org.apache.flink.orc;

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
import org.apache.flink.orc.shim.OrcShim;
import org.apache.flink.orc.vector.RowDataVectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.format.FileBasedStatisticsReportableInputFormat;
import org.apache.flink.table.connector.format.ProjectableDecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

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

import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** Orc format factory for file system. */
public class OrcFileFormatFactory implements BulkReaderFormatFactory, BulkWriterFormatFactory {

    public static final String IDENTIFIER = "orc";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        // support "orc.*"
        return new HashSet<>();
    }

    private static Properties getOrcProperties(ReadableConfig options) {
        Properties orcProperties = new Properties();
        Properties properties = new Properties();
        ((org.apache.flink.configuration.Configuration) options).addAllToProperties(properties);
        properties.forEach((k, v) -> orcProperties.put(IDENTIFIER + "." + k, v));
        return orcProperties;
    }

    @Override
    public BulkDecodingFormat<RowData> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        return new OrcBulkDecodingFormat(formatOptions);
    }

    @Override
    public EncodingFormat<BulkWriter.Factory<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        return new EncodingFormat<BulkWriter.Factory<RowData>>() {
            @Override
            public BulkWriter.Factory<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context sinkContext, DataType consumedDataType) {
                RowType formatRowType = (RowType) consumedDataType.getLogicalType();
                LogicalType[] orcTypes = formatRowType.getChildren().toArray(new LogicalType[0]);

                TypeDescription typeDescription =
                        OrcSplitReaderUtil.logicalTypeToOrcType(formatRowType);

                return new OrcBulkWriterFactory<>(
                        new RowDataVectorizer(typeDescription.toString(), orcTypes),
                        getOrcProperties(formatOptions),
                        new Configuration());
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    /** OrcBulkDecodingFormat which implements {@link FileBasedStatisticsReportableInputFormat}. */
    @VisibleForTesting
    public static class OrcBulkDecodingFormat
            implements BulkDecodingFormat<RowData>,
                    ProjectableDecodingFormat<BulkFormat<RowData, FileSourceSplit>>,
                    FileBasedStatisticsReportableInputFormat {

        private final ReadableConfig formatOptions;
        private List<ResolvedExpression> filters;

        public OrcBulkDecodingFormat(ReadableConfig formatOptions) {
            this.formatOptions = formatOptions;
        }

        @Override
        public BulkFormat<RowData, FileSourceSplit> createRuntimeDecoder(
                DynamicTableSource.Context sourceContext,
                DataType producedDataType,
                int[][] projections) {
            List<OrcFilters.Predicate> orcPredicates = new ArrayList<>();

            if (filters != null) {
                for (Expression pred : filters) {
                    OrcFilters.Predicate orcPred = OrcFilters.toOrcPredicate(pred);
                    if (orcPred != null) {
                        orcPredicates.add(orcPred);
                    }
                }
            }

            Properties properties = getOrcProperties(formatOptions);
            Configuration conf = new Configuration();
            properties.forEach((k, v) -> conf.set(k.toString(), v.toString()));

            return OrcColumnarRowInputFormat.createPartitionedFormat(
                    OrcShim.defaultShim(),
                    conf,
                    (RowType) producedDataType.getLogicalType(),
                    Collections.emptyList(),
                    null,
                    Projection.of(projections).toTopLevelIndexes(),
                    orcPredicates,
                    VectorizedColumnBatch.DEFAULT_SIZE,
                    sourceContext::createTypeInformation);
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }

        @Override
        public void applyFilters(List<ResolvedExpression> filters) {
            this.filters = filters;
        }

        @Override
        public TableStats reportStatistics(List<Path> files, DataType producedDataType) {
            try {
                Properties properties = getOrcProperties(formatOptions);
                Configuration hadoopConfig = new Configuration();
                properties.forEach((k, v) -> hadoopConfig.set(k.toString(), v.toString()));

                long rowCount = 0;
                Map<String, ColumnStatistics> columnStatisticsMap = new HashMap<>();
                RowType producedRowType = (RowType) producedDataType.getLogicalType();
                for (Path file : files) {
                    rowCount +=
                            updateStatistics(
                                    hadoopConfig, file, columnStatisticsMap, producedRowType);
                }

                Map<String, ColumnStats> columnStatsMap =
                        convertToColumnStats(rowCount, columnStatisticsMap, producedRowType);

                return new TableStats(rowCount, columnStatsMap);
            } catch (Exception e) {
                return TableStats.UNKNOWN;
            }
        }

        private long updateStatistics(
                Configuration hadoopConf,
                Path file,
                Map<String, ColumnStatistics> columnStatisticsMap,
                RowType producedRowType)
                throws IOException {
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
            for (String column : producedRowType.getFieldNames()) {
                int fieldIdx = fieldNames.indexOf(column);
                if (fieldIdx >= 0) {
                    int colId = columnTypes.get(fieldIdx).getId();
                    ColumnStatistics statistic = statistics[colId];
                    updateStatistics(statistic, column, columnStatisticsMap);
                }
            }

            return reader.getNumberOfRows();
        }

        private void updateStatistics(
                ColumnStatistics statistic,
                String column,
                Map<String, ColumnStatistics> columnStatisticsMap) {
            ColumnStatistics previousStatistics = columnStatisticsMap.get(column);
            if (previousStatistics == null) {
                columnStatisticsMap.put(column, statistic);
            } else {
                if (previousStatistics instanceof ColumnStatisticsImpl) {
                    ((ColumnStatisticsImpl) previousStatistics)
                            .merge((ColumnStatisticsImpl) statistic);
                }
            }
        }

        private Map<String, ColumnStats> convertToColumnStats(
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

        private ColumnStats convertToColumnStats(
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
                        Date maximum =
                                (Date) ((DateColumnStatistics) columnStatistics).getMaximum();
                        Date minimum =
                                (Date) ((DateColumnStatistics) columnStatistics).getMinimum();
                        builder.setMax(maximum).setMin(minimum);
                        break;
                    } else {
                        return null;
                    }
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_TIME_ZONE:
                    if (columnStatistics instanceof TimestampColumnStatistics) {
                        builder.setMax(((TimestampColumnStatistics) columnStatistics).getMaximum())
                                .setMin(
                                        ((TimestampColumnStatistics) columnStatistics)
                                                .getMinimum());
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
    }
}
