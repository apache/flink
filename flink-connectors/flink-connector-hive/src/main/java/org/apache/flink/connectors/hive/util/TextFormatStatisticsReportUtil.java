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

package org.apache.flink.connectors.hive.util;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/** Utils for text format statistics report. */
public class TextFormatStatisticsReportUtil {
    private static final Logger LOG = LoggerFactory.getLogger(TextFormatStatisticsReportUtil.class);

    public static TableStats estimateTableStatistics(
            List<Path> files, DataType producedDataType, Configuration hadoopConfig) {
        try {
            long rowCount;
            RowType rowType = (RowType) producedDataType.getLogicalType();
            long totalFileSize = 0L;
            for (Path file : files) {
                totalFileSize += getTextFileSize(hadoopConfig, file);
            }
            rowCount = (long) Math.ceil(totalFileSize / estimateRowSize(rowType));
            return new TableStats(rowCount);
        } catch (Exception e) {
            LOG.warn("Estimating statistics failed for text format: {}", e.getMessage());
            return TableStats.UNKNOWN;
        }
    }

    private static double estimateRowSize(RowType rowType) {
        double rowSize = 0.0;
        for (int index = 0; index < rowType.getFieldCount(); ++index) {
            LogicalType logicalType = rowType.getTypeAt(index);
            rowSize += getAverageTypeValueSize(logicalType);
        }
        return rowSize;
    }

    /**
     * Estimation rules based on Hive field types, draws inspiration from
     * FlinkRelMdSize#averageTypeValueSize.
     */
    private static double getAverageTypeValueSize(LogicalType logicalType) {
        LogicalTypeRoot typeRoot = logicalType.getTypeRoot();
        switch (typeRoot) {
            case CHAR:
            case TINYINT:
            case BOOLEAN:
                return 1;
            case VARCHAR:
            case DATE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case TIME_WITHOUT_TIME_ZONE:
            case DECIMAL:
                return 12;
            case SMALLINT:
                return 2;
            case INTEGER:
            case FLOAT:
            case INTERVAL_DAY_TIME:
                return 4;
            case BIGINT:
            case DOUBLE:
            case INTERVAL_YEAR_MONTH:
                return 8;
            case BINARY:
            case VARBINARY:
                return 16;
            case ARRAY:
                return getAverageTypeValueSize(((ArrayType) logicalType).getElementType()) * 16;
            case MAP:
                return (getAverageTypeValueSize(((MapType) logicalType).getKeyType())
                                + getAverageTypeValueSize(((MapType) logicalType).getValueType()))
                        * 16;
            case ROW:
            case DISTINCT_TYPE:
            case STRUCTURED_TYPE:
                return logicalType.getChildren().stream()
                        .map(TextFormatStatisticsReportUtil::getAverageTypeValueSize)
                        .reduce(0.0, Double::sum);
            case MULTISET:
                return (getAverageTypeValueSize(((MultisetType) logicalType).getElementType())
                                + getAverageTypeValueSize(new IntType()))
                        * 16;
            default:
                // For unknown data types, we use a smaller data size for estimation.
                return 8;
        }
    }

    private static long getTextFileSize(Configuration hadoopConfig, Path file) throws IOException {
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(file.toUri());
        return hadoopPath.getFileSystem(hadoopConfig).getContentSummary(hadoopPath).getLength();
    }
}
