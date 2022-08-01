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

package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBinary;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBoolean;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.catalog.stats.Date;
import org.apache.flink.table.operations.ddl.AnalyzeTableOperation;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** ANALYZE TABLE statement Util. */
@Internal
public class AnalyzeTableUtil {

    private AnalyzeTableUtil() {}

    public static TableResultInternal analyzeTable(
            TableEnvironmentImpl tableEnv, AnalyzeTableOperation operation)
            throws TableNotPartitionedException, TableNotExistException, PartitionNotExistException,
                    TablePartitionedException {
        CatalogManager catalogManager = tableEnv.getCatalogManager();
        CatalogTable table =
                catalogManager.getTable(operation.getTableIdentifier()).get().getTable();
        ResolvedSchema schema =
                table.getUnresolvedSchema().resolve(catalogManager.getSchemaResolver());
        List<Column> columns =
                operation.getColumns().stream()
                        .map(c -> schema.getColumn(c).get())
                        .collect(Collectors.toList());
        Catalog catalog =
                catalogManager.getCatalog(operation.getTableIdentifier().getCatalogName()).get();
        ObjectPath objectPath = operation.getTableIdentifier().toObjectPath();

        if (table.isPartitioned()) {
            List<CatalogPartitionSpec> targetPartitions =
                    operation.getPartitionSpecs().orElse(catalog.listPartitions(objectPath));
            for (CatalogPartitionSpec partitionSpec : targetPartitions) {
                String statSql =
                        generateAnalyzeSql(
                                catalogManager,
                                operation.getTableIdentifier(),
                                partitionSpec,
                                columns);
                TableResult tableResult = tableEnv.executeSql(statSql);
                List<Row> result = CollectionUtil.iteratorToList(tableResult.collect());
                Preconditions.checkArgument(result.size() == 1);
                Row row = result.get(0);
                CatalogTableStatistics tableStat = convertToTableStatistics(row);
                catalog.alterPartitionStatistics(objectPath, partitionSpec, tableStat, false);
                if (!columns.isEmpty()) {
                    CatalogColumnStatistics columnStat = convertToColumnStatistics(row, columns);
                    catalog.alterPartitionColumnStatistics(
                            objectPath, partitionSpec, columnStat, false);
                }
            }
        } else {
            String statSql =
                    generateAnalyzeSql(
                            catalogManager, operation.getTableIdentifier(), null, columns);
            TableResult tableResult = tableEnv.executeSql(statSql);
            List<Row> result = CollectionUtil.iteratorToList(tableResult.collect());
            Preconditions.checkArgument(result.size() == 1);
            Row row = result.get(0);
            CatalogTableStatistics tableStat = convertToTableStatistics(row);
            catalog.alterTableStatistics(objectPath, tableStat, false);
            if (!columns.isEmpty()) {
                CatalogColumnStatistics columnStat = convertToColumnStatistics(row, columns);
                catalog.alterTableColumnStatistics(objectPath, columnStat, false);
            }
        }
        return TableResultImpl.TABLE_RESULT_OK;
    }

    private static String generateAnalyzeSql(
            CatalogManager catalogManager,
            ObjectIdentifier tableIdentifier,
            @Nullable CatalogPartitionSpec partitionSpec,
            List<Column> columns) {
        Optional<ContextResolvedTable> optionalCatalogTable =
                catalogManager.getTable(tableIdentifier);
        Preconditions.checkArgument(
                optionalCatalogTable.isPresent(), tableIdentifier + " does not exist");

        String partitionFilter;
        if (partitionSpec != null) {
            partitionFilter =
                    " WHERE "
                            + partitionSpec.getPartitionSpec().entrySet().stream()
                                    .map(e -> e.getKey() + "=" + e.getValue())
                                    .collect(Collectors.joining(" AND "));
        } else {
            partitionFilter = "";
        }

        final String columnStatsSelects;
        if (columns.isEmpty()) {
            columnStatsSelects = "";
        } else {
            columnStatsSelects = ", " + getColumnStatsSelects(columns);
        }

        return String.format(
                "SELECT COUNT(1) AS %s %s FROM %s %s",
                getRowCountColumn(), columnStatsSelects, tableIdentifier, partitionFilter);
    }

    private static String getColumnStatsSelects(List<Column> columns) {
        return columns.stream()
                .flatMap(
                        f -> {
                            String c = f.getName();
                            List<String> columnStatSelect = new ArrayList<>();
                            String computeNullCount =
                                    String.format(
                                            "(COUNT(1) - COUNT(`%s`)) AS %s",
                                            c, getNullCountColumn(c));
                            columnStatSelect.add(computeNullCount);

                            String computeNdv =
                                    String.format(
                                            "APPROX_COUNT_DISTINCT(`%s`) AS %s",
                                            c, getNdvColumn(c));

                            switch (f.getDataType().getLogicalType().getTypeRoot()) {
                                case BOOLEAN:
                                    columnStatSelect.add(
                                            String.format(
                                                    "COUNT(`%s`) FILTER (WHERE `%s` IS TRUE) AS %s",
                                                    c, c, getTrueCountColumn(c)));
                                    columnStatSelect.add(
                                            String.format(
                                                    "COUNT(`%s`) FILTER (WHERE `%s` IS FALSE) AS %s",
                                                    c, c, getFalseCountColumn(c)));
                                    break;
                                case TINYINT:
                                case SMALLINT:
                                case INTEGER:
                                case FLOAT:
                                case DATE:
                                case TIME_WITHOUT_TIME_ZONE:
                                case BIGINT:
                                case DOUBLE:
                                case DECIMAL:
                                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                                case TIMESTAMP_WITHOUT_TIME_ZONE:
                                    columnStatSelect.add(computeNdv);
                                    columnStatSelect.add(
                                            String.format("MAX(`%s`) AS %s", c, getMaxColumn(c)));
                                    columnStatSelect.add(
                                            String.format("MIN(`%s`) AS %s", c, getMinColumn(c)));
                                    break;
                                case CHAR:
                                case VARCHAR:
                                    columnStatSelect.add(computeNdv);
                                    columnStatSelect.add(
                                            String.format(
                                                    "AVG(CAST(CHAR_LENGTH(`%s`) AS DOUBLE)) AS %s",
                                                    c, getAvgLenColumn(c)));
                                    columnStatSelect.add(
                                            String.format(
                                                    "MAX(CAST(CHAR_LENGTH(`%s`) AS BIGINT)) AS %s",
                                                    c, getMaxLenColumn(c)));
                                    break;
                                default:
                                    break;
                            }
                            return columnStatSelect.stream();
                        })
                .collect(Collectors.joining(", "));
    }

    private static String getRowCountColumn() {
        return "rowCount";
    }

    private static String getNullCountColumn(String column) {
        return String.format("%s_nullCount", column);
    }

    private static String getNdvColumn(String column) {
        return String.format("%s_ndv", column);
    }

    private static String getTrueCountColumn(String column) {
        return String.format("%s_trueCount", column);
    }

    private static String getFalseCountColumn(String column) {
        return String.format("%s_falseCount", column);
    }

    private static String getMaxColumn(String column) {
        return String.format("%s_max", column);
    }

    private static String getMinColumn(String column) {
        return String.format("%s_min", column);
    }

    private static String getAvgLenColumn(String column) {
        return String.format("%s_avgLen", column);
    }

    private static String getMaxLenColumn(String column) {
        return String.format("%s_maxLen", column);
    }

    private static CatalogTableStatistics convertToTableStatistics(Row row) {
        Long rowCount = row.getFieldAs(getRowCountColumn());
        return new CatalogTableStatistics(rowCount, -1, -1, -1);
    }

    private static CatalogColumnStatistics convertToColumnStatistics(
            Row row, List<Column> columns) {
        Preconditions.checkArgument(!columns.isEmpty());
        Map<String, CatalogColumnStatisticsDataBase> columnStatMap = new HashMap<>();
        for (Column column : columns) {
            CatalogColumnStatisticsDataBase columnStat = convertToColumnStatisticsData(row, column);
            if (columnStat != null) {
                columnStatMap.put(column.getName(), columnStat);
            }
        }
        return new CatalogColumnStatistics(columnStatMap);
    }

    private static CatalogColumnStatisticsDataBase convertToColumnStatisticsData(
            Row row, Column column) {
        String c = column.getName();
        Long nullCount = row.getFieldAs(getNullCountColumn(c));
        switch (column.getDataType().getLogicalType().getTypeRoot()) {
            case BOOLEAN:
                Long trueCount = row.getFieldAs(getTrueCountColumn(c));
                Long falseCount = row.getFieldAs(getFalseCountColumn(c));
                return new CatalogColumnStatisticsDataBoolean(trueCount, falseCount, nullCount);
            case TINYINT:
                Byte maxByte = row.getFieldAs(getMaxColumn(c));
                Byte minByte = row.getFieldAs(getMinColumn(c));
                Long ndvByte = row.getFieldAs(getNdvColumn(c));
                return new CatalogColumnStatisticsDataLong(
                        minByte != null ? minByte.longValue() : null,
                        maxByte != null ? maxByte.longValue() : null,
                        ndvByte,
                        nullCount);
            case SMALLINT:
                Short maxShort = row.getFieldAs(getMaxColumn(c));
                Short minShort = row.getFieldAs(getMinColumn(c));
                Long ndvShort = row.getFieldAs(getNdvColumn(c));
                return new CatalogColumnStatisticsDataLong(
                        minShort != null ? minShort.longValue() : null,
                        maxShort != null ? maxShort.longValue() : null,
                        ndvShort,
                        nullCount);
            case INTEGER:
                Integer maxInt = row.getFieldAs(getMaxColumn(c));
                Integer minInt = row.getFieldAs(getMinColumn(c));
                Long ndvInt = row.getFieldAs(getNdvColumn(c));
                return new CatalogColumnStatisticsDataLong(
                        minInt != null ? minInt.longValue() : null,
                        maxInt != null ? maxInt.longValue() : null,
                        ndvInt,
                        nullCount);
            case BIGINT:
                Long ndvLong = row.getFieldAs(getNdvColumn(c));
                Long maxLong = row.getFieldAs(getMaxColumn(c));
                Long minLong = row.getFieldAs(getMinColumn(c));
                return new CatalogColumnStatisticsDataLong(minLong, maxLong, ndvLong, nullCount);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                Long ndvTs = row.getFieldAs(getNdvColumn(c));
                LocalDateTime maxTs = row.getFieldAs(getMaxColumn(c));
                LocalDateTime minTs = row.getFieldAs(getMinColumn(c));

                return new CatalogColumnStatisticsDataLong(
                        minTs != null ? minTs.toEpochSecond(ZoneOffset.UTC) : null,
                        maxTs != null ? maxTs.toEpochSecond(ZoneOffset.UTC) : null,
                        ndvTs,
                        nullCount);
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                Long ndvTsLtz = row.getFieldAs(getNdvColumn(c));
                Instant maxTsLtz = row.getFieldAs(getMaxColumn(c));
                Instant minTsLtz = row.getFieldAs(getMinColumn(c));
                return new CatalogColumnStatisticsDataLong(
                        minTsLtz != null ? minTsLtz.toEpochMilli() : null,
                        maxTsLtz != null ? maxTsLtz.toEpochMilli() : null,
                        ndvTsLtz,
                        nullCount);
            case FLOAT:
                Long ndvFloat = row.getFieldAs(getNdvColumn(c));
                Float maxFloat = row.getFieldAs(getMaxColumn(c));
                Float minFloat = row.getFieldAs(getMinColumn(c));
                return new CatalogColumnStatisticsDataDouble(
                        minFloat != null ? minFloat.doubleValue() : null,
                        maxFloat != null ? maxFloat.doubleValue() : null,
                        ndvFloat,
                        nullCount);
            case DOUBLE:
                Long ndvDouble = row.getFieldAs(getNdvColumn(c));
                Double maxDouble = row.getFieldAs(getMaxColumn(c));
                Double minDouble = row.getFieldAs(getMinColumn(c));
                return new CatalogColumnStatisticsDataDouble(
                        minDouble, maxDouble, ndvDouble, nullCount);
            case DECIMAL:
                Long ndvDecimal = row.getFieldAs(getNdvColumn(c));
                BigDecimal maxDecimal = row.getFieldAs(getMaxColumn(c));
                BigDecimal minDecimal = row.getFieldAs(getMinColumn(c));
                return new CatalogColumnStatisticsDataDouble(
                        minDecimal != null ? minDecimal.doubleValue() : null,
                        maxDecimal != null ? maxDecimal.doubleValue() : null,
                        ndvDecimal,
                        nullCount);
            case DATE:
                Long ndvDate = row.getFieldAs(getNdvColumn(c));
                LocalDate maxDate = row.getFieldAs(getMaxColumn(c));
                LocalDate minDate = row.getFieldAs(getMinColumn(c));
                return new CatalogColumnStatisticsDataDate(
                        minDate != null ? new Date(minDate.toEpochDay()) : null,
                        maxDate != null ? new Date(maxDate.toEpochDay()) : null,
                        ndvDate,
                        nullCount);
            case TIME_WITHOUT_TIME_ZONE:
                Long ndvTime = row.getFieldAs(getNdvColumn(c));
                LocalTime maxTime = row.getFieldAs(getMaxColumn(c));
                LocalTime minTime = row.getFieldAs(getMinColumn(c));
                return new CatalogColumnStatisticsDataLong(
                        minTime != null ? minTime.toNanoOfDay() : null,
                        maxTime != null ? maxTime.toNanoOfDay() : null,
                        ndvTime,
                        nullCount);
            case CHAR:
            case VARCHAR:
                Long ndvString = row.getFieldAs(getNdvColumn(c));
                Double avgLen = row.getFieldAs(getAvgLenColumn(c));
                Long maxLen = row.getFieldAs(getMaxLenColumn(c));
                return new CatalogColumnStatisticsDataString(maxLen, avgLen, ndvString, nullCount);
            case BINARY:
            case VARBINARY:
                Long ndvBinary = row.getFieldAs(getNdvColumn(c));
                return new CatalogColumnStatisticsDataBinary(null, null, ndvBinary);
            default:
                return null;
        }
    }
}
