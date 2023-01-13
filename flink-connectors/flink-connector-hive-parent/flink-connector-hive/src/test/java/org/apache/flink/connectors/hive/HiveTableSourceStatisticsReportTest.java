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

package org.apache.flink.connectors.hive;

import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.utils.StatisticsReportTestBase;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.DateTimeUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for statistics functionality in {@link HiveTableSource}. */
public class HiveTableSourceStatisticsReportTest extends StatisticsReportTestBase {

    private static HiveCatalog hiveCatalog;
    private static final String catalogName = "hive";
    private static final String dbName = "db1";
    private static final String sourceTable = "sourceTable";

    @BeforeEach
    public void setup(@TempDir File file) throws Exception {
        super.setup(file);
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        hiveCatalog.open();

        // we disable auto gather statistic in this test to avoid skipping
        // method HiveTableSource#reportStatistics
        tEnv.getConfig().set(HiveOptions.TABLE_EXEC_HIVE_SINK_STATISTIC_AUTO_GATHER_ENABLE, false);
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tEnv.registerCatalog(catalogName, hiveCatalog);
        tEnv.useCatalog(catalogName);
        tEnv.executeSql(String.format("create database %s", dbName));

        tEnv.executeSql(
                String.format(
                        "create table %s.%s.%s ( %s )",
                        catalogName,
                        dbName,
                        sourceTable,
                        String.join(", ", ddlTypesMapToStringList(ddlTypesMap()))));

        DataType dataType =
                tEnv.from(String.format("%s.%s.%s", catalogName, dbName, sourceTable))
                        .getResolvedSchema()
                        .toPhysicalRowDataType();
        tEnv.fromValues(dataType, getData())
                .executeInsert(String.format("%s.%s.%s", catalogName, dbName, sourceTable))
                .await();
    }

    @AfterEach
    public void after() {
        super.after();
        if (null != hiveCatalog) {
            hiveCatalog.close();
        }
    }

    @Override
    protected String[] properties() {
        return new String[0];
    }

    @Test
    public void testMapRedCsvFormatHiveTableSourceStatisticsReport() {
        FlinkStatistic statistic =
                getStatisticsFromOptimizedPlan(
                        String.format("select * from %s.%s.%s", catalogName, dbName, sourceTable));
        assertThat(statistic.getTableStats()).isEqualTo(TableStats.UNKNOWN);
    }

    @Test
    public void testFlinkOrcFormatHiveTableSourceStatisticsReport() throws Exception {
        tEnv.executeSql(
                String.format(
                        "create table hive.db1.orcTable ( %s ) stored as orc",
                        String.join(", ", ddlTypesMapToStringList(ddlTypesMap()))));
        tEnv.executeSql(
                        String.format(
                                "insert into hive.db1.orcTable select * from %s.%s.%s",
                                catalogName, dbName, sourceTable))
                .await();

        // Hive to read Orc file
        FlinkStatistic statistic =
                getStatisticsFromOptimizedPlan(
                        "select * from hive.db1.orcTable where f_smallint > 100");
        assertHiveTableOrcFormatTableStatsEquals(statistic.getTableStats(), 3, 1L);
    }

    @Test
    public void testFlinkParquetFormatHiveTableSourceStatisticsReport() throws Exception {
        tEnv.executeSql(
                String.format(
                        "create table hive.db1.parquetTable ( %s ) stored as parquet",
                        String.join(", ", ddlTypesMapToStringList(ddlTypesMap()))));
        tEnv.executeSql(
                        String.format(
                                "insert into hive.db1.parquetTable select * from %s.%s.%s",
                                catalogName, dbName, sourceTable))
                .await();

        // Hive to read Parquet file
        FlinkStatistic statistic =
                getStatisticsFromOptimizedPlan(
                        "select * from hive.db1.parquetTable where f_smallint > 100");
        assertHiveTableParquetFormatTableStatsEquals(statistic.getTableStats(), 3, 1L);
    }

    @Test
    public void testMapRedOrcFormatHiveTableSourceStatisticsReport() throws Exception {
        // Use mapRed parquet format.
        tEnv.getConfig().set(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER, true);
        tEnv.executeSql(
                String.format(
                        "create table hive.db1.orcTable ( %s ) stored as orc",
                        String.join(", ", ddlTypesMapToStringList(ddlTypesMap()))));
        tEnv.executeSql(
                        String.format(
                                "insert into hive.db1.orcTable select * from %s.%s.%s",
                                catalogName, dbName, sourceTable))
                .await();

        // Hive to read Orc file
        FlinkStatistic statistic =
                getStatisticsFromOptimizedPlan(
                        "select * from hive.db1.orcTable where f_smallint > 100");
        assertHiveTableOrcFormatTableStatsEquals(statistic.getTableStats(), 3, 1L);
    }

    @Test
    public void testMapRedParquetFormatHiveTableSourceStatisticsReport() throws Exception {
        // Use mapRed parquet format.
        tEnv.getConfig().set(HiveOptions.TABLE_EXEC_HIVE_FALLBACK_MAPRED_READER, true);
        tEnv.executeSql(
                String.format(
                        "create table hive.db1.parquetTable ( %s ) stored as parquet",
                        String.join(", ", ddlTypesMapToStringList(ddlTypesMap()))));
        tEnv.executeSql(
                        String.format(
                                "insert into hive.db1.parquetTable select * from %s.%s.%s",
                                catalogName, dbName, sourceTable))
                .await();

        // Hive to read Parquet file.
        FlinkStatistic statistic =
                getStatisticsFromOptimizedPlan(
                        "select * from hive.db1.parquetTable where f_smallint > 100");
        assertHiveTableParquetFormatTableStatsEquals(statistic.getTableStats(), 3, 1L);
    }

    @Test
    public void testHiveTableSourceWithLimitPushDown() {
        FlinkStatistic statistic =
                getStatisticsFromOptimizedPlan(
                        String.format(
                                "select * from %s.%s.%s limit 1",
                                catalogName, dbName, sourceTable));
        assertThat(statistic.getTableStats()).isEqualTo(new TableStats(1));
    }

    @Test
    public void testHiveTableSourceWithoutData() {
        tEnv.executeSql(
                String.format(
                        "create table hive.db1.orcTable ( %s ) stored as orc",
                        String.join(", ", ddlTypesMapToStringList(ddlTypesMap()))));
        FlinkStatistic statistic =
                getStatisticsFromOptimizedPlan(
                        String.format("select * from %s.%s.%s", catalogName, dbName, "orcTable"));
        assertThat(statistic.getTableStats()).isEqualTo(new TableStats(0));
    }

    @Test
    public void testHiveTableSourceWithoutDataWithLimitPushDown() {
        tEnv.executeSql(
                String.format(
                        "create table hive.db1.orcTable ( %s ) stored as orc",
                        String.join(", ", ddlTypesMapToStringList(ddlTypesMap()))));
        FlinkStatistic statistic =
                getStatisticsFromOptimizedPlan(
                        String.format(
                                "select * from %s.%s.%s limit 1", catalogName, dbName, "orcTable"));
        assertThat(statistic.getTableStats()).isEqualTo(new TableStats(0));
    }

    @Override
    protected Map<String, String> ddlTypesMap() {
        // hive table ddl now don't support type: TIMESTAMP(3), TIMESTAMP(9), TIMESTAMP WITHOUT TIME
        // ZONE, TIMESTAMP WITH LOCAL TIME ZONE AND ROW. So we remove these types.
        Map<String, String> ddlTypesMap = super.ddlTypesMap();
        String timestampTypeName = ddlTypesMap.remove("timestamp(3)");
        ddlTypesMap.remove("timestamp(9)");
        ddlTypesMap.remove("timestamp without time zone");
        ddlTypesMap.remove("timestamp with local time zone");
        String binaryTypeName = ddlTypesMap.remove("binary(1)");
        ddlTypesMap.remove("varbinary(1)");
        ddlTypesMap.remove("time");
        String rowName = ddlTypesMap.remove("row<col1 string, col2 int>");
        ddlTypesMap.put("timestamp", timestampTypeName);
        ddlTypesMap.put("binary", binaryTypeName);
        ddlTypesMap.put("STRUCT<col1 : string, col2 : int>", rowName);

        return ddlTypesMap;
    }

    @Override
    protected Map<String, List<Object>> getDataMap() {
        // hive table ddl now don't support type: TIMESTAMP(3), TIMESTAMP(9), TIMESTAMP WITHOUT TIME
        // ZONE, TIMESTAMP WITH LOCAL TIME ZONE AND ROW. So we remove these types related data.
        Map<String, List<Object>> dataMap = super.getDataMap();
        List<Object> timestampData = dataMap.remove("timestamp(3)");
        dataMap.remove("timestamp(9)");
        dataMap.remove("timestamp without time zone");
        dataMap.remove("timestamp with local time zone");
        List<Object> binaryData = dataMap.remove("binary(1)");
        dataMap.remove("varbinary(1)");
        dataMap.remove("time");
        List<Object> rowData = dataMap.remove("row<col1 string, col2 int>");
        dataMap.put("timestamp", timestampData);
        dataMap.put("binary", binaryData);
        dataMap.put("row<col1 string, col2 int>", rowData);

        return dataMap;
    }

    private static void assertHiveTableOrcFormatTableStatsEquals(
            TableStats tableStats, int expectedRowCount, long nullCount) {
        Map<String, ColumnStats> expectedColumnStatsMap = new HashMap<>();
        expectedColumnStatsMap.put(
                "f_boolean", new ColumnStats.Builder().setNullCount(nullCount).build());
        expectedColumnStatsMap.put(
                "f_tinyint",
                new ColumnStats.Builder().setMax(3L).setMin(1L).setNullCount(0L).build());
        expectedColumnStatsMap.put(
                "f_smallint",
                new ColumnStats.Builder().setMax(128L).setMin(100L).setNullCount(0L).build());
        expectedColumnStatsMap.put(
                "f_int",
                new ColumnStats.Builder()
                        .setMax(45536L)
                        .setMin(31000L)
                        .setNullCount(nullCount)
                        .build());
        expectedColumnStatsMap.put(
                "f_bigint",
                new ColumnStats.Builder()
                        .setMax(1238123899121L)
                        .setMin(1238123899000L)
                        .setNullCount(0L)
                        .build());
        expectedColumnStatsMap.put(
                "f_float",
                new ColumnStats.Builder()
                        .setMax(33.33300018310547D)
                        .setMin(33.31100082397461D)
                        .setNullCount(nullCount)
                        .build());
        expectedColumnStatsMap.put(
                "f_double",
                new ColumnStats.Builder().setMax(10.1D).setMin(1.1D).setNullCount(0L).build());
        expectedColumnStatsMap.put(
                "f_string",
                new ColumnStats.Builder().setMax("def").setMin("abcd").setNullCount(0L).build());
        expectedColumnStatsMap.put(
                "f_decimal5",
                new ColumnStats.Builder()
                        .setMax(new BigDecimal("223.45"))
                        .setMin(new BigDecimal("123.45"))
                        .setNullCount(0L)
                        .build());
        expectedColumnStatsMap.put(
                "f_decimal14",
                new ColumnStats.Builder()
                        .setMax(new BigDecimal("123333333355.33"))
                        .setMin(new BigDecimal("123333333333.33"))
                        .setNullCount(0L)
                        .build());
        expectedColumnStatsMap.put(
                "f_decimal38",
                new ColumnStats.Builder()
                        .setMax(new BigDecimal("123433343334333433343334333433343334.34"))
                        .setMin(new BigDecimal("123433343334333433343334333433343334.33"))
                        .setNullCount(nullCount)
                        .build());
        expectedColumnStatsMap.put(
                "f_date",
                new ColumnStats.Builder()
                        .setMax(Date.valueOf("1990-10-16"))
                        .setMin(Date.valueOf("1990-10-14"))
                        .setNullCount(0L)
                        .build());
        expectedColumnStatsMap.put(
                "f_timestamp3",
                new ColumnStats.Builder()
                        .setMax(
                                DateTimeUtils.parseTimestampData("1990-10-16 12:12:43.123", 3)
                                        .toTimestamp())
                        .setMin(
                                DateTimeUtils.parseTimestampData("1990-10-14 12:12:43.123", 3)
                                        .toTimestamp())
                        .setNullCount(0L)
                        .build());
        expectedColumnStatsMap.put("f_binary", null);
        expectedColumnStatsMap.put("f_array", null);
        expectedColumnStatsMap.put("f_map", null);
        expectedColumnStatsMap.put("f_row", null);

        assertThat(tableStats).isEqualTo(new TableStats(expectedRowCount, expectedColumnStatsMap));
    }

    private static void assertHiveTableParquetFormatTableStatsEquals(
            TableStats tableStats, int expectedRowCount, long nullCount) {
        Map<String, ColumnStats> expectedColumnStatsMap = new HashMap<>();
        expectedColumnStatsMap.put(
                "f_boolean", new ColumnStats.Builder().setNullCount(nullCount).build());
        expectedColumnStatsMap.put(
                "f_tinyint",
                new ColumnStats.Builder().setMax(3).setMin(1).setNullCount(0L).build());
        expectedColumnStatsMap.put(
                "f_smallint",
                new ColumnStats.Builder().setMax(128).setMin(100).setNullCount(0L).build());
        expectedColumnStatsMap.put(
                "f_int",
                new ColumnStats.Builder()
                        .setMax(45536)
                        .setMin(31000)
                        .setNullCount(nullCount)
                        .build());
        expectedColumnStatsMap.put(
                "f_bigint",
                new ColumnStats.Builder()
                        .setMax(1238123899121L)
                        .setMin(1238123899000L)
                        .setNullCount(0L)
                        .build());
        expectedColumnStatsMap.put(
                "f_float",
                new ColumnStats.Builder()
                        .setMax(33.333F)
                        .setMin(33.311F)
                        .setNullCount(nullCount)
                        .build());
        expectedColumnStatsMap.put(
                "f_double",
                new ColumnStats.Builder().setMax(10.1D).setMin(1.1D).setNullCount(0L).build());
        expectedColumnStatsMap.put(
                "f_string",
                new ColumnStats.Builder().setMax("def").setMin("abcd").setNullCount(0L).build());
        expectedColumnStatsMap.put(
                "f_decimal5",
                new ColumnStats.Builder()
                        .setMax(new BigDecimal("223.45"))
                        .setMin(new BigDecimal("123.45"))
                        .setNullCount(0L)
                        .build());
        expectedColumnStatsMap.put(
                "f_decimal14",
                new ColumnStats.Builder()
                        .setMax(new BigDecimal("123333333355.33"))
                        .setMin(new BigDecimal("123333333333.33"))
                        .setNullCount(0L)
                        .build());
        expectedColumnStatsMap.put(
                "f_decimal38",
                new ColumnStats.Builder()
                        .setMax(new BigDecimal("123433343334333433343334333433343334.34"))
                        .setMin(new BigDecimal("123433343334333433343334333433343334.33"))
                        .setNullCount(nullCount)
                        .build());
        expectedColumnStatsMap.put(
                "f_date",
                new ColumnStats.Builder()
                        .setMax(Date.valueOf("1990-10-16"))
                        .setMin(Date.valueOf("1990-10-14"))
                        .setNullCount(0L)
                        .build());
        // Now parquet store timestamp as type int96, and int96 now not support statistics, so
        // timestamp not support statistics now.
        expectedColumnStatsMap.put("f_timestamp3", null);

        expectedColumnStatsMap.put("f_binary", new ColumnStats.Builder().setNullCount(0L).build());
        expectedColumnStatsMap.put("f_array", null);
        expectedColumnStatsMap.put("f_map", null);
        expectedColumnStatsMap.put("f_row", null);
        assertThat(tableStats).isEqualTo(new TableStats(expectedRowCount, expectedColumnStatsMap));
    }
}
