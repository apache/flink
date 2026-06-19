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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.planner.utils.StatisticsReportTestBase;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for statistics functionality in which storage format is parquet. */
public class ParquetFormatStatisticsReportTest extends StatisticsReportTestBase {

    private static ParquetFileFormatFactory.ParquetBulkDecodingFormat parquetBulkDecodingFormat;

    @BeforeEach
    public void setup(@TempDir File file) throws Exception {
        super.setup(file);
        createFileSystemSource();
        Configuration configuration = new Configuration();
        parquetBulkDecodingFormat =
                new ParquetFileFormatFactory.ParquetBulkDecodingFormat(configuration);
    }

    @Override
    protected String[] properties() {
        List<String> ret = new ArrayList<>();
        ret.add("'format'='parquet'");
        ret.add("'parquet.utc-timezone'='true'");
        ret.add("'parquet.compression'='gzip'");
        return ret.toArray(new String[0]);
    }

    @Test
    public void testParquetFormatStatsReportWithSingleFile() throws Exception {
        // insert data and get statistics.
        DataType dataType = tEnv.from("sourceTable").getResolvedSchema().toPhysicalRowDataType();
        tEnv.fromValues(dataType, getData()).executeInsert("sourceTable").await();
        assertThat(folder.listFiles()).hasSize(1);
        File[] files = folder.listFiles();
        assert files != null;
        TableStats tableStats =
                parquetBulkDecodingFormat.reportStatistics(
                        Collections.singletonList(new Path(files[0].toURI().toString())), dataType);
        assertParquetFormatTableStatsEquals(tableStats, 3, 1L);
    }

    @Test
    public void testParquetFormatStatsReportWithMultiFile() throws Exception {
        // insert data and get statistics.
        DataType dataType = tEnv.from("sourceTable").getResolvedSchema().toPhysicalRowDataType();
        tEnv.fromValues(dataType, getData()).executeInsert("sourceTable").await();
        tEnv.fromValues(dataType, getData()).executeInsert("sourceTable").await();
        assertThat(folder.listFiles()).hasSize(2);
        File[] files = folder.listFiles();
        List<Path> paths = new ArrayList<>();
        assert files != null;
        paths.add(new Path(files[0].toURI().toString()));
        paths.add(new Path(files[1].toURI().toString()));
        TableStats tableStats = parquetBulkDecodingFormat.reportStatistics(paths, dataType);
        assertParquetFormatTableStatsEquals(tableStats, 6, 2L);
    }

    @Test
    public void testParquetFormatStatsReportWithEmptyFile() {
        TableStats tableStats = parquetBulkDecodingFormat.reportStatistics(null, null);
        assertThat(tableStats).isEqualTo(TableStats.UNKNOWN);
    }

    protected static void assertParquetFormatTableStatsEquals(
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
        expectedColumnStatsMap.put(
                "f_timestamp3", new ColumnStats.Builder().setNullCount(0L).build());
        expectedColumnStatsMap.put(
                "f_timestamp9", new ColumnStats.Builder().setNullCount(0L).build());
        expectedColumnStatsMap.put(
                "f_timestamp_wtz", new ColumnStats.Builder().setNullCount(0L).build());
        expectedColumnStatsMap.put(
                "f_timestamp_ltz", new ColumnStats.Builder().setNullCount(nullCount).build());

        expectedColumnStatsMap.put("f_binary", new ColumnStats.Builder().setNullCount(0L).build());
        expectedColumnStatsMap.put(
                "f_varbinary", new ColumnStats.Builder().setNullCount(nullCount).build());
        expectedColumnStatsMap.put(
                "f_time",
                new ColumnStats.Builder()
                        .setMax(Time.valueOf("12:12:45"))
                        .setMin(Time.valueOf("12:12:43"))
                        .setNullCount(0L)
                        .build());

        // For complex types: ROW, ARRAY, MAP. The returned statistics have wrong null count
        // value, so now complex types stats return null.
        expectedColumnStatsMap.put("f_row", null);
        expectedColumnStatsMap.put("f_array", null);
        expectedColumnStatsMap.put("f_map", null);
        assertThat(tableStats).isEqualTo(new TableStats(expectedRowCount, expectedColumnStatsMap));
    }
}
