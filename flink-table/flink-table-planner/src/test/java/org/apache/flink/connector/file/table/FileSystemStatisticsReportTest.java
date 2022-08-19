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

package org.apache.flink.connector.file.table;

import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.plan.stats.ColumnStats;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.utils.CatalogTableStatisticsConverter;
import org.apache.flink.table.planner.utils.StatisticsReportTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for statistics functionality in {@link FileSystemTableSource}. */
public class FileSystemStatisticsReportTest extends StatisticsReportTestBase {

    @BeforeEach
    public void setup(@TempDir File file) throws Exception {
        super.setup(file);
        String filePath1 =
                createFileAndWriteData(
                        file, "00-00.tmp", Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world"));
        String ddl1 =
                String.format(
                        "CREATE TABLE NonPartTable (\n"
                                + "  a bigint,\n"
                                + "  b int,\n"
                                + "  c varchar\n"
                                + ") with (\n"
                                + " 'connector' = 'filesystem',"
                                + " 'format' = 'testcsv',"
                                + " 'path' = '%s')",
                        filePath1);
        tEnv.executeSql(ddl1);

        File partitionDataPath = new File(file, "partitionData");
        partitionDataPath.mkdirs();
        writeData(new File(partitionDataPath, "b=1"), Arrays.asList("1,1,hi", "2,1,hello"));
        writeData(new File(partitionDataPath, "b=2"), Collections.singletonList("3,2,hello world"));
        writeData(new File(partitionDataPath, "b=3"), Collections.singletonList("4,3,hello"));
        String ddl2 =
                String.format(
                        "CREATE TABLE PartTable (\n"
                                + "  a bigint,\n"
                                + "  b int,\n"
                                + "  c varchar\n"
                                + ") partitioned by(b) with (\n"
                                + " 'connector' = 'filesystem',"
                                + " 'format' = 'testcsv',"
                                + " 'path' = '%s')",
                        partitionDataPath.toURI());
        tEnv.executeSql(ddl2);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .createPartition(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "1")),
                        new CatalogPartitionImpl(new HashMap<>(), ""),
                        false);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .createPartition(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "2")),
                        new CatalogPartitionImpl(new HashMap<>(), ""),
                        false);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .createPartition(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "3")),
                        new CatalogPartitionImpl(new HashMap<>(), ""),
                        false);

        String filePath2 =
                createFileAndWriteData(
                        file, "00-01.tmp", Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world"));

        String ddl3 =
                String.format(
                        "CREATE TABLE DisableSourceReportTable (\n"
                                + "  a bigint,\n"
                                + "  b int,\n"
                                + "  c varchar\n"
                                + ") with (\n"
                                + " 'connector' = 'filesystem',"
                                + " 'format' = 'testcsv',"
                                + " 'source.report-statistics' = 'NONE',"
                                + " 'path' = '%s')",
                        filePath2);
        tEnv.executeSql(ddl3);

        String emptyPath = createFileAndWriteData(file, "00-02.tmp", Collections.emptyList());
        String ddl4 =
                String.format(
                        "CREATE TABLE emptyTable (\n"
                                + "  a bigint,\n"
                                + "  b int,\n"
                                + "  c varchar\n"
                                + ") with (\n"
                                + " 'connector' = 'filesystem',"
                                + " 'format' = 'testcsv',"
                                + " 'path' = '%s')",
                        emptyPath);
        tEnv.executeSql(ddl4);
    }

    @Override
    protected String[] properties() {
        return new String[0];
    }

    private String createFileAndWriteData(File path, String fileName, List<String> data)
            throws IOException {
        String file = path.getAbsolutePath() + "/" + fileName;
        Files.write(new File(file).toPath(), String.join("\n", data).getBytes());
        return file;
    }

    private void writeData(File file, List<String> data) throws IOException {
        Files.write(file.toPath(), String.join("\n", data).getBytes());
    }

    @Test
    public void testCatalogStatisticsExist() throws Exception {
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .alterTableStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "NonPartTable"),
                        new CatalogTableStatistics(10L, 1, 100L, 100L),
                        false);

        FlinkStatistic statistic = getStatisticsFromOptimizedPlan("select * from NonPartTable");
        assertThat(statistic.getTableStats()).isEqualTo(new TableStats(10));
    }

    @Test
    public void testCatalogStatisticsDoNotExist() {
        FlinkStatistic statistic = getStatisticsFromOptimizedPlan("select * from NonPartTable");
        assertThat(statistic.getTableStats()).isEqualTo(new TableStats(3));
    }

    @Test
    public void testDisableSourceReport() {
        FlinkStatistic statistic =
                getStatisticsFromOptimizedPlan("select * from DisableSourceReportTable");
        assertThat(statistic.getTableStats()).isEqualTo(TableStats.UNKNOWN);
    }

    @Test
    public void testFilterPushDownAndCatalogStatisticsExist() throws Exception {
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .alterTableStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "NonPartTable"),
                        new CatalogTableStatistics(10L, 1, 100L, 100L),
                        false);

        FlinkStatistic statistic =
                getStatisticsFromOptimizedPlan("select * from NonPartTable where a > 10");
        assertThat(statistic.getTableStats()).isEqualTo(new TableStats(10));
    }

    @Test
    public void testFilterPushDownAndCatalogStatisticsDoNotExist() {
        FlinkStatistic statistic =
                getStatisticsFromOptimizedPlan("select * from NonPartTable where a > 10");
        assertThat(statistic.getTableStats()).isEqualTo(new TableStats(3));
    }

    @Test
    public void testFilterPushDownAndReportStatisticsDisabled() {
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_REPORT_STATISTICS_ENABLED,
                        false);
        FlinkStatistic statistic =
                getStatisticsFromOptimizedPlan("select * from NonPartTable where a > 10");
        assertThat(statistic.getTableStats()).isEqualTo(TableStats.UNKNOWN);
    }

    @Test
    public void testLimitPushDownAndCatalogStatisticsDoNotExist() {
        FlinkStatistic statistic =
                getStatisticsFromOptimizedPlan("select * from NonPartTable limit 1");
        assertThat(statistic.getTableStats()).isEqualTo(new TableStats(1));
    }

    @Test
    public void testNoPartitionPushDownAndCatalogStatisticsExist() throws Exception {
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .alterPartitionStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "1")),
                        new CatalogTableStatistics(6L, 1, 100L, 100L),
                        false);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .alterPartitionStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "2")),
                        new CatalogTableStatistics(3L, 1, 100L, 100L),
                        false);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .alterPartitionStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "3")),
                        new CatalogTableStatistics(3L, 1, 100L, 100L),
                        false);

        FlinkStatistic statistic = getStatisticsFromOptimizedPlan("select * from PartTable");
        assertThat(statistic.getTableStats()).isEqualTo(new TableStats(12));
    }

    @Test
    public void tesNoPartitionPushDownAndCatalogStatisticsPartialExist() throws Exception {
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .alterPartitionStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "1")),
                        new CatalogTableStatistics(6L, 1, 100L, 100L),
                        false);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .alterPartitionStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "2")),
                        new CatalogTableStatistics(3L, 1, 100L, 100L),
                        false);
        // For partition table 'PartTable', partition 'b=3' have no catalog statistics, so get
        // partition table stats from catalog will return TableStats.UNKNOWN. So we will recompute
        // stats from source.
        FlinkStatistic statistic = getStatisticsFromOptimizedPlan("select * from PartTable");
        // there are four rows in file system.
        assertThat(statistic.getTableStats()).isEqualTo(new TableStats(4));
    }

    @Test
    public void testNoPartitionPushDownAndReportStatisticsDisabled() {
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_REPORT_STATISTICS_ENABLED,
                        false);
        FlinkStatistic statistic = getStatisticsFromOptimizedPlan("select * from PartTable");
        assertThat(statistic.getTableStats()).isEqualTo(TableStats.UNKNOWN);
    }

    @Test
    public void testPartitionPushDownAndCatalogStatisticsExist() throws Exception {
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .alterPartitionStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "1")),
                        new CatalogTableStatistics(6L, 1, 100L, 100L),
                        false);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .alterPartitionStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "2")),
                        new CatalogTableStatistics(3L, 1, 100L, 100L),
                        false);

        FlinkStatistic statistic =
                getStatisticsFromOptimizedPlan("select * from PartTable where b = 1");
        assertThat(statistic.getTableStats()).isEqualTo(new TableStats(6));
    }

    @Test
    public void testPartitionPushDownAndCatalogColumnStatisticsExist() throws Exception {
        // The purpose of this test case is to test the correctness of stats after partition push
        // down, and recompute partition table and column stats. For partition table, merged Ndv for
        // columns which are partition keys using sum instead of max (other columns using max).
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .alterPartitionStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "1")),
                        new CatalogTableStatistics(6L, 1, 100L, 100L),
                        false);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .alterPartitionStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "2")),
                        new CatalogTableStatistics(3L, 1, 100L, 100L),
                        false);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .alterPartitionStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "3")),
                        new CatalogTableStatistics(3L, 1, 100L, 100L),
                        false);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .alterPartitionColumnStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "1")),
                        createSinglePartitionColumnStats(),
                        false);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .alterPartitionColumnStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "2")),
                        createSinglePartitionColumnStats(),
                        false);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .alterPartitionColumnStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "3")),
                        createSinglePartitionColumnStats(),
                        false);
        FlinkStatistic statistic =
                getStatisticsFromOptimizedPlan("select * from PartTable where b < 3");
        assertThat(statistic.getTableStats())
                .isEqualTo(new TableStats(9, createMergedPartitionColumnStats()));
    }

    @Test
    public void testFilterPartitionPushDownPushDownAndCatalogStatisticsExist() throws Exception {
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .alterPartitionStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "1")),
                        new CatalogTableStatistics(6L, 1, 100L, 100L),
                        false);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .orElseThrow(Exception::new)
                .alterPartitionStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "2")),
                        new CatalogTableStatistics(3L, 1, 100L, 100L),
                        false);

        FlinkStatistic statistic =
                getStatisticsFromOptimizedPlan("select * from PartTable where a > 10 and b = 1");
        assertThat(statistic.getTableStats()).isEqualTo(new TableStats(6));
    }

    @Test
    public void testFilterPartitionPushDownAndCatalogStatisticsDoNotExist() {
        FlinkStatistic statistic =
                getStatisticsFromOptimizedPlan("select * from PartTable where a > 10 and b = 1");
        assertThat(statistic.getTableStats()).isEqualTo(new TableStats(2));
    }

    @Test
    public void testFilterPartitionPushDownAndReportStatisticsDisabled() {
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_REPORT_STATISTICS_ENABLED,
                        false);
        FlinkStatistic statistic =
                getStatisticsFromOptimizedPlan("select * from PartTable where a > 10 and b = 1");
        assertThat(statistic.getTableStats()).isEqualTo(TableStats.UNKNOWN);
    }

    @Test
    public void testFileSystemSourceWithoutData() {
        FlinkStatistic statistic = getStatisticsFromOptimizedPlan("select * from emptyTable");
        assertThat(statistic.getTableStats()).isEqualTo(TableStats.UNKNOWN);
    }

    @Test
    public void testFileSystemSourceWithoutDataWithLimitPushDown() {
        // TODO for source support limit push down and query have limit condition, In
        // PushLimitIntoTableSourceScanRule will give stats a new rowCount value even if this table
        // have no data.
        FlinkStatistic statistic =
                getStatisticsFromOptimizedPlan("select * from emptyTable limit 1");
        assertThat(statistic.getTableStats()).isEqualTo(new TableStats(1));
    }

    private CatalogColumnStatistics createSinglePartitionColumnStats() {
        Map<String, CatalogColumnStatisticsDataBase> colStatsMap = new HashMap<>();
        CatalogColumnStatisticsDataLong longColStats =
                new CatalogColumnStatisticsDataLong(1L, 10L, 5L, 5L);
        colStatsMap.put("a", longColStats);
        colStatsMap.put("b", longColStats);
        CatalogColumnStatisticsDataString stringColStats =
                new CatalogColumnStatisticsDataString(10L, 10D, 5L, 5L);
        colStatsMap.put("c", stringColStats);
        return new CatalogColumnStatistics(colStatsMap);
    }

    private Map<String, ColumnStats> createMergedPartitionColumnStats() {
        Map<String, CatalogColumnStatisticsDataBase> colStatsMap = new HashMap<>();
        CatalogColumnStatisticsDataLong longColStats =
                new CatalogColumnStatisticsDataLong(1L, 10L, 5L, 10L);
        colStatsMap.put("a", longColStats);
        // Merged Ndv for columns which are partition keys using sum instead of max.
        CatalogColumnStatisticsDataLong longColStats2 =
                new CatalogColumnStatisticsDataLong(1L, 10L, 10L, 10L);
        colStatsMap.put("b", longColStats2);
        CatalogColumnStatisticsDataString stringColStats =
                new CatalogColumnStatisticsDataString(10L, 10D, 5L, 10L);
        colStatsMap.put("c", stringColStats);
        return CatalogTableStatisticsConverter.convertToColumnStatsMap(colStatsMap);
    }
}
