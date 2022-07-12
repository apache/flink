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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.catalog.CatalogPartitionImpl;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for statistics functionality in {@link FileSystemTableSource}. */
public class FileSystemStatisticsReportTest extends TableTestBase {

    private BatchTableTestUtil util;
    private TableEnvironment tEnv;

    @Before
    public void setup() throws Exception {
        util = batchTestUtil(TableConfig.getDefault());
        util.buildBatchProgram(FlinkBatchProgram.JOIN_REORDER());
        tEnv = util.getTableEnv();
        String path1 = tempFolder().newFile().getAbsolutePath();
        writeData(new File(path1), Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world"));

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
                        path1);
        tEnv.executeSql(ddl1);

        String path2 = tempFolder().newFolder().getAbsolutePath();
        writeData(new File(path2, "b=1"), Arrays.asList("1,1,hi", "2,1,hello"));
        writeData(new File(path2, "b=2"), Arrays.asList("3,2,hello world"));
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
                        path2);
        tEnv.executeSql(ddl2);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .get()
                .createPartition(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "1")),
                        new CatalogPartitionImpl(new HashMap<>(), ""),
                        false);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .get()
                .createPartition(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "2")),
                        new CatalogPartitionImpl(new HashMap<>(), ""),
                        false);

        String path3 = tempFolder().newFile().getAbsolutePath();
        writeData(new File(path1), Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world"));

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
                        path3);
        tEnv.executeSql(ddl3);
    }

    private void writeData(File file, List<String> data) throws IOException {
        Files.write(file.toPath(), String.join("\n", data).getBytes());
    }

    @Test
    public void testCatalogStatisticsExist() throws Exception {
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .get()
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
    public void testFilterPushDownAndCatalogStatisticsExist() throws TableNotExistException {
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .get()
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
    public void testNoPartitionPushDownAndCatalogStatisticsExist()
            throws PartitionNotExistException {
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .get()
                .alterPartitionStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "1")),
                        new CatalogTableStatistics(6L, 1, 100L, 100L),
                        false);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .get()
                .alterPartitionStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "2")),
                        new CatalogTableStatistics(3L, 1, 100L, 100L),
                        false);

        FlinkStatistic statistic = getStatisticsFromOptimizedPlan("select * from PartTable");
        assertThat(statistic.getTableStats()).isEqualTo(new TableStats(9));
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
    public void testPartitionPushDownAndCatalogStatisticsExist() throws PartitionNotExistException {
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .get()
                .alterPartitionStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "1")),
                        new CatalogTableStatistics(6L, 1, 100L, 100L),
                        false);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .get()
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
    public void testFilterPartitionPushDownPushDownAndCatalogStatisticsExist() throws Exception {
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .get()
                .alterPartitionStatistics(
                        new ObjectPath(tEnv.getCurrentDatabase(), "PartTable"),
                        new CatalogPartitionSpec(Collections.singletonMap("b", "1")),
                        new CatalogTableStatistics(6L, 1, 100L, 100L),
                        false);
        tEnv.getCatalog(tEnv.getCurrentCatalog())
                .get()
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

    private FlinkStatistic getStatisticsFromOptimizedPlan(String sql) {
        RelNode relNode = TableTestUtil.toRelNode(tEnv.sqlQuery(sql));
        RelNode optimized = util.getPlanner().optimize(relNode);
        FlinkStatisticVisitor visitor = new FlinkStatisticVisitor();
        visitor.go(optimized);
        return visitor.result;
    }

    private static class FlinkStatisticVisitor extends RelVisitor {
        private FlinkStatistic result = null;

        @Override
        public void visit(RelNode node, int ordinal, RelNode parent) {
            if (node instanceof TableScan) {
                Preconditions.checkArgument(result == null);
                TableSourceTable table = (TableSourceTable) node.getTable();
                result = table.getStatistic();
            }
            super.visit(node, ordinal, parent);
        }
    }
}
