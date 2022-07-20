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

package org.apache.flink.formats.csv;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for statistics functionality in {@link CsvFormatFactory} in the case of file system source.
 */
public class CsvFormatFilesystemStatisticsReportTest extends TableTestBase {
    private BatchTableTestUtil util;
    private TableEnvironment tEnv;
    @TempDir private static File path;

    @BeforeEach
    public void setup() throws IOException {
        util = batchTestUtil(TableConfig.getDefault());
        tEnv = util.getTableEnv();
        String pathName = writeData(path, Arrays.asList("1,1,hi", "2,1,hello", "3,2,hello world"));

        String ddl =
                String.format(
                        "CREATE TABLE sourceTable (\n"
                                + "  a bigint,\n"
                                + "  b int,\n"
                                + "  c varchar\n"
                                + ") with (\n"
                                + " 'connector' = 'filesystem',"
                                + " 'format' = 'csv',"
                                + " 'path' = '%s')",
                        pathName);
        tEnv.executeSql(ddl);
    }

    @Test
    public void testCsvFileSystemStatisticsReport() {
        FlinkStatistic statistic = getStatisticsFromOptimizedPlan("select * from sourceTable");
        assertThat(statistic.getTableStats()).isEqualTo(new TableStats(3));
    }

    private String writeData(File path, List<String> data) throws IOException {
        String file = path.getAbsolutePath() + "/00-00.tmp";
        Files.write(new File(file).toPath(), String.join("\n", data).getBytes());
        return file;
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
