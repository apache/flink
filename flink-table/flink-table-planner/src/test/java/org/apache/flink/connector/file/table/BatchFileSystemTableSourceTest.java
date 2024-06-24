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
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.plan.utils.OperatorType;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

/** Test for {@link FileSystemTableSource}. */
class BatchFileSystemTableSourceTest extends TableTestBase {

    private BatchTableTestUtil util;

    @TempDir protected Path fileTempFolder;
    protected String resultPath;

    @BeforeEach
    void setup() throws IOException, ExecutionException, InterruptedException {
        resultPath = TempDirUtils.newFolder(fileTempFolder).toURI().getPath();
        util = batchTestUtil(TableConfig.getDefault());
        TableEnvironment tEnv = util.getTableEnv();

        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar\n"
                        + ") with (\n"
                        + " 'connector' = 'filesystem',"
                        + " 'format' = 'testcsv',"
                        + " 'path' = '"
                        + resultPath
                        + "/no-partition')";
        tEnv.executeSql(srcTableDdl).await();

        String srcTableWithPartitionsDdl =
                "CREATE TABLE MyTableP (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar\n"
                        + ") PARTITIONED BY (a, b) with (\n"
                        + " 'connector' = 'filesystem',"
                        + " 'format' = 'testcsv',"
                        + " 'path' = '"
                        + resultPath
                        + "/partitioned', "
                        + " 'partitioned-read' = 'true')";
        tEnv.executeSql(srcTableWithPartitionsDdl).await();

        tEnv.executeSql(
                        "insert into MyTable values (1, 2, 'a'), (3, 4, 'b'), (5, 6, 'c'), (7, 8, 'd')")
                .await();

        tEnv.executeSql("insert into MyTableP partition(a='1', b='1') values ('a')").await();
        tEnv.executeSql("insert into MyTableP partition(a='1', b='2') values ('b')").await();
        tEnv.executeSql("insert into MyTableP partition(a='2', b='1') values ('c')").await();
        tEnv.executeSql("insert into MyTableP partition(a='2', b='2') values ('d')").await();
    }

    @Test
    void testShouldNotEliminatePartitioning1() {
        setHashAgg();
        util.verifyExecPlan("select a, b, count (c) from MyTable group by a, b");
    }

    @Test
    void testShouldNotEliminatePartitioning2() {
        setSortAgg();
        util.verifyExecPlan("select a, b, count (c) from MyTable group by a, b");
    }

    @Test
    void testShouldNotEliminatePartitioning3() {
        setHashAgg();
        util.verifyExecPlan("select c, b, count (a) from MyTableP group by c, b");
    }

    @Test
    void testShouldNotEliminatePartitioning4() {
        setHashAgg();
        util.verifyExecPlan("select a, count (c) from MyTableP group by a");
    }

    @Test
    void testShouldEliminatePartitioning1() {
        setHashAgg();
        util.verifyExecPlan("select a, b, count (c) from MyTableP group by a, b");
    }

    @Test
    void testShouldEliminatePartitioning2() {
        setHashAgg();
        util.verifyExecPlan("select b, a, count (c) from MyTableP group by b, a");
    }

    @Test
    void testShouldEliminatePartitioning3() {
        util.verifyExecPlan("select a, b, count (c) from MyTableP group by a, b");
    }

    @Test
    void testShouldEliminatePartitioning4() {
        setHashAgg();
        util.verifyExecPlan("select a, count (c) from MyTableP group by a, b");
    }

    void setSortAgg() {
        util.tableEnv()
                .getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        OperatorType.HashAgg.toString());
    }

    void setHashAgg() {
        util.tableEnv()
                .getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        OperatorType.SortAgg.toString());
    }
}
