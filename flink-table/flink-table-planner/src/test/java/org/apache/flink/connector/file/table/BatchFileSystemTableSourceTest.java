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
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

/** Test for {@link FileSystemTableSource}. */
class BatchFileSystemTableSourceTest extends TableTestBase {

    private BatchTableTestUtil util;

    @BeforeEach
    void setup() throws ExecutionException, InterruptedException {
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
                        + " 'path' = '/tmp')";
        tEnv.executeSql(srcTableDdl);

        String srcTableWithPartitionsDdl =
                "CREATE TABLE MyTableP (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c varchar\n"
                        + ") PARTITIONED BY (a, b) with (\n"
                        + " 'connector' = 'filesystem',"
                        + " 'format' = 'testcsv',"
                        + " 'path' = '/tmp2')";
        tEnv.executeSql(srcTableWithPartitionsDdl);
    }

    @Test
    void testShouldNotEliminatePartitioning1() {
        util.verifyExecPlan("select a, b, count (c) from MyTable group by a, b");
    }

    @Test
    void testShouldEliminatePartitioning1() {
        util.verifyExecPlan("select a, b, count (c) from MyTableP group by a, b");
    }

    @Test
    void testShouldEliminatePartitioning2() {
        util.verifyExecPlan("select b, a, count (c) from MyTableP group by b, a");
    }
}
