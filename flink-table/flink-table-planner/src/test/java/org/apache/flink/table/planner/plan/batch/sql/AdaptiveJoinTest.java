/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * imitations under the License.
 */

package org.apache.flink.table.planner.plan.batch.sql;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for AdaptiveBroadcastJoinProcessor. */
class AdaptiveJoinTest extends TableTestBase {

    private BatchTableTestUtil util;

    @BeforeEach
    void before() {
        util = batchTestUtil(TableConfig.getDefault());

        util.tableEnv()
                .getConfig()
                .getConfiguration()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_BROADCAST_JOIN_STRATEGY,
                        OptimizerConfigOptions.AdaptiveBroadcastJoinStrategy.AUTO);
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE T (\n"
                                + "  a BIGINT,\n"
                                + "  b BIGINT,\n"
                                + "  c VARCHAR,\n"
                                + "  d BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE T1 (\n"
                                + "  a1 BIGINT,\n"
                                + "  b1 BIGINT,\n"
                                + "  c1 VARCHAR,\n"
                                + "  d1 BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE T2 (\n"
                                + "  a2 BIGINT,\n"
                                + "  b2 BIGINT,\n"
                                + "  c2 VARCHAR,\n"
                                + "  d2 BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE T3 (\n"
                                + "  a3 BIGINT,\n"
                                + "  b3 BIGINT,\n"
                                + "  c3 VARCHAR,\n"
                                + "  d3 BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'bounded' = 'true'\n"
                                + ")");
    }

    @Test
    void testWithShuffleHashJoin() {
        util.tableEnv()
                .getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "NestedLoopJoin,SortMergeJoin");
        util.verifyExecPlan("SELECT * FROM T1, T2 WHERE a1 = a2");
    }

    @Test
    void testWithShuffleMergeJoin() {
        util.tableEnv()
                .getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "ShuffleHashJoin,NestedLoopJoin");
        util.verifyExecPlan("SELECT * FROM T1, T2 WHERE a1 = a2");
    }

    @Test
    void testWithStaticBroadcastJoin() {
        util.tableEnv()
                .getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "SortMergeJoin,ShuffleHashJoin,NestedLoopJoin")
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD,
                        Long.MAX_VALUE);
        util.verifyExecPlan("SELECT * FROM T1, T2 WHERE a1 = a2");
    }

    @Test
    void testWithBroadcastJoinRuntimeOnly() {
        util.tableEnv()
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin")
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD,
                        Long.MAX_VALUE)
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_ADAPTIVE_BROADCAST_JOIN_STRATEGY,
                        OptimizerConfigOptions.AdaptiveBroadcastJoinStrategy.RUNTIME_ONLY);
        util.verifyExecPlan("SELECT * FROM T1, T2 WHERE a1 = a2");
    }

    @Test
    void testShuffleJoinWithForwardForConsecutiveHash() {
        util.tableEnv()
                .getConfig()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_ENABLED, false);
        util.verifyExecPlan(
                "WITH r AS (SELECT * FROM T1, T2, T3 WHERE a1 = a2 and a1 = a3)\n"
                        + "SELECT sum(b1) FROM r group by a1");
    }

    @Test
    void testWithMultipleInput() {
        util.verifyExecPlan(
                "SELECT * FROM\n"
                        + "  (SELECT a FROM T1 JOIN T ON a = a1) t1\n"
                        + "  INNER JOIN\n"
                        + "  (SELECT d2 FROM T JOIN T2 ON d2 = a) t2\n"
                        + "  ON t1.a = t2.d2");
    }
}
