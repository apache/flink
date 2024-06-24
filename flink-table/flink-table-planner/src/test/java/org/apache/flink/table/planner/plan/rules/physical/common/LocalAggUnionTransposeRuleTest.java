/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.physical.common;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;

/**
 * Tests for BatchLocalAggUnionTransposeRule and BatchLocalSortAggWithKeysUnionTransposeRule and
 * StreamLocalGroupAggregateUnionTransposeRule.
 */
@RunWith(Parameterized.class)
public class LocalAggUnionTransposeRuleTest extends TableTestBase {

    private TableTestUtil util;
    private TableConfig tableConfig;
    private final String aggWithGroup =
            "SELECT sum(price)\n"
                    + "FROM (\n"
                    + " SELECT price, type\n"
                    + " FROM table1\n"
                    + "\n"
                    + " UNION ALL\n"
                    + "\n"
                    + " SELECT price, type\n"
                    + " FROM table2\n"
                    + "\n"
                    + " UNION ALL\n"
                    + "\n"
                    + " SELECT price, type\n"
                    + " FROM table3\n"
                    + " )\n"
                    + "GROUP BY type";
    private final String aggWithoutGroup =
            "SELECT sum(price)\n"
                    + "FROM (\n"
                    + " SELECT price, type\n"
                    + " FROM table1\n"
                    + "\n"
                    + " UNION ALL\n"
                    + "\n"
                    + " SELECT price, type\n"
                    + " FROM table2\n"
                    + "\n"
                    + " UNION ALL\n"
                    + "\n"
                    + " SELECT price, type\n"
                    + " FROM table3\n"
                    + " )";

    private final String expand =
            "SELECT count(DISTINCT price), count(DISTINCT type)\n"
                    + "FROM (\n"
                    + " SELECT price, type\n"
                    + " FROM table1\n"
                    + "\n"
                    + " UNION ALL\n"
                    + "\n"
                    + " SELECT price, type\n"
                    + " FROM table2\n"
                    + " )";

    @Parameterized.Parameter public boolean isBatch;

    @Parameterized.Parameters(name = "isBatch: {0}")
    public static Collection<Boolean> isBatch() {
        return Arrays.asList(false, true);
    }

    @Before
    public void setup() {
        tableConfig = new TableConfig();
        if (isBatch) {
            util = batchTestUtil(tableConfig);
        } else {
            util = streamTestUtil(tableConfig);
        }

        tryEnableTwoPhaseAgg();
        String ddl =
                String.format(
                        "CREATE TABLE table1 (\n"
                                + "  id BIGINT,\n"
                                + "  name STRING,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT,\n"
                                + "  type STRING\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'filterable-fields' = 'id;type',\n"
                                + " 'bounded' = '%s'\n"
                                + ")",
                        isBatch);
        util.tableEnv().executeSql(ddl);

        String ddl2 =
                String.format(
                        "CREATE TABLE table2 (\n"
                                + "  id BIGINT,\n"
                                + "  name STRING,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT,\n"
                                + "  type STRING\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'filterable-fields' = 'id;type',\n"
                                + " 'bounded' = '%s'\n"
                                + ")",
                        isBatch);
        util.tableEnv().executeSql(ddl2);

        String ddl3 =
                String.format(
                        "CREATE TABLE table3 (\n"
                                + "  id BIGINT,\n"
                                + "  name STRING,\n"
                                + "  amount BIGINT,\n"
                                + "  price BIGINT,\n"
                                + "  type STRING\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values',\n"
                                + " 'filterable-fields' = 'id;type',\n"
                                + " 'bounded' = '%s'\n"
                                + ")",
                        isBatch);
        util.tableEnv().executeSql(ddl3);
    }

    // ---------------------------hash agg---------------------------------------

    @Test
    public void testDisableLocalHashUnionTranspose() {
        tableConfig.set(
                OptimizerConfigOptions.TABLE_OPTIMIZER_LOCAL_AGG_UNION_TRANSPOSE_ENABLED, false);
        util.verifyRelPlan(aggWithGroup);
    }

    @Test
    public void testEnableLocalHashUnionTranspose() {
        util.verifyRelPlan(aggWithGroup);
    }

    @Test
    public void testEnableLocalHashUnionTransposeNoGroup() {
        util.verifyRelPlan(aggWithoutGroup);
    }

    // ---------------------------sort agg---------------------------------------

    @Test
    public void testLocalSortAggUnionTranspose() {
        if (!isBatch) {
            return;
        }
        tableConfig.set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashAgg");
        util.verifyRelPlan(aggWithGroup);
    }

    @Test
    public void testLocalSortAggUnionTransposeNoGroup() {
        if (!isBatch) {
            return;
        }
        tableConfig.set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashAgg");
        util.verifyRelPlan(aggWithoutGroup);
    }

    // ---------------------------Expand--------------------------------------
    @Test
    public void testDisableExpandUnionTranspose() {
        tableConfig.set(
                OptimizerConfigOptions.TABLE_OPTIMIZER_LOCAL_AGG_UNION_TRANSPOSE_ENABLED, false);
        util.verifyRelPlan(expand);
    }

    @Test
    public void testEnableExpandUnionTranspose() {
        util.verifyRelPlan(expand);
    }

    @Test
    public void testStreamEnableExpandUnionTransposeDisableMiniBatch() {
        if (isBatch) {
            return;
        }
        // disable minibatch
        tableConfig.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        tableConfig.set(
                ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, Duration.ofMillis(0));
        util.verifyRelPlan(expand);
    }

    // ---------------------------Helper methods--------------------------------------

    private void tryEnableTwoPhaseAgg() {
        TableConfig tableConfig = util.getTableEnv().getConfig();
        // force to use two phase
        tableConfig.set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, "TWO_PHASE");
        // avoid push down local agg
        tableConfig.set(
                OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_AGGREGATE_PUSHDOWN_ENABLED, false);

        if (!isBatch) {
            // enable minibatch for stream
            tableConfig.set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
            tableConfig.set(
                    ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                    Duration.ofMillis(1000));
            // enable distinct split optimizer
            tableConfig.set(
                    OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true);
        }
    }
}
