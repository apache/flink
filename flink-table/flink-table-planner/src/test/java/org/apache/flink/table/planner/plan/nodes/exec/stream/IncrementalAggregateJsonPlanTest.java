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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.rules.physical.stream.IncrementalAggregateRule;
import org.apache.flink.table.planner.utils.AggregatePhaseStrategy;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

import java.time.Duration;

/** Test json serialization/deserialization for incremental aggregate. */
public class IncrementalAggregateJsonPlanTest extends TableTestBase {

    private StreamTableTestUtil util;
    private TableEnvironment tEnv;

    @Before
    public void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        tEnv = util.getTableEnv();
        tEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
                        AggregatePhaseStrategy.TWO_PHASE.name())
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true)
                .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                        Duration.ofSeconds(10))
                .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 5L)
                .set(IncrementalAggregateRule.TABLE_OPTIMIZER_INCREMENTAL_AGG_ENABLED(), true);

        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + "  a bigint,\n"
                        + "  b int not null,\n"
                        + "  c varchar,\n"
                        + "  d bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        tEnv.executeSql(srcTableDdl);
    }

    @Test
    public void testIncrementalAggregate() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  c bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select a, "
                        + "count(distinct c) as c "
                        + "from MyTable group by a");
    }

    @Test
    public void testIncrementalAggregateWithSumCountDistinctAndRetraction() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  b bigint,\n"
                        + "  sum_b int,\n"
                        + "  cnt_distinct_b bigint,\n"
                        + "  cnt1 bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink "
                        + "select b, sum(b1), count(distinct b1), count(1) "
                        + " from "
                        + "   (select a, count(b) as b, max(b) as b1 from MyTable group by a)"
                        + " group by b");
    }
}
