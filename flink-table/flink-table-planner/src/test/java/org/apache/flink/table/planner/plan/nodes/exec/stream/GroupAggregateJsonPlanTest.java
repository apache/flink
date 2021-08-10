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
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.CountDistinct;
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.VarSum1AggFunction;
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.VarSum2AggFunction;
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvg;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/** Test json serialization/deserialization for group aggregate. */
@RunWith(Parameterized.class)
public class GroupAggregateJsonPlanTest extends TableTestBase {

    @Parameterized.Parameter public boolean isMiniBatchEnabled;

    private StreamTableTestUtil util;
    private TableEnvironment tEnv;

    @Parameterized.Parameters(name = "isMiniBatchEnabled={0}")
    public static List<Boolean> testData() {
        return Arrays.asList(true, false);
    }

    @Before
    public void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        tEnv = util.getTableEnv();
        if (isMiniBatchEnabled) {
            tEnv.getConfig()
                    .getConfiguration()
                    .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
            tEnv.getConfig()
                    .getConfiguration()
                    .set(
                            ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                            Duration.ofSeconds(10));
            tEnv.getConfig()
                    .getConfiguration()
                    .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 5L);
        } else {
            tEnv.getConfig()
                    .getConfiguration()
                    .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        }

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
    public void testSimpleAggCallsWithGroupBy() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  b bigint,\n"
                        + "  cnt_a bigint,\n"
                        + "  max_b bigint,\n"
                        + "  min_c varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select b, "
                        + "count(a) as cnt_a, "
                        + "max(b) filter (where b > 1) as max_b, "
                        + "min(c) as min_c "
                        + "from MyTable group by b");
    }

    @Test
    public void testSimpleAggWithoutGroupBy() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  avg_a double,\n"
                        + "  cnt bigint,\n"
                        + "  cnt_b bigint,\n"
                        + "  min_b bigint,\n"
                        + "  max_c varchar\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select "
                        + "avg(a) as avg_a, "
                        + "count(*) as cnt, "
                        + "count(b) as cnt_b, "
                        + "min(b) as min_b, "
                        + "max(c) filter (where a > 1) as max_c "
                        + "from MyTable");
    }

    @Test
    public void testDistinctAggCalls() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  d bigint,\n"
                        + "  cnt_a1 bigint,\n"
                        + "  cnt_a2 bigint,\n"
                        + "  sum_a bigint,\n"
                        + "  sum_b int,\n"
                        + "  avg_b double,\n"
                        + "  cnt_c bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select d, "
                        + "count(distinct a) filter (where b > 10) as cnt_a1, "
                        + "count(distinct a) as cnt_a2, "
                        + "sum(distinct a) as sum_a, "
                        + "sum(distinct b) as sum_b, "
                        + "avg(b) as avg_b, "
                        + "count(distinct c) as cnt_d "
                        + "from MyTable group by d");
    }

    @Test
    public void testUserDefinedAggCalls() {
        tEnv.createTemporaryFunction("my_sum1", new VarSum1AggFunction());
        tEnv.createFunction("my_avg", WeightedAvg.class);
        tEnv.createTemporarySystemFunction("my_sum2", VarSum2AggFunction.class);
        tEnv.createTemporarySystemFunction("my_count", new CountDistinct());
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  b bigint,\n"
                        + "  a1 bigint,\n"
                        + "  a2 bigint,\n"
                        + "  a3 bigint,\n"
                        + "  c1 bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select "
                        + "b, "
                        + "my_sum1(b, 10) as a1, "
                        + "my_sum2(5, b) as a2, "
                        + "my_avg(d, a) as a3, "
                        + "my_count(c) as c1 "
                        + "from MyTable group by b");
    }
}
