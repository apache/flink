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

package org.apache.flink.table.planner.runtime.stream.jsonplan;

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions;
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.VarSum1AggFunction;
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.VarSum2AggFunction;
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvg;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/** Test for group aggregate json plan. */
@RunWith(Parameterized.class)
public class GroupAggregateJsonPlanITCase extends JsonPlanTestBase {

    @Parameterized.Parameter public boolean isMiniBatchEnabled;

    @Parameterized.Parameters(name = "isMiniBatchEnabled={0}")
    public static List<Boolean> testData() {
        return Arrays.asList(true, false);
    }

    @Before
    public void setup() throws Exception {
        super.setup();
        if (isMiniBatchEnabled) {
            tableEnv.getConfig()
                    .getConfiguration()
                    .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true);
            tableEnv.getConfig()
                    .getConfiguration()
                    .set(
                            ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY,
                            Duration.ofSeconds(10));
            tableEnv.getConfig()
                    .getConfiguration()
                    .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 5L);
        } else {
            tableEnv.getConfig()
                    .getConfiguration()
                    .set(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, false);
        }
    }

    @Test
    public void testSimpleAggCallsWithGroupBy() throws Exception {
        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.smallData3()),
                "a int",
                "b bigint",
                "c varchar");
        createTestNonInsertOnlyValuesSinkTable(
                "MySink",
                "b bigint",
                "cnt bigint",
                "avg_a double",
                "min_c varchar",
                "primary key (b) not enforced");
        String jsonPlan =
                tableEnv.getJsonPlan(
                        "insert into MySink select b, "
                                + "count(*) as cnt, "
                                + "avg(a) filter (where a > 1) as avg_a, "
                                + "min(c) as min_c "
                                + "from MyTable group by b");
        tableEnv.executeJsonPlan(jsonPlan).await();

        List<String> result = TestValuesTableFactory.getResults("MySink");
        assertResult(Arrays.asList("+I[1, 1, null, Hi]", "+I[2, 2, 2.0, Hello]"), result);
    }

    @Test
    public void testDistinctAggCalls() throws Exception {
        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.data2()),
                "a int",
                "b bigint",
                "c int",
                "d varchar",
                "e bigint");
        createTestNonInsertOnlyValuesSinkTable(
                "MySink",
                "e bigint",
                "cnt_a1 bigint",
                "cnt_a2 bigint",
                "sum_a bigint",
                "sum_b bigint",
                "avg_b double",
                "cnt_d bigint",
                "primary key (e) not enforced");
        String jsonPlan =
                tableEnv.getJsonPlan(
                        "insert into MySink select e, "
                                + "count(distinct a) filter (where b > 10) as cnt_a1, "
                                + "count(distinct a) as cnt_a2, "
                                + "sum(distinct a) as sum_a, "
                                + "sum(distinct b) as sum_b, "
                                + "avg(b) as avg_b, "
                                + "count(distinct d) as concat_d "
                                + "from MyTable group by e");
        tableEnv.executeJsonPlan(jsonPlan).await();

        List<String> result = TestValuesTableFactory.getResults("MySink");
        assertResult(
                Arrays.asList(
                        "+I[1, 1, 4, 12, 32, 6.0, 5]",
                        "+I[2, 1, 4, 14, 57, 8.0, 7]",
                        "+I[3, 1, 2, 8, 31, 10.0, 3]"),
                result);
    }

    @Test
    public void testUserDefinedAggCallsWithoutMerge() throws Exception {
        tableEnv.createTemporaryFunction("my_sum1", new VarSum1AggFunction());
        tableEnv.createFunction("my_avg", WeightedAvg.class);
        tableEnv.createTemporarySystemFunction("my_sum2", VarSum2AggFunction.class);

        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.data2()),
                "a int",
                "b bigint",
                "c int",
                "d varchar",
                "e bigint");
        createTestNonInsertOnlyValuesSinkTable(
                "MySink",
                "d bigint",
                "s1 bigint",
                "s2 bigint",
                "s3 bigint",
                "primary key (d) not enforced");

        String jsonPlan =
                tableEnv.getJsonPlan(
                        "insert into MySink select "
                                + "e, "
                                + "my_sum1(c, 10) as s1, "
                                + "my_sum2(5, c) as s2, "
                                + "my_avg(e, a) as s3 "
                                + "from MyTable group by e");
        tableEnv.executeJsonPlan(jsonPlan).await();

        List<String> result = TestValuesTableFactory.getResults("MySink");
        assertResult(
                Arrays.asList("+I[1, 77, 0, 1]", "+I[2, 120, 0, 2]", "+I[3, 58, 0, 3]"), result);
    }

    @Test
    public void testUserDefinedAggCallsWithMerge() throws Exception {
        tableEnv.createFunction("my_avg", JavaUserDefinedAggFunctions.WeightedAvgWithMerge.class);
        tableEnv.createTemporarySystemFunction(
                "my_concat_agg", JavaUserDefinedAggFunctions.ConcatDistinctAggFunction.class);

        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.data2()),
                "a int",
                "b bigint",
                "c int",
                "d varchar",
                "e bigint");
        createTestNonInsertOnlyValuesSinkTable(
                "MySink", "d bigint", "s1 bigint", "c1 varchar", "primary key (d) not enforced");

        String jsonPlan =
                tableEnv.getJsonPlan(
                        "insert into MySink select "
                                + "e, "
                                + "my_avg(e, a) as s1, "
                                + "my_concat_agg(d) as c1 "
                                + "from MyTable group by e");
        tableEnv.executeJsonPlan(jsonPlan).await();

        List<String> result = TestValuesTableFactory.getResults("MySink");
        assertResult(
                Arrays.asList(
                        "+I[1, 1, Hallo Welt wie|Hallo|GHI|EFG|DEF]",
                        "+I[2, 2, Hallo Welt wie gehts?|Hallo Welt|ABC|FGH|CDE|JKL|KLM]",
                        "+I[3, 3, HIJ|IJK|BCD]"),
                result);
    }
}
