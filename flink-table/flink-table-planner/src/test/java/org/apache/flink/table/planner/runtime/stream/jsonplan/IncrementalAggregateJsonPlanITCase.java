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
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.rules.physical.stream.IncrementalAggregateRule;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.planner.utils.AggregatePhaseStrategy;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/** Test for incremental aggregate json plan. */
public class IncrementalAggregateJsonPlanITCase extends JsonPlanTestBase {

    @Before
    public void setup() throws Exception {
        super.setup();
        tableEnv.getConfig()
                .getConfiguration()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
                        AggregatePhaseStrategy.TWO_PHASE.name());
        tableEnv.getConfig()
                .getConfiguration()
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true);
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
        tableEnv.getConfig()
                .getConfiguration()
                .set(IncrementalAggregateRule.TABLE_OPTIMIZER_INCREMENTAL_AGG_ENABLED(), true);
    }

    @Test
    public void testIncrementalAggregate()
            throws IOException, ExecutionException, InterruptedException {
        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.smallData3()),
                "a int",
                "b bigint",
                "c varchar");
        createTestNonInsertOnlyValuesSinkTable(
                "MySink", "b bigint", "a bigint", "primary key (b) not enforced");
        String jsonPlan =
                tableEnv.getJsonPlan(
                        "insert into MySink select b, "
                                + "count(distinct a) as a "
                                + "from MyTable group by b");

        tableEnv.executeJsonPlan(jsonPlan).await();

        List<String> result = TestValuesTableFactory.getResults("MySink");
        assertResult(Arrays.asList("+I[1, 1]", "+I[2, 2]"), result);
    }
}
