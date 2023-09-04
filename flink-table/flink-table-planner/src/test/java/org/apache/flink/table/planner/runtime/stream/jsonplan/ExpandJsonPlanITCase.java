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

import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.plan.rules.physical.stream.IncrementalAggregateRule;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.table.planner.utils.AggregatePhaseStrategy;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.JsonPlanTestBase;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/** Test for expand json plan. */
class ExpandJsonPlanITCase extends JsonPlanTestBase {

    @Test
    void testExpand() throws Exception {
        tableEnv.getConfig()
                .set(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
                        AggregatePhaseStrategy.ONE_PHASE.name())
                .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true)
                .set(IncrementalAggregateRule.TABLE_OPTIMIZER_INCREMENTAL_AGG_ENABLED(), false);

        createTestValuesSourceTable(
                "MyTable",
                JavaScalaConversionUtil.toJava(TestData.smallData3()),
                "a int",
                "b bigint",
                "c varchar");
        createTestNonInsertOnlyValuesSinkTable(
                "MySink", "b bigint", "a bigint", "c varchar", "primary key (b) not enforced");
        compileSqlAndExecutePlan(
                        "insert into MySink select b, "
                                + "count(distinct a) as a, "
                                + "max(c) as c "
                                + "from MyTable group by b")
                .await();

        List<String> result = TestValuesTableFactory.getResults("MySink");
        assertResult(Arrays.asList("+I[1, 1, Hi]", "+I[2, 2, Hello world]"), result);
    }
}
