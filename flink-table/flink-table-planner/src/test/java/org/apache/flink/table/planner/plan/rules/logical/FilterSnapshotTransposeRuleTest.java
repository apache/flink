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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.plan.optimize.program.BatchOptimizeContext;
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSets;
import org.junit.Before;
import org.junit.Test;

/** Test for {@link FlinkFilterSnapshotTransposeRule}. */
public class FilterSnapshotTransposeRuleTest extends TableTestBase {
    private final BatchTableTestUtil util = batchTestUtil(TableConfig.getDefault());

    @Before
    public void setup() {
        util.buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE());
        CalciteConfig calciteConfig =
                TableConfigUtils.getCalciteConfig(util.tableEnv().getConfig());
        calciteConfig
                .getBatchProgram()
                .get()
                .addLast(
                        "rules",
                        FlinkHepRuleSetProgramBuilder.<BatchOptimizeContext>newBuilder()
                                .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
                                .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                                .add(
                                        RuleSets.ofList(
                                                CoreRules.FILTER_INTO_JOIN,
                                                FlinkFilterSnapshotTransposeRule.INSTANCE,
                                                PushFilterIntoTableSourceScanRule.INSTANCE))
                                .build());
        String ddl1 =
                "CREATE TABLE ScanTable (\n"
                        + "  `id` BIGINT,\n"
                        + "  `len` BIGINT,\n"
                        + "  `content` STRING,\n"
                        + "  `proctime` AS PROCTIME()"
                        + ") WITH (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'true'\n"
                        + ")";
        util.tableEnv().executeSql(ddl1);
        String ddl2 =
                "CREATE TABLE LookupTable (\n"
                        + "  `age` INT,\n"
                        + "  `id` BIGINT,\n"
                        + "  `name` STRING,\n"
                        + "  `info` STRING,\n"
                        + "  `price` DOUBLE\n"
                        + ") WITH ("
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'true', \n"
                        + "  'filterable-fields' =  'price;age;info'\n"
                        + ")";
        util.tableEnv().executeSql(ddl2);
    }

    @Test
    public void testCanPushDown() {
        String ddl =
                "SELECT ScanTable.id, ScanTable.len, LookupTable.age, LookupTable.name "
                        + "FROM ScanTable "
                        + "JOIN LookupTable FOR system_time AS OF ScanTable.proctime ON ScanTable.id = LookupTable.id "
                        + "WHERE LookupTable.age > 10";

        util.verifyRelPlan(ddl);
    }

    @Test
    public void testCannotPushDown() {
        String ddl =
                "SELECT ScanTable.id, ScanTable.len, LookupTable.age, LookupTable.name "
                        + "FROM ScanTable "
                        + "JOIN LookupTable FOR system_time AS OF ScanTable.proctime ON ScanTable.id = LookupTable.id "
                        + "WHERE LookupTable.name = 'flink'";

        util.verifyRelPlan(ddl);
    }

    @Test
    public void testCannotPushDown2() {
        String ddl =
                "SELECT ScanTable.id, ScanTable.len, LookupTable.age, LookupTable.name "
                        + "FROM ScanTable "
                        + "JOIN LookupTable FOR system_time AS OF ScanTable.proctime ON ScanTable.id = LookupTable.id "
                        + "WHERE LookupTable.name <> 'beam' AND LookupTable.name <> 'spark'";

        util.verifyRelPlan(ddl);
    }

    @Test
    public void testCannotPushDown3() {
        String ddl =
                "SELECT ScanTable.id, ScanTable.len, LookupTable.age, LookupTable.name "
                        + "FROM ScanTable "
                        + "JOIN LookupTable FOR system_time AS OF ScanTable.proctime ON ScanTable.id = LookupTable.id "
                        + "WHERE LookupTable.name = 'beam' OR LookupTable.name = 'spark'";

        util.verifyRelPlan(ddl);
    }

    @Test
    public void testWithUdf() {
        String ddl =
                "SELECT ScanTable.id, ScanTable.len, LookupTable.age, LookupTable.name, LookupTable.info "
                        + "FROM ScanTable "
                        + "JOIN LookupTable FOR system_time AS OF ScanTable.proctime ON ScanTable.id = LookupTable.id "
                        + "WHERE UPPER(LookupTable.info) = 'test'";

        util.verifyRelPlan(ddl);
    }

    @Test
    public void testPartialPushDown() {
        String ddl =
                "SELECT ScanTable.id, ScanTable.len, LookupTable.age, LookupTable.name "
                        + "FROM ScanTable "
                        + "JOIN LookupTable FOR system_time AS OF ScanTable.proctime ON ScanTable.id = LookupTable.id "
                        + "WHERE LookupTable.name = 'flink' AND LookupTable.age > 10";

        util.verifyRelPlan(ddl);
    }

    @Test
    public void testCanNotPartialPushDown() {
        String ddl =
                "SELECT ScanTable.id, ScanTable.len, LookupTable.age, LookupTable.name "
                        + "FROM ScanTable "
                        + "JOIN LookupTable FOR system_time AS OF ScanTable.proctime ON ScanTable.id = LookupTable.id "
                        + "WHERE LookupTable.name = 'flink' OR LookupTable.age > 10";

        util.verifyRelPlan(ddl);
    }

    @Test
    public void testFullyPushDown() {
        String ddl =
                "SELECT ScanTable.id, ScanTable.len, LookupTable.age, LookupTable.name "
                        + "FROM ScanTable "
                        + "JOIN LookupTable FOR system_time AS OF ScanTable.proctime ON ScanTable.id = LookupTable.id "
                        + "WHERE LookupTable.age < 100 AND LookupTable.age > 10";

        util.verifyRelPlan(ddl);
    }

    @Test
    public void testUnconvertedExpression() {
        String ddl =
                "SELECT ScanTable.id, ScanTable.len, LookupTable.age, LookupTable.name "
                        + "FROM ScanTable "
                        + "JOIN LookupTable FOR system_time AS OF ScanTable.proctime ON ScanTable.id = LookupTable.id "
                        + "WHERE LookupTable.age < 100 AND LookupTable.age > 10 AND CAST(LookupTable.price AS BIGINT) < 1000";

        util.verifyRelPlan(ddl);
    }

    @Test
    public void testComplicatedPartialPushDown() {
        String ddl =
                "SELECT ScanTable.id, ScanTable.len, LookupTable.age, LookupTable.name "
                        + "FROM ScanTable "
                        + "JOIN LookupTable FOR system_time AS OF ScanTable.proctime ON ScanTable.id = LookupTable.id "
                        + "WHERE LookupTable.name = 'flink' AND LookupTable.age > 10 AND ScanTable.content <> 'flink'";

        util.verifyRelPlan(ddl);
    }
}
