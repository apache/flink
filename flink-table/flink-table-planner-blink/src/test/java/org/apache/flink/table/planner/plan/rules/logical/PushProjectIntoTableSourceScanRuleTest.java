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

import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.plan.optimize.program.BatchOptimizeContext;
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram;
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.utils.TableConfigUtils;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.tools.RuleSets;
import org.junit.Test;

/** Test for {@link PushProjectIntoTableSourceScanRule}. */
public class PushProjectIntoTableSourceScanRuleTest
        extends PushProjectIntoLegacyTableSourceScanRuleTest {

    @Override
    public void setup() {
        util().buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE());
        CalciteConfig calciteConfig =
                TableConfigUtils.getCalciteConfig(util().tableEnv().getConfig());
        calciteConfig
                .getBatchProgram()
                .get()
                .addLast(
                        "rules",
                        FlinkHepRuleSetProgramBuilder.<BatchOptimizeContext>newBuilder()
                                .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE())
                                .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                                .add(RuleSets.ofList(PushProjectIntoTableSourceScanRule.INSTANCE))
                                .build());

        String ddl1 =
                "CREATE TABLE MyTable (\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util().tableEnv().executeSql(ddl1);

        String ddl2 =
                "CREATE TABLE VirtualTable (\n"
                        + "  a int,\n"
                        + "  b bigint,\n"
                        + "  c string,\n"
                        + "  d as a + 1\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util().tableEnv().executeSql(ddl2);

        String ddl3 =
                "CREATE TABLE NestedTable (\n"
                        + "  id int,\n"
                        + "  deepNested row<nested1 row<name string, `value` int>, nested2 row<num int, flag boolean>>,\n"
                        + "  nested row<name string, `value` int>,\n"
                        + "  `deepNestedWith.` row<`.value` int, nested row<name string, `.value` int>>,\n"
                        + "  name string,\n"
                        + "  testMap Map<string, string>\n"
                        + ") WITH (\n"
                        + " 'connector' = 'values',\n"
                        + " 'nested-projection-supported' = 'true',"
                        + " 'bounded' = 'true'\n"
                        + ")";
        util().tableEnv().executeSql(ddl3);

        String ddl4 =
                "CREATE TABLE MetadataTable(\n"
                        + "  id int,\n"
                        + "  deepNested row<nested1 row<name string, `value` int>, nested2 row<num int, flag boolean>>,\n"
                        + "  metadata_1 int metadata,\n"
                        + "  metadata_2 string metadata\n"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'nested-projection-supported' = 'true',"
                        + " 'bounded' = 'true',\n"
                        + " 'readable-metadata' = 'metadata_1:INT, metadata_2:STRING, metadata_3:BIGINT'"
                        + ")";
        util().tableEnv().executeSql(ddl4);

        String ddl5 =
                "CREATE TABLE UpsertTable("
                        + "  id int,\n"
                        + "  deepNested row<nested1 row<name string, `value` int>, nested2 row<num int, flag boolean>>,\n"
                        + "  metadata_1 int metadata,\n"
                        + "  metadata_2 string metadata,\n"
                        + "  PRIMARY KEY(id, deepNested) NOT ENFORCED"
                        + ") WITH ("
                        + "  'connector' = 'values',"
                        + "  'nested-projection-supported' = 'true',"
                        + "  'bounded' = 'false',\n"
                        + "  'changelod-mode' = 'I,UB,D',"
                        + " 'readable-metadata' = 'metadata_1:INT, metadata_2:STRING, metadata_3:BIGINT'"
                        + ")";
        util().tableEnv().executeSql(ddl5);
    }

    @Test
    public void testProjectWithMapType() {
        String sqlQuery = "SELECT id, testMap['e']\n" + "FROM NestedTable";
        util().verifyRelPlan(sqlQuery);
    }

    @Override
    @Test
    public void testNestedProject() {
        String sqlQuery =
                "SELECT id,\n"
                        + "    deepNested.nested1.name AS nestedName,\n"
                        + "    nested.`value` AS nestedValue,\n"
                        + "    deepNested.nested2.flag AS nestedFlag,\n"
                        + "    deepNested.nested2.num AS nestedNum\n"
                        + "FROM NestedTable";
        util().verifyRelPlan(sqlQuery);
    }

    @Test
    public void testComplicatedNestedProject() {
        String sqlQuery =
                "SELECT id,"
                        + "    deepNested.nested1.name AS nestedName,\n"
                        + "    (`deepNestedWith.`.`.value` + `deepNestedWith.`.nested.`.value`) AS nestedSum\n"
                        + "FROM NestedTable";
        util().verifyRelPlan(sqlQuery);
    }

    @Test
    public void testNestProjectWithMetadata() {
        String sqlQuery =
                "SELECT id,"
                        + "    deepNested.nested1 AS nested1,\n"
                        + "    deepNested.nested1.`value` + deepNested.nested2.num + metadata_1 as results\n"
                        + "FROM MetadataTable";

        util().verifyRelPlan(sqlQuery);
    }

    @Test
    public void testNestProjectWithUpsertSource() {
        String sqlQuery =
                "SELECT id,"
                        + "    deepNested.nested1 AS nested1,\n"
                        + "    deepNested.nested1.`value` + deepNested.nested2.num + metadata_1 as results\n"
                        + "FROM MetadataTable";

        util().verifyRelPlan(sqlQuery);
    }
}
