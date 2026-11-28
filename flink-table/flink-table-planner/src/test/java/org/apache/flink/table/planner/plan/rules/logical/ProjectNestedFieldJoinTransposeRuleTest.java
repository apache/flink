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
import org.apache.flink.table.planner.plan.optimize.program.FlinkHepRuleSetProgramBuilder;
import org.apache.flink.table.planner.plan.optimize.program.FlinkStreamProgram;
import org.apache.flink.table.planner.plan.optimize.program.HEP_RULES_EXECUTION_TYPE;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableConfigUtils;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.tools.RuleSets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link ProjectNestedFieldJoinTransposeRule}. */
class ProjectNestedFieldJoinTransposeRuleTest extends TableTestBase {

    private final StreamTableTestUtil util = streamTestUtil(TableConfig.getDefault());

    @BeforeEach
    void setup() {
        util.buildStreamProgram(FlinkStreamProgram.PROJECT_REWRITE());
        TableConfigUtils.getCalciteConfig(util.tableEnv().getConfig())
                .getStreamProgram()
                .get()
                .addLast(
                        "rules",
                        FlinkHepRuleSetProgramBuilder.<StreamOptimizeContext>newBuilder()
                                // As in production, so that the rules can merge the projects that
                                // Calcite adds around a join condition before the nested accesses
                                // are pushed.
                                .setHepRulesExecutionType(
                                        HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION())
                                .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
                                .add(
                                        RuleSets.ofList(
                                                ProjectNestedFieldJoinTransposeRule.INSTANCE,
                                                FlinkProjectMergeRule.INSTANCE,
                                                CoreRules.PROJECT_REMOVE,
                                                // folds IS NULL on a NOT NULL reference, which
                                                // exposes wrongly typed conditions
                                                SimplifyJoinConditionRule.INSTANCE))
                                .build());
        createTable("L", "id int, nested row<name string, `value` int>");
        createTable("R", "id int, info row<city string, zip int>");
        createTable("S", "id int, extra row<tag string, n int>");
        // NOT NULL leaves in a NOT NULL struct, and in a nullable struct
        createTable("RNN", "id int not null, info row<city string not null, zip int> not null");
        createTable("LNN", "id int, nested row<v int not null, w string>");
        // a struct that is used whole and read deeper
        createTable(
                "RA",
                "id int, info row<addr row<zip int not null, st string> not null, other int>");
        createTable("D", "id int, deep row<l1 row<l2 row<l3 int, x int>, y int>, z int>");
    }

    private void createTable(String name, String columns) {
        util.tableEnv()
                .executeSql(
                        String.format(
                                "CREATE TABLE %s (%s) WITH ('connector' = 'values', 'bounded' = 'false')",
                                name, columns));
    }

    @Test
    void testInnerEquiJoinSingleLeaf() {
        // the structs must not cross the join whole
        util.verifyRelPlan("SELECT L.nested.name, R.info.city FROM L JOIN R ON L.id = R.id");
    }

    @Test
    void testSelfJoin() {
        // both sides have the same schema, which exercises the offset of the right side
        util.verifyRelPlan(
                "SELECT a.nested.name, b.nested.name FROM L a "
                        + "JOIN L b ON a.nested.`value` = b.nested.`value`");
    }

    @Test
    void testNonEquiNestedCondition() {
        util.verifyRelPlan(
                "SELECT L.nested.name FROM L JOIN R "
                        + "ON L.id = R.id AND L.nested.`value` > R.info.zip");
    }

    @Test
    void testLeftOuterJoinNotNullNestedLeafWidensToNullable() {
        // The golden file does not show nullability. The test relies on the planner's type check,
        // which fails if the rewritten project keeps NOT NULL above the outer join. The
        // nullability is asserted explicitly in NestedProjectionUtilJoinTest.
        util.verifyRelPlanWithType(
                "SELECT L.nested.name, RNN.info.city FROM L LEFT JOIN RNN ON L.id = RNN.id");
    }

    @Test
    void testFullOuterJoinNotNullNestedLeafInCondition() {
        util.verifyRelPlan(
                "SELECT L.nested.name FROM L FULL JOIN RNN ON L.nested.name = RNN.info.city");
    }

    @Test
    void testNotNullLeafOfNullableStructInCondition() {
        // the leaf is nullable through its nullable struct, so IS NULL must not be folded to false
        util.verifyRelPlan(
                "SELECT LNN.id, R.id FROM LNN LEFT JOIN R "
                        + "ON LNN.id = R.id AND LNN.nested.v IS NULL");
    }

    @Test
    void testStructLeafUsedWholeAndAccessedDeeper() {
        util.verifyRelPlan(
                "SELECT L.id, RA.info.addr, RA.info.addr.zip FROM L JOIN RA ON L.id = RA.id");
    }

    @Test
    void testDeeplyNestedLeavesAtDifferentDepths() {
        util.verifyRelPlan(
                "SELECT D.deep.l1.l2.l3, D.deep.l1.y, D.deep.z, R.info.city "
                        + "FROM D JOIN R ON D.id = R.id");
    }

    @Test
    void testThreeWayJoin() {
        util.verifyRelPlan(
                "SELECT L.nested.name, R.info.city, S.extra.tag FROM L "
                        + "JOIN R ON L.id = R.id JOIN S ON R.id = S.id");
    }

    @Test
    void testAllLeavesUsedIsNotTransposed() {
        // `deep.l1` is used whole and `deep.z` is the only other leaf: nothing to prune
        util.verifyRelPlan("SELECT D.deep.l1, D.deep.z FROM D JOIN R ON D.id = R.id");
    }

    @Test
    void testSemiJoinNotTransposed() {
        util.verifyRelPlan("SELECT L.nested.name FROM L WHERE L.id IN (SELECT R.id FROM R)");
    }
}
