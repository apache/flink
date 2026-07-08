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
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Tests for {@link WrapJsonAggFunctionArgumentsRule}. */
@ExtendWith(ParameterizedTestExtension.class)
class WrapJsonAggFunctionArgumentsRuleTest extends TableTestBase {

    private final boolean batchMode;
    private final boolean isGroupWindowAgg;
    private TableTestUtil util;

    @Parameters(name = "batchMode = {0}, isGroupWindowAgg = {1}")
    private static List<Boolean[]> data() {
        return Arrays.asList(
                new Boolean[] {true, false},
                new Boolean[] {true, true},
                new Boolean[] {false, false},
                new Boolean[] {false, true});
    }

    WrapJsonAggFunctionArgumentsRuleTest(boolean batchMode, boolean isGroupWindowAgg) {
        this.batchMode = batchMode;
        this.isGroupWindowAgg = isGroupWindowAgg;
    }

    @BeforeEach
    void setup() {
        if (batchMode) {
            util = batchTestUtil(TableConfig.getDefault());
        } else {
            util = streamTestUtil(TableConfig.getDefault());
        }

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE T(\n"
                                + " f0 INTEGER,\n"
                                + " f1 VARCHAR,\n"
                                + " f2 BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values'\n"
                                + " ,'bounded' = '"
                                + batchMode
                                + "'\n)");

        if (isGroupWindowAgg) {
            util.tableEnv()
                    .executeSql(
                            "ALTER TABLE T add (rt TIMESTAMP(3), WATERMARK FOR rt as rt - interval '1' second)");
        }
    }

    @TestTemplate
    void testJsonObjectAgg() {
        util.verifyRelPlan(renderAggregateSQL("JSON_OBJECTAGG(f1 VALUE f1)"));
    }

    @TestTemplate
    void testJsonObjectAggInGroupWindow() {
        util.verifyRelPlan(renderAggregateSQL("f0, JSON_OBJECTAGG(f1 VALUE f0)", "f0"));
    }

    @TestTemplate
    void testJsonArrayAgg() {
        util.verifyRelPlan(renderAggregateSQL("JSON_ARRAYAGG(f0)"));
    }

    @TestTemplate
    void testJsonArrayAggInGroupWindow() {
        util.verifyRelPlan(renderAggregateSQL("f0, JSON_ARRAYAGG(f0)", "f0"));
    }

    @TestTemplate
    void testJsonObjectAggWithOtherAggs() {
        util.verifyRelPlan(renderAggregateSQL("COUNT(*), JSON_OBJECTAGG(f1 VALUE f1)"));
    }

    @TestTemplate
    void testGroupJsonObjectAggWithOtherAggs() {
        util.verifyRelPlan(
                renderAggregateSQL("f0, COUNT(*), JSON_OBJECTAGG(f1 VALUE f0), SUM(f2)", "f0"));
    }

    @TestTemplate
    void testJsonArrayAggWithOtherAggs() {
        util.verifyRelPlan(renderAggregateSQL("COUNT(*), JSON_ARRAYAGG(f0)"));
    }

    @TestTemplate
    void testGroupJsonArrayAggInWithOtherAggs() {
        util.verifyRelPlan(renderAggregateSQL("f0, COUNT(*), JSON_ARRAYAGG(f0), SUM(f2)", "f0"));
    }

    @TestTemplate
    void testJsonArrayAggAndJsonObjectAggWithOtherAggs() {
        util.verifyRelPlan(
                renderAggregateSQL(
                        "MAX(f0), JSON_OBJECTAGG(f1 VALUE f0), JSON_ARRAYAGG(f1), JSON_ARRAYAGG(f0)"));
    }

    @TestTemplate
    void testGroupJsonArrayAggAndJsonObjectAggWithOtherAggs() {
        util.verifyRelPlan(
                renderAggregateSQL(
                        "f0, JSON_OBJECTAGG(f1 VALUE f2), JSON_ARRAYAGG(f1), JSON_ARRAYAGG(f2),"
                                + " SUM(f2)",
                        "f0"));
    }

    @TestTemplate
    void testJsonObjectAggWithWindowTVF() {
        Assumptions.assumeTrue(isGroupWindowAgg);
        util.verifyRelPlan(
                "SELECT JSON_OBJECTAGG(f1 VALUE f1) "
                        + "FROM TABLE(TUMBLE(TABLE T, DESCRIPTOR(rt), INTERVAL '5' SECOND))");
    }

    private String renderAggregateSQL(String projects, String... groupKeys) {
        List<String> newGroupKeys = new ArrayList<>(Arrays.asList(groupKeys));
        if (isGroupWindowAgg) {
            newGroupKeys.add("TUMBLE(rt, INTERVAL '5' SECOND)");
        }
        String groupKeyStr;
        if (newGroupKeys.isEmpty()) {
            groupKeyStr = "";
        } else {
            groupKeyStr = " GROUP BY " + String.join(",", newGroupKeys);
        }
        return String.format("SELECT %s FROM T%s", projects, groupKeyStr);
    }
}
