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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;

/** Tests for {@link WrapJsonAggFunctionArgumentsRule}. */
@ExtendWith(ParameterizedTestExtension.class)
class WrapJsonAggFunctionArgumentsRuleTest extends TableTestBase {

    private final boolean batchMode;
    private TableTestUtil util;

    @Parameters(name = "batchMode = {0}")
    private static Collection<Boolean> data() {
        return Arrays.asList(true, false);
    }

    WrapJsonAggFunctionArgumentsRuleTest(boolean batchMode) {
        this.batchMode = batchMode;
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

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE T1(\n"
                                + " f0 INTEGER,\n"
                                + " f1 VARCHAR,\n"
                                + " f2 BIGINT\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values'\n"
                                + " ,'bounded' = '"
                                + batchMode
                                + "'\n)");
    }

    @TestTemplate
    void testJsonObjectAgg() {
        util.verifyRelPlan("SELECT JSON_OBJECTAGG(f1 VALUE f1) FROM T");
    }

    @TestTemplate
    void testJsonObjectAggInGroupWindow() {
        util.verifyRelPlan("SELECT f0, JSON_OBJECTAGG(f1 VALUE f0) FROM T GROUP BY f0");
    }

    @TestTemplate
    void testJsonArrayAgg() {
        util.verifyRelPlan("SELECT JSON_ARRAYAGG(f0) FROM T");
    }

    @TestTemplate
    void testJsonArrayAggInGroupWindow() {
        util.verifyRelPlan("SELECT f0, JSON_ARRAYAGG(f0) FROM T GROUP BY f0");
    }

    @TestTemplate
    void testJsonObjectAggWithOtherAggs() {
        util.verifyRelPlan("SELECT COUNT(*), JSON_OBJECTAGG(f1 VALUE f1) FROM T");
    }

    @TestTemplate
    void testGroupJsonObjectAggWithOtherAggs() {
        util.verifyRelPlan(
                "SELECT f0, COUNT(*), JSON_OBJECTAGG(f1 VALUE f0), SUM(f2) FROM T GROUP BY f0");
    }

    @TestTemplate
    void testJsonArrayAggWithOtherAggs() {
        util.verifyRelPlan("SELECT COUNT(*), JSON_ARRAYAGG(f0) FROM T");
    }

    @TestTemplate
    void testGroupJsonArrayAggInWithOtherAggs() {
        util.verifyRelPlan("SELECT f0, COUNT(*), JSON_ARRAYAGG(f0), SUM(f2) FROM T GROUP BY f0");
    }

    @TestTemplate
    void testJsonArrayAggAndJsonObjectAggWithOtherAggs() {
        util.verifyRelPlan(
                "SELECT MAX(f0), JSON_OBJECTAGG(f1 VALUE f0), JSON_ARRAYAGG(f1), JSON_ARRAYAGG(f0) FROM T");
    }

    @TestTemplate
    void testGroupJsonArrayAggAndJsonObjectAggWithOtherAggs() {
        util.verifyRelPlan(
                "SELECT f0, JSON_OBJECTAGG(f1 VALUE f2), JSON_ARRAYAGG(f1), JSON_ARRAYAGG(f2),"
                        + " SUM(f2) FROM T GROUP BY f0");
    }
}
