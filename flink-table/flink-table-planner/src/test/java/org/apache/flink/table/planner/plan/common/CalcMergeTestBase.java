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

package org.apache.flink.table.planner.plan.common;

import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.Before;
import org.junit.Test;

/**
 * Base plan test for calc merge, the difference between FlinkCalcMergeRuleTest is this test
 * includes all rules.
 */
public abstract class CalcMergeTestBase extends TableTestBase {

    private TableTestUtil util;

    protected abstract boolean isBatchMode();

    protected abstract TableTestUtil getTableTestUtil();

    @Before
    public void setup() {
        util = getTableTestUtil();

        util.tableEnv()
                .executeSql(
                        "CREATE TABLE MyTable (\n"
                                + " a INTEGER,\n"
                                + " b INTEGER,\n"
                                + " c VARCHAR\n"
                                + ") WITH (\n"
                                + " 'connector' = 'values'\n"
                                + " ,'bounded' = '"
                                + isBatchMode()
                                + "'\n"
                                + ")");
        util.addFunction("random_udf", new JavaUserDefinedScalarFunctions.NonDeterministicUdf());
    }

    @Test
    public void testCalcMergeWithSameDigest() {
        util.verifyExecPlan("SELECT a, b FROM (SELECT * FROM MyTable WHERE a = b) t WHERE b = a");
    }

    @Test
    public void testCalcMergeWithNonDeterministicExpr1() {
        util.verifyExecPlan(
                "SELECT a, a1 FROM (SELECT a, random_udf(a) AS a1 FROM MyTable) t WHERE a1 > 10");
    }

    @Test
    public void testCalcMergeWithNonDeterministicExpr2() {
        util.verifyExecPlan(
                "SELECT random_udf(a1) as a2 FROM (SELECT random_udf(a) as"
                        + " a1, b FROM MyTable) t WHERE b > 10");
    }

    @Test
    public void testCalcMergeWithTopMultiNonDeterministicExpr() {
        util.verifyExecPlan(
                "SELECT random_udf(a1) as a2, random_udf(a1) as a3 FROM"
                        + " (SELECT random_udf(a) as a1, b FROM MyTable) t WHERE b > 10");
    }

    @Test
    public void testCalcMergeTopFilterHasNonDeterministicExpr() {
        util.verifyExecPlan(
                "SELECT a, c FROM"
                        + " (SELECT a, random_udf(b) as b1, c FROM MyTable) t WHERE b1 > 10");
    }

    @Test
    public void testCalcMergeWithBottomMultiNonDeterministicExpr() {
        util.verifyExecPlan(
                "SELECT a1, b2 FROM"
                        + " (SELECT random_udf(a) as a1, random_udf(b) as b2, c FROM MyTable) t WHERE c > 10");
    }

    @Test
    public void testCalcMergeWithBottomMultiNonDeterministicInConditionExpr() {
        util.verifyExecPlan(
                "SELECT c FROM"
                        + " (SELECT random_udf(a) as a1, random_udf(b) as b2, c FROM MyTable) t WHERE a1 > b2");
    }

    @Test
    public void testCalcMergeWithoutInnerNonDeterministicExpr() {
        util.verifyExecPlan(
                "SELECT a, c FROM (SELECT a, random_udf(a) as a1, c FROM MyTable) t WHERE c > 10");
    }

    @Test
    public void testCalcMergeWithNonDeterministicNestedExpr() {
        util.verifyExecPlan(
                "SELECT a, a1 FROM (SELECT a, substr(cast(random_udf(a) as varchar), 1, 2) AS a1 FROM MyTable) t WHERE a1 > '10'");
    }
}
