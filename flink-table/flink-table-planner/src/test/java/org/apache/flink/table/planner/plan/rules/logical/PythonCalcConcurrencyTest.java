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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.functions.python.PythonFunctionKind;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.ParallelPythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.ResourcePythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.ResourceRowPythonScalarFunction;
import org.apache.flink.table.planner.utils.JavaTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Plans concurrency groups and ordering around Python projections. */
class PythonCalcConcurrencyTest extends TableTestBase {
    private JavaTableTestUtil util;

    @BeforeEach
    void setUp() {
        util = javaStreamTestUtil();
        util.addTableSource(
                "T",
                Schema.newBuilder()
                        .column("a", DataTypes.INT())
                        .column("b", DataTypes.INT())
                        .column("c", DataTypes.INT())
                        .build());
        util.tableEnv().createTemporarySystemFunction("plain", new PythonScalarFunction("plain"));
        util.tableEnv()
                .createTemporarySystemFunction("p1", new ParallelPythonScalarFunction("p1", 1));
        util.tableEnv()
                .createTemporarySystemFunction("p2", new ParallelPythonScalarFunction("p2", 2));
        util.tableEnv()
                .createTemporarySystemFunction("p4", new ParallelPythonScalarFunction("p4", 4));
    }

    @Test
    void testDifferentConcurrencySplitsCalc() {
        util.verifyRelPlan("SELECT p2(a, b), p4(a, c) FROM T");
    }

    @Test
    void testSameAndUnspecifiedConcurrencyStayTogether() {
        util.verifyRelPlan("SELECT p2(a, b), p2(a, c), plain(a, b) FROM T");
    }

    @Test
    void testNestedDifferentConcurrencySplitsDeterministically() {
        util.verifyRelPlan("SELECT p2(a, p1(a, c)) FROM T");
    }

    @Test
    void testUnspecifiedOuterWithDifferentInnerConcurrencies() {
        util.verifyRelPlan("SELECT plain(p1(a, b), p2(a, c)) FROM T");
    }

    @Test
    void testNestedConcurrencyRemainsBeforeTopN() {
        util.verifyRelPlan("SELECT a, plain(p2(a, b), b) + 1 FROM T ORDER BY a DESC LIMIT 3");
    }

    @Test
    void testRowFieldConcurrencyRemainsBeforeTopN() {
        util.tableEnv()
                .createTemporarySystemFunction(
                        "row_func", new ResourceRowPythonScalarFunction("row_func", 2, -1));
        util.verifyRelPlan("SELECT a, row_func(a).f0 FROM T ORDER BY a DESC LIMIT 3");
    }

    @Test
    void testUnspecifiedConcurrencyAllowsTopNPushdown() {
        util.verifyRelPlan("SELECT a, plain(a, b) FROM T ORDER BY a DESC LIMIT 3");
    }

    @Test
    void testBatchSizeAllowsTopNPushdown() {
        util.tableEnv()
                .createTemporarySystemFunction(
                        "vector",
                        new ResourcePythonScalarFunction(
                                "vector", -1, PythonFunctionKind.PANDAS, 32));
        util.verifyRelPlan("SELECT a, vector(a, b) FROM T ORDER BY a DESC LIMIT 3");
    }

    @Test
    void testUnorderedLimitAllowsPushdown() {
        util.verifyRelPlan("SELECT a, p2(a, b) FROM T LIMIT 3 OFFSET 1");
    }

    @Test
    void testKeyedAggregationRepartitionsAfterPython() {
        util.verifyRelPlan("SELECT a, SUM(v) FROM (SELECT a, p2(b, c) AS v FROM T) GROUP BY a");
    }
}
