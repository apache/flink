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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.ResourceRowPythonScalarFunction;
import org.apache.flink.table.planner.utils.JavaTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.apache.flink.table.api.Expressions.withColumns;

/** Tests fusion of vectorized maps with execution options. */
class PythonMapExecutionOptionsTest extends TableTestBase {
    private JavaTableTestUtil util;
    private Table source;

    @BeforeEach
    void setUp() {
        util = javaStreamTestUtil();
        util.addTableSource("T", Schema.newBuilder().column("a", DataTypes.INT()).build());
        source = util.tableEnv().from("T");
        util.tableEnv()
                .createTemporarySystemFunction(
                        "plain", new ResourceRowPythonScalarFunction("plain", -1, -1));
    }

    @Test
    void testCompatibleExecutionOptionsAreMerged() {
        util.tableEnv()
                .createTemporarySystemFunction(
                        "explicit", new ResourceRowPythonScalarFunction("explicit", 2, 64));
        util.verifyRelPlan(
                source.map(call("explicit", withColumns($("*"))))
                        .map(call("plain", withColumns($("*"))))
                        .map(call("explicit", withColumns($("*")))));
    }

    @Test
    void testNestedConflictingConcurrencyIsNotMerged() {
        util.tableEnv()
                .createTemporarySystemFunction(
                        "p2", new ResourceRowPythonScalarFunction("p2", 2, -1));
        util.tableEnv()
                .createTemporarySystemFunction(
                        "p4", new ResourceRowPythonScalarFunction("p4", 4, -1));
        util.verifyRelPlan(
                source.map(call("p2", withColumns($("*"))))
                        .map(call("plain", withColumns($("*"))))
                        .map(call("p4", withColumns($("*")))));
    }

    @Test
    void testConflictingBatchSizesAreNotMerged() {
        util.tableEnv()
                .createTemporarySystemFunction(
                        "batch64", new ResourceRowPythonScalarFunction("batch64", -1, 64));
        util.tableEnv()
                .createTemporarySystemFunction(
                        "batch128", new ResourceRowPythonScalarFunction("batch128", -1, 128));
        util.verifyRelPlan(
                source.map(call("batch64", withColumns($("*"))))
                        .map(call("batch128", withColumns($("*")))));
    }
}
