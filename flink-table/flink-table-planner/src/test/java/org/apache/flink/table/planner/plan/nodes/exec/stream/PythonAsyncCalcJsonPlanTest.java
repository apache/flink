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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.AsyncPythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test json serialization/deserialization for Python Async Calc. */
class PythonAsyncCalcJsonPlanTest extends TableTestBase {

    private StreamTableTestUtil util;
    private TableEnvironment tEnv;

    @BeforeEach
    void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        tEnv = util.getTableEnv();

        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + "  a bigint,\n"
                        + "  b int not null,\n"
                        + "  c varchar,\n"
                        + "  d timestamp(3)\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        tEnv.executeSql(srcTableDdl);
    }

    @Test
    void testPythonAsyncCalc() {
        tEnv.createTemporaryFunction("asyncFunc", new AsyncPythonScalarFunction("asyncFunc"));
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b int\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan("insert into MySink select a, asyncFunc(b, b) from MyTable");
    }

    @Test
    void testPythonAsyncFunctionMixedWithJavaFunction() {
        tEnv.createTemporaryFunction("asyncFunc", new AsyncPythonScalarFunction("asyncFunc"));
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c int\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan("insert into MySink select a, asyncFunc(b, b), b + 1 from MyTable");
    }

    @Test
    void testMultiplePythonAsyncFunctions() {
        tEnv.createTemporaryFunction("asyncFunc1", new AsyncPythonScalarFunction("asyncFunc1"));
        tEnv.createTemporaryFunction("asyncFunc2", new AsyncPythonScalarFunction("asyncFunc2"));
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c int\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select a, asyncFunc1(b, b), asyncFunc2(b, b + 1) from MyTable");
    }

    @Test
    void testPythonAsyncFunctionInWhereClause() {
        tEnv.createTemporaryFunction("asyncFunc", new AsyncPythonScalarFunction("asyncFunc"));
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b int\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select a, b from MyTable where asyncFunc(b, b + 1) > 0");
    }

    @Test
    void testPythonAsyncFunctionMixedWithSyncPythonFunction() {
        tEnv.createTemporaryFunction("asyncFunc", new AsyncPythonScalarFunction("asyncFunc"));
        tEnv.createTemporaryFunction("pyFunc", new PythonScalarFunction("pyFunc"));
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b int,\n"
                        + "  c int\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select a, asyncFunc(b, b), pyFunc(b, b + 1) from MyTable");
    }

    @Test
    void testChainingPythonAsyncFunctions() {
        tEnv.createTemporaryFunction("asyncFunc1", new AsyncPythonScalarFunction("asyncFunc1"));
        tEnv.createTemporaryFunction("asyncFunc2", new AsyncPythonScalarFunction("asyncFunc2"));
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b int\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);
        util.verifyJsonPlan(
                "insert into MySink select a, asyncFunc2(asyncFunc1(b, b), b + 1) from MyTable");
    }
}
