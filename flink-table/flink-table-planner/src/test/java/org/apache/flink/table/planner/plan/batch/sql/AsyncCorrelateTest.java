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

package org.apache.flink.table.planner.plan.batch.sql;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalAsyncCorrelate;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions;
import org.apache.flink.table.planner.utils.BatchTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BatchPhysicalAsyncCorrelate}. */
class AsyncCorrelateTest extends TableTestBase {

    private BatchTableTestUtil util;

    @BeforeEach
    void setup() {
        util = batchTestUtil(TableConfig.getDefault());
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE MyTable (\n"
                                + "  a INT NOT NULL,\n"
                                + "  b BIGINT,\n"
                                + "  c STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'test-simple-table-source'\n"
                                + ")");
        util.addTemporarySystemFunction(
                "asyncTableFunc", new JavaUserDefinedTableFunctions.AsyncTestTableFunction());
        util.addTemporarySystemFunction(
                "mySum", new JavaUserDefinedTableFunctions.SumScalarFunction());
    }

    @Test
    void testAsyncCorrelate() {
        util.verifyExecPlan("SELECT * FROM MyTable, LATERAL asyncTableFunc(a)");
    }

    @Test
    void testAsyncCorrelateLeftJoin() {
        util.verifyExecPlan("SELECT * FROM MyTable LEFT JOIN LATERAL asyncTableFunc(a) ON TRUE");
    }

    @Test
    void testAsyncCorrelateWithScalarArgument() {
        util.verifyExecPlan("SELECT * FROM MyTable, LATERAL asyncTableFunc(mySum(a, 10))");
    }

    @Test
    void testAsyncCorrelateWithFilterInLeftSide() {
        util.verifyExecPlan(
                "SELECT c, s FROM MyTable, LATERAL asyncTableFunc(a) AS T(s) WHERE a > 1");
    }

    @Test
    void testAsyncCorrelateIsNotMergedIntoMultipleInput() {
        util.verifyExecPlan(
                "SELECT t1.a, t1.s, t2.b FROM "
                        + "(SELECT a, s FROM (SELECT a FROM MyTable GROUP BY a), "
                        + "LATERAL asyncTableFunc(a) AS T(s)) t1 "
                        + "JOIN MyTable t2 ON t1.a = t2.a");
    }

    @Test
    void testConstantAsyncTableFunction() {
        util.verifyExecPlan("SELECT * FROM asyncTableFunc(1)");
    }

    @Test
    void testAsyncCorrelateWithRightSelectStar() {
        util.verifyExecPlan(
                "SELECT * FROM MyTable, LATERAL (SELECT * FROM asyncTableFunc(a) AS T(s))");
    }

    @Test
    void testAsyncCorrelateWithRightConditionIsNotSupported() {
        assertThatThrownBy(
                        () ->
                                util.verifyExecPlan(
                                        "SELECT * FROM MyTable, LATERAL (SELECT * FROM "
                                                + "asyncTableFunc(a) AS T(s) WHERE CHAR_LENGTH(s) > 0)"))
                .isInstanceOf(TableException.class)
                .hasMessage(
                        "Currently Async correlate does not support projections or conditions.");
    }
}
