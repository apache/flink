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

package org.apache.flink.table.api;

import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.planner.plan.nodes.exec.testutils.SemanticTestBase;
import org.apache.flink.table.test.program.TableApiTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.table.test.program.TestStep;
import org.apache.flink.table.test.program.TestStep.TestKind;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

/** Tests for executing results of {@link QueryOperation#asSerializableString()}. */
public class QueryOperationSqlSemanticTest extends SemanticTestBase {

    @Override
    public List<TableTestProgram> programs() {
        return Arrays.asList(
                QueryOperationTestPrograms.SOURCE_QUERY_OPERATION,
                QueryOperationTestPrograms.VALUES_QUERY_OPERATION,
                QueryOperationTestPrograms.FILTER_QUERY_OPERATION,
                QueryOperationTestPrograms.AGGREGATE_QUERY_OPERATION,
                QueryOperationTestPrograms.AGGREGATE_NO_GROUP_BY_QUERY_OPERATION,
                QueryOperationTestPrograms.DISTINCT_QUERY_OPERATION,
                QueryOperationTestPrograms.JOIN_QUERY_OPERATION,
                QueryOperationTestPrograms.ORDER_BY_QUERY_OPERATION,
                QueryOperationTestPrograms.WINDOW_AGGREGATE_QUERY_OPERATION,
                QueryOperationTestPrograms.UNION_ALL_QUERY_OPERATION,
                QueryOperationTestPrograms.LATERAL_JOIN_QUERY_OPERATION,
                QueryOperationTestPrograms.GROUP_HOP_WINDOW_EVENT_TIME,
                QueryOperationTestPrograms.SORT_LIMIT_DESC,
                QueryOperationTestPrograms.GROUP_BY_UDF_WITH_MERGE,
                QueryOperationTestPrograms.NON_WINDOW_INNER_JOIN,
                QueryOperationTestPrograms.SQL_QUERY_OPERATION,
                QueryOperationTestPrograms.OVER_WINDOW_RANGE,
                QueryOperationTestPrograms.OVER_WINDOW_ROWS,
                QueryOperationTestPrograms.OVER_WINDOW_ROWS_UNBOUNDED_NO_PARTITION,
                QueryOperationTestPrograms.OVER_WINDOW_LAG,
                QueryOperationTestPrograms.ACCESSING_NESTED_COLUMN,
                QueryOperationTestPrograms.TABLE_AS_ROW_PTF,
                QueryOperationTestPrograms.TABLE_AS_SET_PTF);
    }

    @Override
    protected void runStep(TestStep testStep, TableEnvironment env) throws Exception {
        if (testStep instanceof TableApiTestStep) {
            final TableApiTestStep tableApiStep = (TableApiTestStep) testStep;
            tableApiStep.applyAsSql(env).await();
        } else {
            super.runStep(testStep, env);
        }
    }

    @Override
    public EnumSet<TestKind> supportedRunSteps() {
        return EnumSet.of(TestKind.TABLE_API, TestKind.SQL);
    }
}
