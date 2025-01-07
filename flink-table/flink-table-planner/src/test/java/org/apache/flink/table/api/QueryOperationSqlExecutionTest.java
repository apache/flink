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
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.test.program.TableApiTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.table.test.program.TableTestProgramRunner;
import org.apache.flink.table.test.program.TestStep.TestKind;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for executing results of {@link QueryOperation#asSerializableString()}. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(MiniClusterExtension.class)
public class QueryOperationSqlExecutionTest implements TableTestProgramRunner {

    @AfterEach
    protected void after() {
        TestValuesTableFactory.clearAllData();
    }

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
                QueryOperationTestPrograms.ACCESSING_NESTED_COLUMN);
    }

    @ParameterizedTest
    @MethodSource("supportedPrograms")
    void testSerializedSqlExecution(TableTestProgram program)
            throws ExecutionException, InterruptedException {
        final TableEnvironment env = setupEnv(program);

        final TableApiTestStep tableApiStep =
                (TableApiTestStep)
                        program.runSteps.stream()
                                .filter(s -> s instanceof TableApiTestStep)
                                .findFirst()
                                .get();

        final TableResult tableResult = tableApiStep.applyAsSql(env);
        tableResult.await();

        program.getSetupSinkTestSteps()
                .forEach(
                        s -> {
                            final List<String> expectedAsStrings = s.getExpectedAsStrings();
                            if (isAppendOnly(expectedAsStrings)) {
                                assertThat(TestValuesTableFactory.getResultsAsStrings(s.name))
                                        .containsExactlyInAnyOrderElementsOf(expectedAsStrings);
                            } else {
                                assertThat(TestValuesTableFactory.getRawResultsAsStrings(s.name))
                                        .containsExactlyInAnyOrderElementsOf(expectedAsStrings);
                            }
                        });
    }

    private boolean isAppendOnly(List<String> expectedAsStrings) {
        return expectedAsStrings.stream().allMatch(str -> str.startsWith("+I"));
    }

    private static TableEnvironment setupEnv(TableTestProgram program) {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        final Map<String, String> connectorOptions = new HashMap<>();
        connectorOptions.put("connector", "values");
        connectorOptions.put("sink-insert-only", "false");
        connectorOptions.put("runtime-source", "NewSource");
        program.getSetupSourceTestSteps()
                .forEach(
                        s -> {
                            final List<Row> data = new ArrayList<>(s.dataBeforeRestore);
                            data.addAll(s.dataAfterRestore);
                            final String id = TestValuesTableFactory.registerData(data);
                            connectorOptions.put("data-id", id);
                            s.apply(env, connectorOptions);
                        });
        program.getSetupSinkTestSteps().forEach(s -> s.apply(env, connectorOptions));
        program.getSetupFunctionTestSteps().forEach(f -> f.apply(env));
        return env;
    }

    @Override
    public EnumSet<TestKind> supportedSetupSteps() {
        return EnumSet.of(
                TestKind.FUNCTION,
                TestKind.SOURCE_WITH_DATA,
                TestKind.SOURCE_WITHOUT_DATA,
                TestKind.SOURCE_WITH_RESTORE_DATA, // restore data is ignored
                TestKind.SINK_WITH_DATA,
                TestKind.SINK_WITH_RESTORE_DATA // restore data is ignored
                );
    }

    @Override
    public EnumSet<TestKind> supportedRunSteps() {
        return EnumSet.of(TestKind.TABLE_API, TestKind.SQL);
    }
}
