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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.operations.CollectModifyOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.test.program.SqlTestStep;
import org.apache.flink.table.test.program.TableApiTestStep;
import org.apache.flink.table.test.program.TableTestProgram;
import org.apache.flink.table.test.program.TableTestProgramRunner;
import org.apache.flink.table.test.program.TestStep.TestKind;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for serialization of {@link org.apache.flink.table.operations.QueryOperation}. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class QueryOperationSqlSerializationTest implements TableTestProgramRunner {

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
                QueryOperationTestPrograms.SQL_QUERY_OPERATION,
                QueryOperationTestPrograms.OVER_WINDOW_RANGE,
                QueryOperationTestPrograms.OVER_WINDOW_ROWS,
                QueryOperationTestPrograms.OVER_WINDOW_ROWS_UNBOUNDED_NO_PARTITION,
                QueryOperationTestPrograms.OVER_WINDOW_LAG,
                QueryOperationTestPrograms.ACCESSING_NESTED_COLUMN);
    }

    @ParameterizedTest
    @MethodSource("supportedPrograms")
    void testSqlSerialization(TableTestProgram program) {
        final TableEnvironment env = setupEnv(program);

        final TableApiTestStep tableApiStep =
                (TableApiTestStep)
                        program.runSteps.stream()
                                .filter(s -> s instanceof TableApiTestStep)
                                .findFirst()
                                .get();

        final SqlTestStep sqlStep =
                (SqlTestStep)
                        program.runSteps.stream()
                                .filter(s -> s instanceof SqlTestStep)
                                .findFirst()
                                .get();
        final Table table = tableApiStep.toTable(env);
        assertThat(table.getQueryOperation().asSerializableString()).isEqualTo(sqlStep.sql);
    }

    @ParameterizedTest
    @MethodSource("supportedPrograms")
    void testSqlAsJobNameForQueryOperation(TableTestProgram program) {
        final TableEnvironmentImpl env = (TableEnvironmentImpl) setupEnv(program);

        final TableApiTestStep tableApiStep =
                (TableApiTestStep)
                        program.runSteps.stream()
                                .filter(s -> s instanceof TableApiTestStep)
                                .findFirst()
                                .get();

        final SqlTestStep sqlStep =
                (SqlTestStep)
                        program.runSteps.stream()
                                .filter(s -> s instanceof SqlTestStep)
                                .findFirst()
                                .get();

        final Table table = tableApiStep.toTable(env);

        QueryOperation queryOperation = table.getQueryOperation();
        CollectModifyOperation sinkOperation = new CollectModifyOperation(queryOperation);
        List<Transformation<?>> transformations =
                env.getPlanner().translate(Collections.singletonList(sinkOperation));

        StreamGraph streamGraph =
                (StreamGraph)
                        env.generatePipelineFromQueryOperation(queryOperation, transformations);

        assertThat(streamGraph.getJobName()).isEqualTo(sqlStep.sql);
    }

    private static TableEnvironment setupEnv(TableTestProgram program) {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        final Map<String, String> connectorOptions = new HashMap<>();
        connectorOptions.put("connector", "values");
        program.getSetupSourceTestSteps().forEach(s -> s.apply(env, connectorOptions));
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
