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

package org.apache.flink.table.planner.runtime;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.functions.python.PythonFunctionKind;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.ParallelPythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.ResourcePythonScalarFunction;
import org.apache.flink.table.planner.utils.MockPythonTableFunction;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for scalar UDF splitting and concurrency in lateral table function calls. */
class PythonCorrelateConcurrencyTest extends TableTestBase {

    @ParameterizedTest
    @EnumSource(
            value = PythonFunctionKind.class,
            names = {"PANDAS", "ARROW"})
    void testSplitsFunctionKindBeforeConcurrency(PythonFunctionKind kind) {
        final TableTestUtil util = createUtil(true);
        util.addTemporarySystemFunction(
                "vector5", new ResourcePythonScalarFunction("vector5", 5, kind, -1));
        final List<Transformation<?>> operators =
                pythonOperators(translate(util, "f3(vector5(a, b), b)"));

        assertThat(operators).extracting(Transformation::getParallelism).containsExactly(5, 3);
        assertThat(operators).allMatch(Transformation::isParallelismConfigured);
        assertThat(operators.get(0).getName()).contains("PythonCalc");
        assertThat(operators.get(1).getName()).contains("PythonCorrelate");
    }

    @ParameterizedTest
    @EnumSource(
            value = PythonFunctionKind.class,
            names = {"PANDAS", "ARROW"})
    void testSplitsFunctionKindWithMatchingConcurrency(PythonFunctionKind kind) {
        final TableTestUtil util = createUtil(true);
        util.addTemporarySystemFunction(
                "vector3", new ResourcePythonScalarFunction("vector3", 3, kind, -1));
        final List<Transformation<?>> operators =
                pythonOperators(translate(util, "f3(vector3(a, b), b)"));

        assertThat(operators).extracting(Transformation::getParallelism).containsExactly(3, 3);
        assertThat(operators).allMatch(Transformation::isParallelismConfigured);
    }

    @ParameterizedTest
    @CsvSource({"false, PANDAS", "true, PANDAS", "false, ARROW", "true, ARROW"})
    void testSplitsFunctionKindWithoutConcurrency(boolean streaming, PythonFunctionKind kind) {
        final TableTestUtil util = createUtil(streaming);
        util.addTemporarySystemFunction(
                "vector", new ResourcePythonScalarFunction("vector", -1, kind, -1));
        final List<Transformation<?>> operators = pythonOperators(translate(util, "vector(a, b)"));

        assertThat(operators).extracting(Transformation::getParallelism).containsExactly(2, 2);
        assertThat(operators).noneMatch(Transformation::isParallelismConfigured);
    }

    @ParameterizedTest
    @EnumSource(
            value = PythonFunctionKind.class,
            names = {"PANDAS", "ARROW"})
    void testSplitsConcurrencyAfterFunctionKind(PythonFunctionKind kind) {
        final TableTestUtil util = createUtil(true);
        util.addTemporarySystemFunction(
                "vector3", new ResourcePythonScalarFunction("vector3", 3, kind, -1));
        final List<Transformation<?>> operators =
                pythonOperators(translate(util, "f3(f5(vector3(a, b), b), b)"));

        assertThat(operators).extracting(Transformation::getParallelism).containsExactly(3, 5, 3);
        assertThat(operators).allMatch(Transformation::isParallelismConfigured);
    }

    @Test
    void testSplitsNestedConflictingConcurrencies() {
        final TableTestUtil util = createUtil(true);
        final List<Transformation<?>> operators =
                pythonOperators(translate(util, "f3(f5(a, b), b)"));

        assertThat(operators).extracting(Transformation::getParallelism).containsExactly(5, 3);
        assertThat(operators).allMatch(Transformation::isParallelismConfigured);
        assertThat(operators.get(0).getName()).contains("PythonCalc");
        assertThat(operators.get(1).getName()).contains("PythonCorrelate");
    }

    @Test
    void testFusesCompatibleConcurrencies() {
        final TableTestUtil util = createUtil(true);
        for (String expression :
                new String[] {"f3(a, b)", "f3(f3(a, b), b)", "plain(f3(a, b), b)"}) {
            final List<Transformation<?>> operators = pythonOperators(translate(util, expression));
            assertThat(operators).as(expression).hasSize(1);
            assertThat(operators.get(0).getParallelism()).as(expression).isEqualTo(3);
            assertThat(operators.get(0).isParallelismConfigured()).as(expression).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testInheritsParallelismWithoutConcurrency(boolean streaming) {
        final List<Transformation<?>> operators =
                pythonOperators(translate(createUtil(streaming), "plain(a, b)"));
        assertThat(operators).hasSize(1);
        assertThat(operators.get(0).getParallelism()).isEqualTo(2);
        assertThat(operators.get(0).isParallelismConfigured()).isFalse();
    }

    @Test
    void testSplitsAlternatingConcurrencies() {
        final List<Transformation<?>> operators =
                pythonOperators(translate(createUtil(true), "f3(f5(f3(a, b), b), b)"));
        assertThat(operators).extracting(Transformation::getParallelism).containsExactly(3, 5, 3);
        assertThat(operators).allMatch(Transformation::isParallelismConfigured);
    }

    @Test
    void testSplitsConflictingArguments() {
        final List<Transformation<?>> operators =
                pythonOperators(
                        translateQuery(
                                createUtil(true),
                                "SELECT x, y FROM src, "
                                        + "LATERAL TABLE(tf(f3(a, b), f5(a, b))) AS T(x, y)"));
        assertThat(operators).extracting(Transformation::getParallelism).containsExactly(5, 3);
    }

    @ParameterizedTest
    @ValueSource(strings = {"I,UA,D", "I,D"})
    void testRejectsConcurrencyOnChangelogInput(String changelogMode) {
        final TableTestUtil util = createUtil(true);
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE changing_src(a INT, b INT, PRIMARY KEY (a) NOT ENFORCED) "
                                + "WITH ('connector' = 'values', 'changelog-mode' = '"
                                + changelogMode
                                + "')");
        assertThatThrownBy(
                        () ->
                                util.getTableEnv()
                                        .explainSql(
                                                "SELECT x, y FROM changing_src, "
                                                        + "LATERAL TABLE(tf(a, f3(a, b))) AS T(x, y)"))
                .hasMessageContaining("Python UDF concurrency is only supported for insert-only");
    }

    private TableTestUtil createUtil(boolean streaming) {
        final TableTestUtil util =
                streaming
                        ? streamTestUtil(TableConfig.getDefault())
                        : batchTestUtil(TableConfig.getDefault());
        util.getTableEnv()
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 2);
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE src(a INT, b INT) WITH ("
                                + "'connector' = 'filesystem', 'format' = 'testcsv', 'path' = '/tmp')");
        util.getTableEnv()
                .executeSql("CREATE TABLE sink_table(x INT, y INT) WITH ('connector' = 'values')");
        util.addTemporarySystemFunction("tf", new MockPythonTableFunction());
        util.addTemporarySystemFunction("f3", new ParallelPythonScalarFunction("f3", 3));
        util.addTemporarySystemFunction("f5", new ParallelPythonScalarFunction("f5", 5));
        util.addTemporarySystemFunction("plain", new PythonScalarFunction("plain"));
        return util;
    }

    private Transformation<?> translate(TableTestUtil util, String expression) {
        return translateQuery(
                util, "SELECT x, y FROM src, LATERAL TABLE(tf(a, " + expression + ")) AS T(x, y)");
    }

    private Transformation<?> translateQuery(TableTestUtil util, String query) {
        final List<Operation> operations =
                util.getPlanner().getParser().parse("INSERT INTO sink_table " + query);
        final List<Transformation<?>> transformations =
                util.getPlanner()
                        .translate(Collections.singletonList((ModifyOperation) operations.get(0)));
        assertThat(transformations).hasSize(1);
        return transformations.get(0);
    }

    private List<Transformation<?>> pythonOperators(Transformation<?> root) {
        final List<Transformation<?>> operators = new ArrayList<>();
        for (Transformation<?> input : root.getInputs()) {
            operators.addAll(pythonOperators(input));
        }
        if (root.getName().contains("Python")) {
            operators.add(root);
        }
        return operators;
    }
}
