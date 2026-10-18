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

package org.apache.flink.table.planner.runtime.stream;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.functions.python.PythonFunctionKind;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.AsyncPythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.ParallelAsyncPythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.ParallelPythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.ResourcePythonScalarFunction;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for Python UDF concurrency on streaming inputs. */
class PythonUdfExecutionOptionsTest extends TableTestBase {

    private StreamTableTestUtil util;

    @BeforeEach
    void setUp() {
        util = streamTestUtil(TableConfig.getDefault());
        util.getTableEnv()
                .getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE source_table (a INT, b INT) WITH ('connector' = 'values')");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE updating_source (a INT, b INT, PRIMARY KEY (a) NOT ENFORCED) "
                                + "WITH ('connector' = 'values', 'changelog-mode' = 'I,UA,D')");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE deleting_source (a INT, b INT) "
                                + "WITH ('connector' = 'values', 'changelog-mode' = 'I,D')");
        util.tableEnv()
                .executeSql(
                        "CREATE TABLE sink_table (a INT, b INT) "
                                + "WITH ('connector' = 'values', 'sink-insert-only' = 'false')");
    }

    @ParameterizedTest
    @CsvSource({
        "false, 2, updating_source",
        "true, 2, updating_source",
        "false, 4, updating_source",
        "false, 2, deleting_source",
        "true, 2, deleting_source"
    })
    void testRejectsExplicitConcurrencyOnChangelogInput(
            boolean async, int concurrency, String source) {
        util.addTemporarySystemFunction("parallel_func", function(async, concurrency));
        assertThatThrownBy(() -> translate("SELECT a, parallel_func(b, b) FROM " + source))
                .hasMessageContaining("Python UDF concurrency is only supported for insert-only")
                .hasMessageContaining("Omit concurrency");
    }

    @Test
    void testRejectsConcurrencyAfterAggregation() {
        util.addTemporarySystemFunction("parallel_func", function(false, 2));

        assertThatThrownBy(
                        () ->
                                translate(
                                        "SELECT a, parallel_func(b, b) FROM "
                                                + "(SELECT a, SUM(b) AS b FROM source_table GROUP BY a)"))
                .hasMessageContaining("Python UDF concurrency is only supported for insert-only");
    }

    @Test
    void testRejectsNestedExplicitConcurrency() {
        util.addTemporarySystemFunction("parallel_func", function(false, 2));
        util.addTemporarySystemFunction("default_func", new PythonScalarFunction("default_func"));

        assertThatThrownBy(
                        () ->
                                translate(
                                        "SELECT a, default_func(parallel_func(b, b), b) "
                                                + "FROM updating_source"))
                .hasMessageContaining("Python UDF concurrency is only supported for insert-only");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testUpdatingInputWithoutExplicitConcurrencyIsSupported(boolean async) {
        util.addTemporarySystemFunction(
                "default_func",
                async
                        ? new AsyncPythonScalarFunction("default_func")
                        : new PythonScalarFunction("default_func"));

        final Transformation<?> python =
                pythonTransformation(
                        translate("SELECT a, default_func(b, b) FROM updating_source"));
        assertThat(python.getParallelism()).isEqualTo(python.getInputs().get(0).getParallelism());
        assertThat(python.isParallelismConfigured()).isFalse();
    }

    @ParameterizedTest
    @CsvSource({"false, 2", "true, 2"})
    void testInsertOnlyInputWithExplicitConcurrencyIsSupported(boolean async, int concurrency) {
        util.addTemporarySystemFunction("parallel_func", function(async, concurrency));

        final Transformation<?> python =
                pythonTransformation(translate("SELECT a, parallel_func(b, b) FROM source_table"));
        assertThat(python.getParallelism()).isEqualTo(concurrency);
        assertThat(python.isParallelismConfigured()).isTrue();
    }

    @ParameterizedTest
    @EnumSource(
            value = PythonFunctionKind.class,
            names = {"PANDAS", "ARROW"})
    void testVectorizedConcurrency(PythonFunctionKind kind) {
        util.addTemporarySystemFunction(
                "vector", new ResourcePythonScalarFunction("vector", 3, kind, 32));
        final Transformation<?> python =
                pythonTransformation(translate("SELECT a, vector(a, b) FROM source_table"));
        assertThat(python.getParallelism()).isEqualTo(3);
        assertThat(python.isParallelismConfigured()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(strings = {"p1(a, b), p2(p1(a, b), b)", "p2(p1(a, b), b), p1(a, b)"})
    void testSharedDependencyIsEvaluatedBeforeItsConsumers(String projection) {
        util.addTemporarySystemFunction("p1", new ParallelPythonScalarFunction("p1", 1));
        util.addTemporarySystemFunction("p2", new ParallelPythonScalarFunction("p2", 2));

        assertThat(
                        translate("SELECT " + projection + " FROM source_table")
                                .get(0)
                                .getTransitivePredecessors()
                                .stream()
                                .filter(node -> node.getName().startsWith("Python")))
                .extracting(Transformation::getParallelism)
                .containsExactly(2, 1);
    }

    @Test
    void testUnspecifiedDependencyIsSharedAcrossConcurrencyGroups() {
        util.addTemporarySystemFunction("plain", new PythonScalarFunction("plain"));
        util.addTemporarySystemFunction("p1", new ParallelPythonScalarFunction("p1", 1));
        util.addTemporarySystemFunction("p2", new ParallelPythonScalarFunction("p2", 2));

        assertThat(
                        translate("SELECT p1(plain(a, b), b), p2(plain(a, b), b) FROM source_table")
                                .get(0)
                                .getTransitivePredecessors()
                                .stream()
                                .filter(node -> node.getDescription().contains("plain(")))
                .hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTemporalSortFollowsConcurrentPython(boolean projectRowtime) {
        util.addTemporarySystemFunction("f", new ParallelPythonScalarFunction("f", 2));
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE src (rt TIMESTAMP(3), v INT, "
                                + "WATERMARK FOR rt AS rt - INTERVAL '1' SECOND) "
                                + "WITH ('connector' = 'values')");
        util.getTableEnv()
                .executeSql(
                        "CREATE TABLE ordered_sink ("
                                + (projectRowtime ? "rt TIMESTAMP(3), " : "")
                                + "p INT) WITH ('connector' = 'values')");
        final Transformation<?> root =
                translate(
                                "SELECT "
                                        + (projectRowtime ? "rt, " : "")
                                        + "f(v, v) FROM src ORDER BY rt, v",
                                "ordered_sink")
                        .get(0);
        final Transformation<?> sort = find(root, "TemporalSort");
        final Transformation<?> python = find(root, "PythonCalc");
        assertThat(sort.getParallelism()).isEqualTo(1);
        assertThat(python.getParallelism()).isEqualTo(2);
        assertThat(python.isParallelismConfigured()).isTrue();
        assertThat(sort.getTransitivePredecessors()).contains(python);
        assertThat(python.getTransitivePredecessors()).doesNotContain(sort);
    }

    @Test
    void testTopNFollowsConcurrentPython() {
        util.addTemporarySystemFunction("f", new ParallelPythonScalarFunction("f", 2));
        final Transformation<?> root =
                translate("SELECT a, f(a, b) FROM source_table ORDER BY a DESC LIMIT 3").get(0);
        final Transformation<?> rank = find(root, "SortLimit");
        final Transformation<?> python = find(root, "PythonCalc");
        assertThat(python.getParallelism()).isEqualTo(2);
        assertThat(rank.getTransitivePredecessors()).contains(python);
    }

    private static Transformation<?> pythonTransformation(List<Transformation<?>> roots) {
        assertThat(roots).hasSize(1);
        return find(roots.get(0), "Python");
    }

    private static Transformation<?> find(Transformation<?> root, String name) {
        return root.getTransitivePredecessors().stream()
                .filter(node -> node.getName().startsWith(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Missing " + name + " in " + root));
    }

    private List<Transformation<?>> translate(String query) {
        return translate(query, "sink_table");
    }

    private List<Transformation<?>> translate(String query, String sink) {
        final List<Operation> operations =
                util.getPlanner().getParser().parse("INSERT INTO " + sink + " " + query);
        return util.getPlanner()
                .translate(Collections.singletonList((ModifyOperation) operations.get(0)));
    }

    private UserDefinedFunction function(boolean async, int concurrency) {
        return async
                ? new ParallelAsyncPythonScalarFunction("parallel_func", concurrency)
                : new ParallelPythonScalarFunction("parallel_func", concurrency);
    }
}
