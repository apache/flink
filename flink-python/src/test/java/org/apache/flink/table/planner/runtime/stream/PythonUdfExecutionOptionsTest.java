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
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.AsyncPythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.ParallelPythonScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for Python UDF concurrency on streaming changelog inputs. */
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
    @CsvSource({"false, 2", "false, 4", "true, 2", "true, 4"})
    void testRejectsExplicitConcurrencyOnUpdatingInput(boolean async, int concurrency) {
        util.addTemporarySystemFunction("parallel_func", function(async, concurrency));

        assertThatThrownBy(
                        () ->
                                util.tableEnv()
                                        .explainSql(
                                                "SELECT a, parallel_func(b, b) FROM updating_source"))
                .hasMessageContaining("Python UDF concurrency is only supported for insert-only")
                .hasMessageContaining("Omit concurrency");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRejectsConcurrencyWhenCompilingUpdatingInput(boolean async) {
        util.addTemporarySystemFunction("parallel_func", function(async, 2));

        assertThatThrownBy(
                        () ->
                                util.tableEnv()
                                        .compilePlanSql(
                                                "INSERT INTO sink_table "
                                                        + "SELECT a, parallel_func(b, b) "
                                                        + "FROM updating_source"))
                .hasMessageContaining("Python UDF concurrency is only supported for insert-only");
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
    void testRejectsConcurrencyOnInputWithDeletes() {
        util.addTemporarySystemFunction("parallel_func", function(false, 2));

        assertThatThrownBy(() -> translate("SELECT a, parallel_func(b, b) FROM deleting_source"))
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

        assertThat(translate("SELECT a, default_func(b, b) FROM updating_source")).hasSize(1);
    }

    @ParameterizedTest
    @CsvSource({"false, 2", "true, 2"})
    void testInsertOnlyInputWithExplicitConcurrencyIsSupported(boolean async, int concurrency) {
        util.addTemporarySystemFunction("parallel_func", function(async, concurrency));

        assertThat(translate("SELECT a, parallel_func(b, b) FROM source_table")).hasSize(1);
    }

    private List<Transformation<?>> translate(String query) {
        final List<Operation> operations =
                util.getPlanner().getParser().parse("INSERT INTO sink_table " + query);
        return util.getPlanner()
                .translate(Collections.singletonList((ModifyOperation) operations.get(0)));
    }

    private UserDefinedFunction function(boolean async, int concurrency) {
        return async
                ? new ResourceAsyncPythonScalarFunction("parallel_func", concurrency)
                : new ParallelPythonScalarFunction("parallel_func", concurrency);
    }

    public static class ResourceAsyncPythonScalarFunction extends AsyncPythonScalarFunction {
        private final int parallelism;

        public ResourceAsyncPythonScalarFunction(String name, int parallelism) {
            super(name);
            this.parallelism = parallelism;
        }

        @Override
        public int getParallelism() {
            return parallelism;
        }
    }
}
