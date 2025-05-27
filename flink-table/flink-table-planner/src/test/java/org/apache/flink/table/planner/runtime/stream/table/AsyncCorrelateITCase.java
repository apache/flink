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

package org.apache.flink.table.planner.runtime.stream.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.AsyncSumScalarFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.AsyncTestTableFunction;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.SumScalarFunction;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT Case tests for correlate queries using {@link AsyncTableFunction}. */
public class AsyncCorrelateITCase extends StreamingTestBase {

    private TableEnvironment tEnv;

    @BeforeEach
    public void before() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_TABLE_BUFFER_CAPACITY, 1);
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_TIMEOUT, Duration.ofMinutes(1));
    }

    @Test
    public void testConstantTableFunc() {
        tEnv.createTemporarySystemFunction("func", new TestTableFunction());
        final List<Row> results = executeSql("SELECT * FROM func(1);");
        final List<Row> expectedRows = Arrays.asList(Row.of("blah 1"), Row.of("foo 1"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testConstantTableFuncNoArg() {
        tEnv.createTemporarySystemFunction("func", new TestTableFunction());
        final List<Row> results = executeSql("SELECT * FROM func();");
        final List<Row> expectedRows = Arrays.asList(Row.of("blah"), Row.of("foo"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testConstantTableFuncWithLateral() {
        tEnv.createTemporarySystemFunction("func", new TestTableFunction());
        final List<Row> results = executeSql("SELECT * FROM LATERAL TABLE(func(1));");
        final List<Row> expectedRows = Arrays.asList(Row.of("blah 1"), Row.of("foo 1"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testTableFunc() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new TestTableFunction());
        final List<Row> results = executeSql("select * FROM t1, LATERAL TABLE(func(f1))");
        final List<Row> expectedRows =
                Arrays.asList(
                        Row.of(1, "blah 1"),
                        Row.of(1, "foo 1"),
                        Row.of(2, "blah 2"),
                        Row.of(2, "foo 2"),
                        Row.of(3, "blah 3"),
                        Row.of(3, "foo 3"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testTableFuncRowTypeWithHints() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new AsyncTestTableFunction());
        final List<Row> results = executeSql("select * FROM t1, LATERAL TABLE(func(f1))");
        final List<Row> expectedRows =
                Arrays.asList(
                        Row.of(1, "blah 1"),
                        Row.of(1, "foo 1"),
                        Row.of(2, "blah 2"),
                        Row.of(2, "foo 2"),
                        Row.of(3, "blah 3"),
                        Row.of(3, "foo 3"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testTableFuncWithCalc() {
        Table t1 = tEnv.fromValues(1, 2).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new AsyncTestTableFunction());
        tEnv.createTemporarySystemFunction("mySum", new SumScalarFunction());
        final List<Row> results =
                executeSql("select * FROM t1, LATERAL TABLE(func(mySum(f1, 10)))");
        final List<Row> expectedRows =
                Arrays.asList(
                        Row.of(1, "blah 11"),
                        Row.of(1, "foo 11"),
                        Row.of(2, "blah 12"),
                        Row.of(2, "foo 12"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testTableFuncWithAsyncCalc() {
        Table t1 = tEnv.fromValues(1, 2).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new AsyncTestTableFunction());
        tEnv.createTemporarySystemFunction("mySum", new AsyncSumScalarFunction());
        final List<Row> results =
                executeSql("select * FROM t1, LATERAL TABLE(func(mySum(ABS(f1), 10)))");
        final List<Row> expectedRows =
                Arrays.asList(
                        Row.of(1, "blah 11"),
                        Row.of(1, "foo 11"),
                        Row.of(2, "blah 12"),
                        Row.of(2, "foo 12"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testTableFuncWithRightCalcSelectStar() {
        Table t1 = tEnv.fromValues(1, 2).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new TestTableFunction());
        final List<Row> results =
                executeSql("select * FROM t1, LATERAL (SELECT * FROM TABLE(func(f1)) as T(foo))");
        final List<Row> expectedRows =
                Arrays.asList(
                        Row.of(1, "blah 1"),
                        Row.of(1, "foo 1"),
                        Row.of(2, "blah 2"),
                        Row.of(2, "foo 2"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testTableFuncWithRightCalcWithSelect() {
        Table t1 = tEnv.fromValues(1, 2).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new TestTableFunction());
        tEnv.createTemporarySystemFunction("mySum", new AsyncSumScalarFunction());
        assertThatThrownBy(
                        () ->
                                executeSql(
                                        "select * FROM t1, LATERAL (SELECT CONCAT(foo, 'abc') "
                                                + "FROM TABLE(func(f1)) as T(foo))"))
                .hasMessageContaining(
                        "Currently Async correlate does not support "
                                + "projections or conditions");
    }

    @Test
    public void testTableFuncWithRightCalcWithConditions() {
        Table t1 = tEnv.fromValues(1, 2).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new AsyncTestTableFunction());
        assertThatThrownBy(
                        () ->
                                executeSql(
                                        "select * FROM t1, LATERAL (SELECT * "
                                                + "FROM TABLE(func(f1)) as T(foo) WHERE CHAR_LENGTH(foo) > 0)"))
                .hasMessageContaining(
                        "Currently Async correlate does not support "
                                + "projections or conditions");
    }

    @Test
    public void testFailures() {
        // If there is a failure after hitting the end of the input, then it doesn't retry. Having
        // the buffer = 1 triggers the end input only after completion.
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_TABLE_BUFFER_CAPACITY, 1);
        Table t1 = tEnv.fromValues(1).as("f1");
        tEnv.createTemporaryView("t1", t1);
        AsyncErrorFunction func = new AsyncErrorFunction(2);
        tEnv.createTemporarySystemFunction("func", func);
        final List<Row> results = executeSql("select * FROM t1, LATERAL TABLE(func(f1))");
        final List<Row> expectedRows = Collections.singletonList(Row.of(1, "3"));
        assertThat(results).containsSequence(expectedRows);
    }

    private List<Row> executeSql(String sql) {
        TableResult result = tEnv.executeSql(sql);
        final List<Row> rows = new ArrayList<>();
        result.collect().forEachRemaining(rows::add);
        return rows;
    }

    /** A table function. */
    public static class TestTableFunction extends AsyncTableFunction<String> {

        public void eval(CompletableFuture<Collection<String>> result, Integer i) {
            result.complete(Arrays.asList("blah " + i, "foo " + i));
        }

        public void eval(CompletableFuture<Collection<String>> result) {
            result.complete(Arrays.asList("blah", "foo"));
        }
    }

    /** A error function. */
    public static class AsyncErrorFunction extends AsyncTableFunction<String> {

        private final int numFailures;
        private final AtomicInteger failures = new AtomicInteger(0);

        public AsyncErrorFunction(int numFailures) {
            this.numFailures = numFailures;
        }

        public void eval(CompletableFuture<Collection<String>> future, int ignored) {
            if (failures.getAndIncrement() < numFailures) {
                future.completeExceptionally(new RuntimeException("Error " + failures.get()));
                return;
            }
            future.complete(Collections.singletonList("" + failures.get()));
        }
    }
}
