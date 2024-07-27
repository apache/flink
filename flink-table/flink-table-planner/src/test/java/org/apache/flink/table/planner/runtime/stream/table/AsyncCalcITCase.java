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
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** IT Case tests for {@link AsyncScalarFunction}. */
public class AsyncCalcITCase extends StreamingTestBase {

    private TableEnvironment tEnv;

    @BeforeEach
    public void before() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inStreamingMode());
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_BUFFER_CAPACITY, 2);
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_TIMEOUT, Duration.ofMinutes(1));
    }

    @Test
    public void testSimpleTableSelect() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new AsyncFunc());
        final List<Row> results = executeSql("select func(f1) from t1");
        final List<Row> expectedRows =
                Arrays.asList(Row.of("val 1"), Row.of("val 2"), Row.of("val 3"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testLiteralPlusTableSelect() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new AsyncFunc());
        final List<Row> results = executeSql("select 'foo', func(f1) from t1");
        final List<Row> expectedRows =
                Arrays.asList(
                        Row.of("foo", "val 1"), Row.of("foo", "val 2"), Row.of("foo", "val 3"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testFieldPlusTableSelect() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new AsyncFunc());
        final List<Row> results = executeSql("select f1, func(f1) from t1");
        final List<Row> expectedRows =
                Arrays.asList(Row.of(1, "val 1"), Row.of(2, "val 2"), Row.of(3, "val 3"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testTwoCalls() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new AsyncFunc());
        final List<Row> results = executeSql("select func(f1), func(f1) from t1");
        final List<Row> expectedRows =
                Arrays.asList(
                        Row.of("val 1", "val 1"),
                        Row.of("val 2", "val 2"),
                        Row.of("val 3", "val 3"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testThreeNestedCalls() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new AsyncFuncAdd10());
        final List<Row> results =
                executeSql("select func(func(f1)), func(func(func(f1))), func(f1) from t1");
        final List<Row> expectedRows =
                Arrays.asList(Row.of(21, 31, 11), Row.of(22, 32, 12), Row.of(23, 33, 13));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testPassedToOtherUDF() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new AsyncFunc());
        final List<Row> results = executeSql("select Concat(func(f1), 'foo') from t1");
        final List<Row> expectedRows =
                Arrays.asList(Row.of("val 1foo"), Row.of("val 2foo"), Row.of("val 3foo"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testJustCall() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new AsyncFunc());
        final List<Row> results = executeSql("select func(1)");
        final List<Row> expectedRows = Collections.singletonList(Row.of("val 1"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testWhereConditionAndProjection() {
        Table t1 = tEnv.fromValues(1, 2, 3).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new AsyncFunc());
        final List<Row> results =
                executeSql("select func(f1) from t1 where REGEXP(func(f1), 'val (2|3)')");
        final List<Row> expectedRows = Arrays.asList(Row.of("val 2"), Row.of("val 3"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testFieldAccessAfter() {
        Table t1 = tEnv.fromValues(2).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new AsyncFuncRow());
        final List<Row> results = executeSql("select func(f1).f0 from t1");
        final List<Row> expectedRows = Collections.singletonList(Row.of(3));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testFieldOperand() {
        Table t1 = tEnv.fromValues(2).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new AsyncFuncRow());
        tEnv.createTemporarySystemFunction("func2", new AsyncFuncAdd10());
        Table structs = tEnv.sqlQuery("select func(f1) from t1");
        tEnv.createTemporaryView("t2", structs);
        final List<Row> results = executeSql("select func2(t2.f0) from t2");
        final List<Row> expectedRows = Collections.singletonList(Row.of(13));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testOverload() {
        Table t1 = tEnv.fromValues(1).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new AsyncFuncOverload());
        final List<Row> results = executeSql("select func(f1), func(cast(f1 as String)) from t1");
        final List<Row> expectedRows =
                Collections.singletonList(Row.of("int version 1", "string version 1"));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testMultiLayerGeneric() {
        Table t1 = tEnv.fromValues(1).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new LongAsyncFuncGeneric());
        final List<Row> results = executeSql("select func(f1) from t1");
        final List<Row> expectedRows = Collections.singletonList(Row.of((Object) new Long[] {11L}));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testMultiLayerMoreGeneric() {
        Table t1 = tEnv.fromValues(1).as("f1");
        tEnv.createTemporaryView("t1", t1);
        tEnv.createTemporarySystemFunction("func", new LongAsyncFuncMoreGeneric());
        final List<Row> results = executeSql("select func(f1) from t1");
        final List<Row> expectedRows = Collections.singletonList(Row.of((Object) new Long[] {11L}));
        assertThat(results).containsSequence(expectedRows);
    }

    @Test
    public void testFailures() {
        // If there is a failure after hitting the end of the input, then it doesn't retry. Having
        // the buffer = 1 triggers the end input only after completion.
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_BUFFER_CAPACITY, 1);
        Table t1 = tEnv.fromValues(1).as("f1");
        tEnv.createTemporaryView("t1", t1);
        AsyncFuncFail func = new AsyncFuncFail(2);
        tEnv.createTemporarySystemFunction("func", func);
        final List<Row> results = executeSql("select func(f1) from t1");
        final List<Row> expectedRows = Collections.singletonList(Row.of(3));
        assertThat(results).containsSequence(expectedRows);
    }

    private List<Row> executeSql(String sql) {
        TableResult result = tEnv.executeSql(sql);
        final List<Row> rows = new ArrayList<>();
        result.collect().forEachRemaining(rows::add);
        return rows;
    }

    /** Test function. */
    public static class AsyncFunc extends AsyncFuncBase {

        private static final long serialVersionUID = 1L;

        public void eval(CompletableFuture<String> future, Integer param) {
            executor.schedule(() -> future.complete("val " + param), 10, TimeUnit.MILLISECONDS);
        }
    }

    /** Test function. */
    public static class AsyncFuncAdd10 extends AsyncFuncBase {

        private static final long serialVersionUID = 2L;

        public void eval(CompletableFuture<Integer> future, Integer param) {
            executor.schedule(() -> future.complete(param + 10), 10, TimeUnit.MILLISECONDS);
        }
    }

    /** Test function. */
    public static class AsyncFuncOverload extends AsyncFuncBase {

        private static final long serialVersionUID = 3L;

        public void eval(CompletableFuture<String> future, Integer param) {
            executor.schedule(
                    () -> future.complete("int version " + param), 10, TimeUnit.MILLISECONDS);
        }

        public void eval(CompletableFuture<String> future, String param) {
            executor.schedule(
                    () -> future.complete("string version " + param), 10, TimeUnit.MILLISECONDS);
        }
    }

    /** Test function. */
    public static class AsyncFuncRow extends AsyncScalarFunction {

        @DataTypeHint("ROW<f0 INT, f1 String>")
        public void eval(CompletableFuture<Row> future, int a) {
            future.complete(Row.of(a + 1, "" + (a * a)));
        }
    }

    /** Test function. */
    public static class AsyncFuncFail extends AsyncFuncBase implements Serializable {

        private static final long serialVersionUID = 8996145425452974113L;

        private final int numFailures;
        private final AtomicInteger failures = new AtomicInteger(0);

        public AsyncFuncFail(int numFailures) {
            this.numFailures = numFailures;
        }

        public void eval(CompletableFuture<Integer> future, int ignoredA) {
            if (failures.getAndIncrement() < numFailures) {
                future.completeExceptionally(new RuntimeException("Error " + failures.get()));
                return;
            }
            future.complete(failures.get());
        }
    }

    /** Test function. */
    public abstract static class AsyncFuncGeneric<T> extends AsyncFuncBase {

        private static final long serialVersionUID = 3L;

        abstract T[] newT(int param);

        public void eval(CompletableFuture<T[]> future, Integer param) {
            executor.schedule(() -> future.complete(newT(param)), 10, TimeUnit.MILLISECONDS);
        }
    }

    /** Test function. */
    public static class LongAsyncFuncGeneric extends AsyncFuncGeneric<Long> {
        @Override
        Long[] newT(int param) {
            Long[] result = new Long[1];
            result[0] = 10L + param;
            return result;
        }
    }

    /** Test function. */
    public abstract static class AsyncFuncMoreGeneric<T> extends AsyncFuncBase {

        private static final long serialVersionUID = 3L;

        abstract void finish(T future, int param);

        public void eval(T future, Integer param) {
            executor.schedule(() -> finish(future, param), 10, TimeUnit.MILLISECONDS);
        }
    }

    /** Test function. */
    public static class LongAsyncFuncMoreGeneric
            extends AsyncFuncMoreGeneric<CompletableFuture<Long[]>> {
        @Override
        void finish(CompletableFuture<Long[]> future, int param) {
            Long[] result = new Long[1];
            result[0] = 10L + param;
            future.complete(result);
        }
    }

    /** Test function. */
    public static class AsyncFuncBase extends AsyncScalarFunction {

        protected ScheduledExecutorService executor;

        @Override
        public void open(FunctionContext context) {
            executor = Executors.newSingleThreadScheduledExecutor();
        }

        @Override
        public void close() {
            if (null != executor && !executor.isShutdown()) {
                executor.shutdownNow();
            }
        }
    }
}
