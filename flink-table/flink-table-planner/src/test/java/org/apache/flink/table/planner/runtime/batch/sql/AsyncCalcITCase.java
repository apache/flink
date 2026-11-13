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

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** IT cases for {@link AsyncScalarFunction} in batch mode. */
class AsyncCalcITCase extends BatchTestBase {

    private static final int NUM_ROWS = 1000;
    private static final int SMALL_NUM_ROWS = 100;
    private static final int RETRY_NUM_ROWS = 10;
    private static final int MAX_CONCURRENT_OPERATIONS = 50;
    private static final int ADDEND = 10;

    @BeforeEach
    @Override
    public void before() throws Exception {
        super.before();
        tEnv().getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_MAX_CONCURRENT_OPERATIONS,
                        MAX_CONCURRENT_OPERATIONS);
        tEnv().getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 1);
        tEnv().createTemporarySystemFunction("delayedAdd", DelayedAddTen.class);
        tEnv().createTemporarySystemFunction("failingAdd", FailingAddTen.class);
        tEnv().createTemporarySystemFunction("twice", Twice.class);
        FailingAddTen.FAILURES.set(0);
        FailingAddTen.ATTEMPTS.set(0);
    }

    private void createTables(int numRows, int sinkParallelism) {
        final List<Row> data = new ArrayList<>();
        for (int i = 0; i < numRows; i++) {
            data.add(Row.of(i));
        }
        tEnv().executeSql(
                        "CREATE TABLE source_t (a INT) WITH ("
                                + "'connector' = 'values', "
                                + "'bounded' = 'true', "
                                + "'data-id' = '"
                                + TestValuesTableFactory.registerData(data)
                                + "')");
        tEnv().executeSql(
                        "CREATE TABLE sink_t (a INT, b INT) WITH ("
                                + "'connector' = 'values', "
                                + "'sink-insert-only' = 'true', "
                                + "'sink.parallelism' = '"
                                + sinkParallelism
                                + "')");
    }

    @Test
    void testAsyncCalc() throws Exception {
        createTables(SMALL_NUM_ROWS, 2);
        final String query = "SELECT a, delayedAdd(a) FROM source_t";
        assertThat(tEnv().explainSql(query)).contains("AsyncCalc");
        tEnv().executeSql("INSERT INTO sink_t " + query).await();
        assertThat(TestValuesTableFactory.getResultsAsStrings("sink_t"))
                .containsExactlyInAnyOrderElementsOf(expected(SMALL_NUM_ROWS));
    }

    @Test
    void testInFlightRowsDrainedAtEndOfInput() throws Exception {
        createTables(NUM_ROWS, 2);
        tEnv().executeSql("INSERT INTO sink_t SELECT a, delayedAdd(a) FROM source_t").await();
        assertThat(TestValuesTableFactory.getResultsAsStrings("sink_t"))
                .containsExactlyInAnyOrderElementsOf(expected(NUM_ROWS));
    }

    @Test
    void testOutputKeepsInputOrder() throws Exception {
        createTables(NUM_ROWS, 1);
        tEnv().executeSql("INSERT INTO sink_t SELECT a, delayedAdd(a) FROM source_t").await();
        assertThat(TestValuesTableFactory.getResultsAsStrings("sink_t"))
                .containsExactlyElementsOf(expected(NUM_ROWS));
    }

    @Test
    void testAsyncCallInFilter() throws Exception {
        createTables(NUM_ROWS, 2);
        final String query = "SELECT a, a FROM source_t WHERE delayedAdd(a) % 2 = 0";
        assertThat(tEnv().explainSql(query)).contains("AsyncCalc");
        tEnv().executeSql("INSERT INTO sink_t " + query).await();
        final List<String> expected = new ArrayList<>();
        for (int i = 0; i < NUM_ROWS; i += 2) {
            expected.add("+I[" + i + ", " + i + "]");
        }
        assertThat(TestValuesTableFactory.getResultsAsStrings("sink_t"))
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void testAsyncCallInCorrelateArgument() throws Exception {
        createTables(SMALL_NUM_ROWS, 2);
        final String query =
                "SELECT a, b FROM source_t, LATERAL TABLE(twice(delayedAdd(a))) AS T(b)";
        assertThat(tEnv().explainSql(query)).contains("AsyncCalc");
        tEnv().executeSql("INSERT INTO sink_t " + query).await();
        final List<String> expected = new ArrayList<>();
        for (int i = 0; i < SMALL_NUM_ROWS; i++) {
            expected.add("+I[" + i + ", " + (i + ADDEND) + "]");
            expected.add("+I[" + i + ", " + (i + ADDEND) + "]");
        }
        assertThat(TestValuesTableFactory.getResultsAsStrings("sink_t"))
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void testAggregateOnAsyncCalcOnHashJoin() throws Exception {
        tEnv().getConfig().set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
        tEnv().getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
                        "SortMergeJoin,NestedLoopJoin,SortAgg");
        createTables(SMALL_NUM_ROWS, 2);
        final String query =
                "SELECT t1.a, SUM(delayedAdd(t1.a + t2.a)) FROM source_t t1 "
                        + "JOIN source_t t2 ON t1.a = t2.a GROUP BY t1.a";
        assertThat(tEnv().explainSql(query)).contains("AsyncCalc", "keep_input_as_is");
        tEnv().executeSql("INSERT INTO sink_t " + query).await();
        final List<String> expected = new ArrayList<>();
        for (int i = 0; i < SMALL_NUM_ROWS; i++) {
            expected.add("+I[" + i + ", " + (2 * i + ADDEND) + "]");
        }
        assertThat(TestValuesTableFactory.getResultsAsStrings("sink_t"))
                .containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void testPendingRetryIsTriedOnceAtEndOfInput() throws Exception {
        configureRetries();
        FailingAddTen.FAILURES.set(1);
        createTables(RETRY_NUM_ROWS, 1);
        tEnv().executeSql("INSERT INTO sink_t SELECT a, failingAdd(a) FROM source_t").await();
        assertThat(TestValuesTableFactory.getResultsAsStrings("sink_t"))
                .containsExactlyElementsOf(expected(RETRY_NUM_ROWS));
        assertThat(FailingAddTen.ATTEMPTS.get()).isEqualTo(2);
    }

    @Test
    void testPendingRetryIsNotRetriedAgainAtEndOfInput() {
        configureRetries();
        FailingAddTen.FAILURES.set(2);
        createTables(RETRY_NUM_ROWS, 1);
        assertThatThrownBy(
                        () ->
                                tEnv().executeSql(
                                                "INSERT INTO sink_t SELECT a, failingAdd(a) FROM source_t")
                                        .await())
                .satisfies(anyCauseMatches(FailingAddTen.MESSAGE + 2));
        assertThat(FailingAddTen.ATTEMPTS.get()).isEqualTo(2);
    }

    private void configureRetries() {
        tEnv().getConfig().set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_MAX_ATTEMPTS, 5);
        tEnv().getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_RETRY_DELAY,
                        Duration.ofMinutes(10));
    }

    private static List<String> expected(int numRows) {
        final List<String> expected = new ArrayList<>();
        for (int i = 0; i < numRows; i++) {
            expected.add("+I[" + i + ", " + (i + ADDEND) + "]");
        }
        return expected;
    }

    /** Adds ten, completing from another thread after a random delay. */
    public static class DelayedAddTen extends AsyncScalarFunction {

        private static final int THREADS = 4;
        private static final int MAX_DELAY_MS = 20;

        private transient ExecutorService executor;

        @Override
        public void open(FunctionContext context) {
            executor = Executors.newFixedThreadPool(THREADS);
        }

        public void eval(CompletableFuture<Integer> future, Integer a) {
            executor.execute(
                    () -> {
                        try {
                            Thread.sleep(ThreadLocalRandom.current().nextInt(MAX_DELAY_MS + 1));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            future.completeExceptionally(e);
                            return;
                        }
                        future.complete(a + ADDEND);
                    });
        }

        @Override
        public void close() {
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }

    /** Adds ten, failing the first {@link #FAILURES} attempts for the first row. */
    public static class FailingAddTen extends AsyncScalarFunction {

        static final String MESSAGE = "Failed attempt ";
        static final AtomicInteger FAILURES = new AtomicInteger();
        static final AtomicInteger ATTEMPTS = new AtomicInteger();

        public void eval(CompletableFuture<Integer> future, Integer a) {
            if (a == 0) {
                final int attempt = ATTEMPTS.incrementAndGet();
                if (attempt <= FAILURES.get()) {
                    future.completeExceptionally(new RuntimeException(MESSAGE + attempt));
                    return;
                }
            }
            future.complete(a + ADDEND);
        }
    }

    /** Emits its argument twice. */
    public static class Twice extends TableFunction<Integer> {

        public void eval(Integer a) {
            collect(a);
            collect(a);
        }
    }
}
