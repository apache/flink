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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end tests for the opt-in per-operator UDF metrics (FLIP-485) registered under {@code
 * <operator>.udf.<udfName>} for sync scalar and table user-defined functions.
 */
class UdfMetricsITCase {

    private static final InMemoryReporter reporter = InMemoryReporter.createWithRetainedMetrics();

    @RegisterExtension
    private static final MiniClusterExtension MINI_CLUSTER =
            new MiniClusterExtension(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .setConfiguration(reporter.addToConfiguration(new Configuration()))
                            .build());

    private static final List<Row> SOURCE_ROWS = Arrays.asList(Row.of(1), Row.of(2), Row.of(3));

    // Matches the processing-time metric of any UDF, for asserting that none is registered.
    private static final String ANY_PROCESSING_TIME_PATTERN = "\\.udf\\..*\\.udfProcessingTime";
    private static final String ANY_EXCEPTION_COUNT_PATTERN = "\\.udf\\..*\\.udfExceptionCount";

    /** The metric identifier group is {@code <operator>.udf.<udfName>}. */
    private static String processingTimePattern(String udfName) {
        return "\\.udf\\." + udfName + "\\.udfProcessingTime";
    }

    private static String exceptionCountPattern(String udfName) {
        return "\\.udf\\." + udfName + "\\.udfExceptionCount";
    }

    /** Doubles an int; the metered sync scalar path. */
    public static class IntDoubler extends ScalarFunction {
        public Integer eval(Integer i) {
            return i == null ? null : i * 2;
        }
    }

    /** Negates an int; a second distinct scalar function. */
    public static class IntNegator extends ScalarFunction {
        public Integer eval(Integer i) {
            return i == null ? null : -i;
        }
    }

    /** Always throws; used to exercise the exception counter. */
    public static class AlwaysThrows extends ScalarFunction {
        public Integer eval(Integer i) {
            throw new RuntimeException("boom");
        }
    }

    /** Emits each input twice; the metered sync table path. */
    public static class DuplicateRows extends TableFunction<Integer> {
        public void eval(Integer i) {
            collect(i);
            collect(i);
        }
    }

    /** Doubles an int off the task thread; the metered async scalar path. */
    public static class AsyncIntDoubler extends AsyncScalarFunction {
        private transient ScheduledExecutorService executor;

        @Override
        public void open(FunctionContext context) {
            executor = Executors.newSingleThreadScheduledExecutor();
        }

        @Override
        public void close() {
            if (executor != null) {
                executor.shutdownNow();
            }
        }

        public void eval(CompletableFuture<Integer> future, Integer i) {
            executor.schedule(
                    () -> future.complete(i == null ? null : i * 2), 5, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Completes exceptionally the first {@code numFailures} invocations, then succeeds. Exercises
     * the async completion-exception counter while the async operator's retry keeps the job alive.
     */
    public static class AsyncFlaky extends AsyncScalarFunction {
        private final int numFailures;
        private final AtomicInteger failures = new AtomicInteger();
        private transient ScheduledExecutorService executor;

        public AsyncFlaky(int numFailures) {
            this.numFailures = numFailures;
        }

        @Override
        public void open(FunctionContext context) {
            executor = Executors.newSingleThreadScheduledExecutor();
        }

        @Override
        public void close() {
            if (executor != null) {
                executor.shutdownNow();
            }
        }

        public void eval(CompletableFuture<Integer> future, Integer i) {
            executor.schedule(
                    () -> {
                        if (failures.getAndIncrement() < numFailures) {
                            future.completeExceptionally(new RuntimeException("boom"));
                        } else {
                            future.complete(i);
                        }
                    },
                    5,
                    TimeUnit.MILLISECONDS);
        }
    }

    /** Emits each input twice off the task thread; the metered async table path. */
    public static class AsyncDuplicateRows extends AsyncTableFunction<Integer> {
        private transient ScheduledExecutorService executor;

        @Override
        public void open(FunctionContext context) {
            executor = Executors.newSingleThreadScheduledExecutor();
        }

        @Override
        public void close() {
            if (executor != null) {
                executor.shutdownNow();
            }
        }

        public void eval(CompletableFuture<Collection<Integer>> future, Integer i) {
            executor.schedule(() -> future.complete(Arrays.asList(i, i)), 5, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void testSyncScalarMetricsRecorded() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv(true);
        tEnv.createTemporarySystemFunction("scalarudf", IntDoubler.class);
        createSource(tEnv, "src", "id INT");
        createBlackHoleSink(tEnv, "sink", "v INT");

        JobID jobId = execute(tEnv, "INSERT INTO sink SELECT scalarudf(id) FROM src");

        Histogram processingTime = histogram(jobId, processingTimePattern("scalarudf"));
        // Sample interval 1 measures every invocation: one per input row.
        assertThat(processingTime.getCount()).isEqualTo(SOURCE_ROWS.size());
        assertThat(counter(jobId, exceptionCountPattern("scalarudf")).getCount()).isZero();
    }

    @Test
    void testSyncTableMetricsRecorded() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv(true);
        tEnv.createTemporarySystemFunction("tableudf", DuplicateRows.class);
        createSource(tEnv, "src", "id INT");
        createBlackHoleSink(tEnv, "sink", "v INT");

        JobID jobId =
                execute(
                        tEnv,
                        "INSERT INTO sink SELECT x FROM src, LATERAL TABLE(tableudf(id)) AS T(x)");

        // eval is called once per input row (it emits two rows internally); one sample each.
        assertThat(histogram(jobId, processingTimePattern("tableudf")).getCount())
                .isEqualTo(SOURCE_ROWS.size());
    }

    @Test
    void testExceptionCounted() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv(true);
        tEnv.createTemporarySystemFunction("boomudf", AlwaysThrows.class);
        createSource(tEnv, "src", "id INT");
        createBlackHoleSink(tEnv, "sink", "v INT");

        TableResult result = tEnv.executeSql("INSERT INTO sink SELECT boomudf(id) FROM src");
        JobID jobId = result.getJobClient().get().getJobID();
        assertThatThrownBy(result::await).isInstanceOf(Exception.class);

        assertThat(counter(jobId, exceptionCountPattern("boomudf")).getCount())
                .isGreaterThanOrEqualTo(1);
    }

    @Test
    void testDisabledRegistersNoUdfMetrics() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv(false);
        tEnv.createTemporarySystemFunction("offudf", IntDoubler.class);
        createSource(tEnv, "src", "id INT");
        createBlackHoleSink(tEnv, "sink", "v INT");

        JobID jobId = execute(tEnv, "INSERT INTO sink SELECT offudf(id) FROM src");

        assertThat(reporter.findMetrics(jobId, ANY_PROCESSING_TIME_PATTERN)).isEmpty();
        assertThat(reporter.findMetrics(jobId, ANY_EXCEPTION_COUNT_PATTERN)).isEmpty();
    }

    @Test
    void testLookupJoinNotMetered() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv(true);
        String probeId = TestValuesTableFactory.registerData(SOURCE_ROWS);
        tEnv.executeSql(
                "CREATE TABLE probe (id INT, proctime AS PROCTIME()) WITH ("
                        + "'connector' = 'values', 'bounded' = 'true', 'data-id' = '"
                        + probeId
                        + "')");
        String dimId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(Row.of(1, "a"), Row.of(2, "b"), Row.of(3, "c")));
        tEnv.executeSql(
                "CREATE TABLE dim (id INT, name STRING) WITH ("
                        + "'connector' = 'values', 'data-id' = '"
                        + dimId
                        + "')");
        createBlackHoleSink(tEnv, "sink", "v STRING");

        JobID jobId =
                execute(
                        tEnv,
                        "INSERT INTO sink SELECT dim.name FROM probe "
                                + "JOIN dim FOR SYSTEM_TIME AS OF probe.proctime "
                                + "ON probe.id = dim.id");

        // The lookup function reuses the same codegen util but opts out of metrics (SD1).
        assertThat(reporter.findMetrics(jobId, ANY_PROCESSING_TIME_PATTERN)).isEmpty();
    }

    @Test
    void testRepeatedFunctionSharesOneHandle() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv(true);
        tEnv.createTemporarySystemFunction("sharedudf", IntDoubler.class);
        String dataId =
                TestValuesTableFactory.registerData(
                        Arrays.asList(Row.of(1, 10), Row.of(2, 20), Row.of(3, 30)));
        tEnv.executeSql(
                "CREATE TABLE src2 (a INT, b INT) WITH ("
                        + "'connector' = 'values', 'bounded' = 'true', 'data-id' = '"
                        + dataId
                        + "')");
        createBlackHoleSink(tEnv, "sink", "v INT");

        JobID jobId =
                execute(tEnv, "INSERT INTO sink SELECT sharedudf(a) + sharedudf(b) FROM src2");

        // Two call sites of one function in one operator share a single handle, so both feed the
        // same histogram: 2 evals per row. Without sharing, the second registration is dropped and
        // only one call site's timings survive (one per row).
        Map<String, Metric> processingTimes =
                reporter.findMetrics(jobId, processingTimePattern("sharedudf"));
        assertThat(processingTimes).hasSize(1);
        assertThat(((Histogram) processingTimes.values().iterator().next()).getCount())
                .isEqualTo(2L * SOURCE_ROWS.size());
    }

    @Test
    void testDistinctFunctionsGetSeparateHandles() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv(true);
        tEnv.createTemporarySystemFunction("doubleudf", IntDoubler.class);
        tEnv.createTemporarySystemFunction("negateudf", IntNegator.class);
        createSource(tEnv, "src", "id INT");
        createBlackHoleSink(tEnv, "sink", "d INT, n INT");

        JobID jobId =
                execute(tEnv, "INSERT INTO sink SELECT doubleudf(id), negateudf(id) FROM src");

        // Two distinct functions register two separate udf.<name> handles, each with its own
        // histogram counting one sample per input row.
        assertThat(histogram(jobId, processingTimePattern("doubleudf")).getCount())
                .isEqualTo(SOURCE_ROWS.size());
        assertThat(histogram(jobId, processingTimePattern("negateudf")).getCount())
                .isEqualTo(SOURCE_ROWS.size());
    }

    @Test
    void testAsyncScalarMetricsRecorded() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv(true);
        tEnv.createTemporarySystemFunction("asyncudf", AsyncIntDoubler.class);
        createSource(tEnv, "src", "id INT");
        createBlackHoleSink(tEnv, "sink", "v INT");

        JobID jobId = execute(tEnv, "INSERT INTO sink SELECT asyncudf(id) FROM src");

        // The processing time spans dispatch to off-thread completion; one sample per input row.
        assertThat(histogram(jobId, processingTimePattern("asyncudf")).getCount())
                .isEqualTo(SOURCE_ROWS.size());
        assertThat(counter(jobId, exceptionCountPattern("asyncudf")).getCount()).isZero();
    }

    @Test
    void testAsyncCompletionExceptionSurvivesJob() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv(true);
        // Serialize invocations so the shared failure counter drives a deterministic retry.
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_MAX_CONCURRENT_OPERATIONS, 1);
        tEnv.createTemporarySystemFunction("flakyudf", new AsyncFlaky(2));
        createSource(tEnv, "src", "id INT");
        createBlackHoleSink(tEnv, "sink", "v INT");

        // Two exceptional completions are counted, then the async retry succeeds and the job
        // finishes normally: an exceptional completion is a soft error, not a job failure.
        JobID jobId = execute(tEnv, "INSERT INTO sink SELECT flakyudf(id) FROM src");

        assertThat(counter(jobId, exceptionCountPattern("flakyudf")).getCount())
                .isGreaterThanOrEqualTo(1);
    }

    @Test
    void testAsyncTableMetricsRecorded() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv(true);
        tEnv.createTemporarySystemFunction("asynctableudf", AsyncDuplicateRows.class);
        createSource(tEnv, "src", "id INT");
        createBlackHoleSink(tEnv, "sink", "v INT");

        JobID jobId =
                execute(
                        tEnv,
                        "INSERT INTO sink SELECT x FROM src, "
                                + "LATERAL TABLE(asynctableudf(id)) AS T(x)");

        // eval is called once per input row (it completes two rows); one sample each.
        assertThat(histogram(jobId, processingTimePattern("asynctableudf")).getCount())
                .isEqualTo(SOURCE_ROWS.size());
    }

    @Test
    void testAsyncDisabledRegistersNoUdfMetrics() throws Exception {
        StreamTableEnvironment tEnv = createTableEnv(false);
        tEnv.createTemporarySystemFunction("asyncoffudf", AsyncIntDoubler.class);
        createSource(tEnv, "src", "id INT");
        createBlackHoleSink(tEnv, "sink", "v INT");

        JobID jobId = execute(tEnv, "INSERT INTO sink SELECT asyncoffudf(id) FROM src");

        assertThat(reporter.findMetrics(jobId, ANY_PROCESSING_TIME_PATTERN)).isEmpty();
        assertThat(reporter.findMetrics(jobId, ANY_EXCEPTION_COUNT_PATTERN)).isEmpty();
    }

    private static StreamTableEnvironment createTableEnv(boolean udfMetricEnabled) {
        Configuration conf = new Configuration();
        conf.set(RestartStrategyOptions.RESTART_STRATEGY, "disable");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_UDF_METRIC_ENABLED, udfMetricEnabled);
        tEnv.getConfig().set(ExecutionConfigOptions.TABLE_EXEC_UDF_METRIC_SAMPLE_INTERVAL, 1);
        return tEnv;
    }

    private static void createSource(StreamTableEnvironment tEnv, String name, String schema) {
        String dataId = TestValuesTableFactory.registerData(SOURCE_ROWS);
        tEnv.executeSql(
                "CREATE TABLE "
                        + name
                        + " ("
                        + schema
                        + ") WITH ('connector' = 'values', 'bounded' = 'true', 'data-id' = '"
                        + dataId
                        + "')");
    }

    private static void createBlackHoleSink(
            StreamTableEnvironment tEnv, String name, String schema) {
        tEnv.executeSql(
                "CREATE TABLE " + name + " (" + schema + ") WITH ('connector' = 'blackhole')");
    }

    private static JobID execute(StreamTableEnvironment tEnv, String insert) throws Exception {
        TableResult result = tEnv.executeSql(insert);
        JobID jobId = result.getJobClient().get().getJobID();
        result.await();
        return jobId;
    }

    private static Histogram histogram(JobID jobId, String pattern) {
        return (Histogram) singleMetric(jobId, pattern);
    }

    private static Counter counter(JobID jobId, String pattern) {
        return (Counter) singleMetric(jobId, pattern);
    }

    private static Metric singleMetric(JobID jobId, String pattern) {
        Map<String, Metric> found = reporter.findMetrics(jobId, pattern);
        assertThat(found).hasSize(1);
        return found.values().iterator().next();
    }
}
