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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.planner.runtime.utils.StreamAbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end coverage for the user-defined {@code timeout(...)} convention on {@link
 * AsyncTableFunction} when invoked from a correlate (LATERAL TABLE) query, complementing the
 * lookup-join coverage in {@code AsyncTableFunctionTimeoutITCase} / {@code
 * AsyncLookupJoinTimeoutITCase}. Drives the correlate path through {@code
 * AsyncCorrelateCodeGenerator} + {@code AsyncCorrelateRunner}, which previously discarded the
 * generated timeout body.
 *
 * <p>Cases covered (mirroring the lookup-join test):
 *
 * <ul>
 *   <li>A valid {@code timeout(...)} that completes the future exceptionally surfaces the user's
 *       exception instead of the framework default {@link TimeoutException}.
 *   <li>A {@code timeout(...)} that completes with a default row routes that row downstream instead
 *       of failing the job.
 *   <li>An {@code AsyncTableFunction} without any {@code timeout(...)} method falls back to the
 *       framework default {@link TimeoutException} (codegen must not break).
 *   <li>An incompatible {@code timeout(...)} signature is rejected by codegen with a {@link
 *       ValidationException} before submission.
 * </ul>
 */
class AsyncCorrelateTimeoutITCase extends StreamAbstractTestBase {

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    @BeforeEach
    void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_TABLE_MAX_CONCURRENT_OPERATIONS, 1);
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_TABLE_TIMEOUT, Duration.ofMillis(200));
    }

    @Test
    void testCustomTimeoutFallbackForCorrelateQuery() {
        tEnv.createTemporarySystemFunction("slow_func", new TimeoutThrowingTableFunction());
        createSourceAndSink();

        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink_t\n"
                                + "SELECT s.id, t.s FROM source_t AS s,\n"
                                + "  LATERAL TABLE(slow_func(s.id)) AS t(s)");

        assertThatThrownBy(result::await)
                .satisfies(anyCauseMatches(CustomCorrelateTimeoutException.class));
    }

    @Test
    void testDefaultTimeoutWhenCorrelateUdfHasNoTimeout() {
        tEnv.createTemporarySystemFunction("slow_func", new NoTimeoutTableFunction());
        createSourceAndSink();

        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink_t\n"
                                + "SELECT s.id, t.s FROM source_t AS s,\n"
                                + "  LATERAL TABLE(slow_func(s.id)) AS t(s)");

        assertThatThrownBy(result::await).satisfies(anyCauseMatches(TimeoutException.class));
    }

    @Test
    void testCustomTimeoutReturnsDefaultRowForCorrelateQuery() throws Exception {
        tEnv.createTemporarySystemFunction("slow_func", new TimeoutDefaultRowTableFunction());
        createSourceOnly();

        List<Row> rows =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "SELECT s.id, t.s FROM source_t AS s,\n"
                                                + "  LATERAL TABLE(slow_func(s.id)) AS t(s)")
                                .collect());

        // Source emits 1..3 — every row times out, so each yields exactly one DEFAULT row.
        assertThat(rows).hasSize(3);
        assertThat(rows)
                .allSatisfy(
                        row ->
                                assertThat(row.getField(1))
                                        .isEqualTo(
                                                TimeoutDefaultRowTableFunction
                                                        .DEFAULT_VALUE_MARKER));
    }

    @Test
    void testCustomTimeoutReturnsEmptyCollectionForCorrelateQuery() {
        tEnv.createTemporarySystemFunction("slow_func", new TimeoutEmptyTableFunction());
        createSourceOnly();

        // INNER (cross) correlate: timeout completing with an empty collection must produce
        // zero output rows for each input — the left row is dropped, not padded.
        List<Row> rows =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "SELECT s.id, t.s FROM source_t AS s,\n"
                                                + "  LATERAL TABLE(slow_func(s.id)) AS t(s)")
                                .collect());

        assertThat(rows).isEmpty();
    }

    @Test
    void testCustomTimeoutReturnsMultipleRowsForCorrelateQuery() {
        tEnv.createTemporarySystemFunction("slow_func", new TimeoutMultiRowTableFunction());
        createSourceOnly();

        // Source emits 1..3; each row times out and the user's timeout(...) emits two rows.
        // Verifies that JoinedRowResultFuture correctly fans out the left row across the
        // multi-row fallback (one-to-many correlate semantics under timeout).
        List<Row> rows =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "SELECT s.id, t.s FROM source_t AS s,\n"
                                                + "  LATERAL TABLE(slow_func(s.id)) AS t(s)")
                                .collect());

        assertThat(rows).hasSize(6);
        for (long id = 1L; id <= 3L; id++) {
            final long expectedId = id;
            assertThat(
                            rows.stream()
                                    .filter(
                                            row ->
                                                    ((Number) row.getField(0)).longValue()
                                                            == expectedId)
                                    .map(row -> row.getField(1))
                                    .collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder(
                            TimeoutMultiRowTableFunction.MARKER_A,
                            TimeoutMultiRowTableFunction.MARKER_B);
        }
    }

    /**
     * The user's {@code timeout(...)} must complete the future <em>before it returns</em>. Codegen
     * enforces this by checking {@code delegates.getCompletableFuture().isDone()} after the call
     * and forcing {@link IllegalStateException} if it's not — without that check, an
     * incorrectly-written timeout (one that issues another async call and stores the future for a
     * later callback, or simply forgets to complete it) would leave the operator's {@code
     * ResultHandler} hanging until shutdown. This test drives that exact path: the UDF returns
     * without touching the future, and the job must fail with the codegen-emitted {@code
     * IllegalStateException}.
     */
    @Test
    void testTimeoutNotCompletingFutureFailsFast() {
        tEnv.createTemporarySystemFunction("slow_func", new NoCompletionTimeoutTableFunction());
        createSourceAndSink();

        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink_t\n"
                                + "SELECT s.id, t.s FROM source_t AS s,\n"
                                + "  LATERAL TABLE(slow_func(s.id)) AS t(s)");

        assertThatThrownBy(result::await)
                .satisfies(anyCauseMatches(IllegalStateException.class, "synchronously"));
    }

    @Test
    void testIncompatibleTimeoutSignatureFailsFastForCorrelateQuery() {
        tEnv.createTemporarySystemFunction("slow_func", new IncompatibleTimeoutTableFunction());
        createSourceAndSink();

        // Codegen runs during executeSql planning, so ValidationException must surface here —
        // before any job is submitted or task is initialised.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "INSERT INTO sink_t\n"
                                                + "SELECT s.id, t.s FROM source_t AS s,\n"
                                                + "  LATERAL TABLE(slow_func(s.id)) AS t(s)"))
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                IncompatibleTimeoutTableFunction.class.getName()))
                .satisfies(anyCauseMatches(ValidationException.class, "CompletableFuture"))
                .satisfies(anyCauseMatches(ValidationException.class, "Long"));
    }

    private void createSourceAndSink() {
        createSourceOnly();
        tEnv.executeSql(
                "CREATE TABLE sink_t (id BIGINT, s STRING) WITH ('connector' = 'blackhole')");
    }

    private void createSourceOnly() {
        tEnv.executeSql(
                "CREATE TABLE source_t (id BIGINT) WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'fields.id.kind' = 'sequence',\n"
                        + "  'fields.id.start' = '1',\n"
                        + "  'fields.id.end' = '3'\n"
                        + ")");
    }

    /** Custom exception raised by user-supplied {@code timeout(...)} hooks below. */
    public static class CustomCorrelateTimeoutException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public CustomCorrelateTimeoutException(String message) {
            super(message);
        }
    }

    /** Never completes eval; timeout completes the future exceptionally. */
    public static class TimeoutThrowingTableFunction extends AsyncTableFunction<String> {

        private static final long serialVersionUID = 1L;

        public void eval(CompletableFuture<Collection<String>> future, Long key) {
            // Never completes — only timeout() ever runs.
        }

        public void timeout(CompletableFuture<Collection<String>> future, Long key) {
            future.completeExceptionally(
                    new CustomCorrelateTimeoutException(
                            "Custom timeout from TimeoutThrowingTableFunction for key " + key));
        }
    }

    /** Never completes eval; timeout completes with a single DEFAULT row. */
    public static class TimeoutDefaultRowTableFunction extends AsyncTableFunction<String> {

        private static final long serialVersionUID = 1L;
        public static final String DEFAULT_VALUE_MARKER = "DEFAULT";

        public void eval(CompletableFuture<Collection<String>> future, Long key) {
            // Never completes — only timeout() ever runs.
        }

        public void timeout(CompletableFuture<Collection<String>> future, Long key) {
            future.complete(Collections.singletonList(DEFAULT_VALUE_MARKER));
        }
    }

    /** Never completes eval; timeout completes with an empty collection (zero rows). */
    public static class TimeoutEmptyTableFunction extends AsyncTableFunction<String> {

        private static final long serialVersionUID = 1L;

        public void eval(CompletableFuture<Collection<String>> future, Long key) {
            // Never completes — only timeout() ever runs.
        }

        public void timeout(CompletableFuture<Collection<String>> future, Long key) {
            future.complete(Collections.emptyList());
        }
    }

    /** Never completes eval; timeout completes with two rows to exercise the fan-out path. */
    public static class TimeoutMultiRowTableFunction extends AsyncTableFunction<String> {

        private static final long serialVersionUID = 1L;
        public static final String MARKER_A = "A";
        public static final String MARKER_B = "B";

        public void eval(CompletableFuture<Collection<String>> future, Long key) {
            // Never completes — only timeout() ever runs.
        }

        public void timeout(CompletableFuture<Collection<String>> future, Long key) {
            future.complete(Arrays.asList(MARKER_A, MARKER_B));
        }
    }

    /**
     * Never completes eval; timeout returns without completing (nor throwing). Simulates the
     * canonical mis-use of the contract: user issues another async call and stores the future for a
     * later callback. Codegen must detect the un-done future and fail-fast.
     */
    public static class NoCompletionTimeoutTableFunction extends AsyncTableFunction<String> {

        private static final long serialVersionUID = 1L;

        public void eval(CompletableFuture<Collection<String>> future, Long key) {
            // Never completes — only timeout() ever runs.
        }

        public void timeout(CompletableFuture<Collection<String>> future, Long key) {
            // Intentionally do nothing — neither complete nor throw. The codegen-emitted
            // isDone() check is the only thing standing between this and a hung ResultHandler.
        }
    }

    /** Never completes eval; declares NO timeout method (default fallback). */
    public static class NoTimeoutTableFunction extends AsyncTableFunction<String> {

        private static final long serialVersionUID = 1L;

        public void eval(CompletableFuture<Collection<String>> future, Long key) {
            // Never completes — only the framework default timeout runs.
        }
        // NOTE: Intentionally NO timeout(...) method declared here.
    }

    /**
     * Declares an incompatible {@code timeout(...)} signature ({@code String} where the BIGINT key
     * requires {@code Long}) — codegen must reject with a {@link ValidationException}.
     */
    public static class IncompatibleTimeoutTableFunction extends AsyncTableFunction<String> {

        private static final long serialVersionUID = 1L;

        public void eval(CompletableFuture<Collection<String>> future, Long key) {
            future.complete(Arrays.asList("blah", "foo"));
        }

        public void timeout(CompletableFuture<Collection<String>> future, String wrongKey) {
            future.completeExceptionally(
                    new CustomCorrelateTimeoutException(
                            "Should never be reached — codegen must reject this signature."));
        }
    }
}
