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
import org.apache.flink.table.planner.factories.TimeoutAsyncLookupTableFactory.CustomLookupTimeoutException;
import org.apache.flink.table.planner.factories.TimeoutAsyncLookupTableFactory.DefaultResultTimeoutAsyncTableFunction;
import org.apache.flink.table.planner.factories.TimeoutAsyncLookupTableFactory.IncompatibleTimeoutAsyncTableFunction;
import org.apache.flink.table.planner.factories.TimeoutAsyncLookupTableFactory.OverloadedTimeoutAsyncTableFunction;
import org.apache.flink.table.planner.factories.TimeoutAsyncLookupTableFactory.ProviderType;
import org.apache.flink.table.planner.runtime.utils.StreamAbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end coverage for the user-defined {@code timeout(...)} convention declared on {@link
 * AsyncTableFunction}. The custom-timeout contract lives on {@code AsyncTableFunction} itself
 * (signature lookup, fail-fast validation, default fallback), so these cases exercise the three
 * contract branches with UDFs that extend {@code AsyncTableFunction} directly — without relying on
 * the convenience subclass {@code AsyncLookupFunction}.
 *
 * <p>Cases covered:
 *
 * <ul>
 *   <li>A valid {@code timeout(...)} on {@code AsyncTableFunction} runs and replaces the framework
 *       default {@link TimeoutException} with the user's exception.
 *   <li>A {@code timeout(...)} that completes the future with a default row routes that row
 *       downstream instead of failing the job.
 *   <li>An {@code AsyncTableFunction} without any {@code timeout(...)} method falls back to the
 *       framework default {@link TimeoutException} (no codegen failure).
 *   <li>An {@code AsyncTableFunction} declaring an incompatible {@code timeout(...)} signature is
 *       rejected by codegen with {@link ValidationException} before submission.
 * </ul>
 *
 * <p>The shared {@code AsyncLookupFunction} coverage (parameterized over both providers) lives in
 * {@code AsyncLookupJoinTimeoutITCase}; this class intentionally narrows the surface to the {@code
 * AsyncTableFunction} contract owner.
 */
class AsyncTableFunctionTimeoutITCase extends StreamAbstractTestBase {

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    @BeforeEach
    void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        tEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());
    }

    /**
     * An {@link AsyncTableFunction} subclass that declares a valid {@code
     * timeout(CompletableFuture, Object...)} must have its handler invoked on timeout, surfacing
     * the user-defined {@link CustomLookupTimeoutException} instead of the framework default {@link
     * TimeoutException}.
     */
    @Test
    void testCustomTimeoutFallbackForAsyncTableFunction() {
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT,
                        Duration.ofMillis(200));

        createTables(/* lookupDelay */ "3s", ProviderType.ASYNC_TABLE_FUNCTION);

        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink_t\n"
                                + "SELECT s.id, d.name FROM source_t AS s\n"
                                + "LEFT JOIN dim_t FOR SYSTEM_TIME AS OF s.proc AS d\n"
                                + "ON s.id = d.id");

        assertThatThrownBy(result::await)
                .satisfies(anyCauseMatches(CustomLookupTimeoutException.class));
    }

    /**
     * An {@link AsyncTableFunction} subclass that declares no {@code timeout(...)} method must keep
     * the legacy behaviour: the framework default {@link TimeoutException} fires. Codegen must not
     * fail just because the user omitted the handler.
     */
    @Test
    void testDefaultTimeoutWhenAsyncTableFunctionTimeoutAbsent() {
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT,
                        Duration.ofMillis(200));

        createTables(/* lookupDelay */ "3s", ProviderType.ASYNC_TABLE_FUNCTION_WITHOUT_TIMEOUT);

        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink_t\n"
                                + "SELECT s.id, d.name FROM source_t AS s\n"
                                + "LEFT JOIN dim_t FOR SYSTEM_TIME AS OF s.proc AS d\n"
                                + "ON s.id = d.id");

        assertThatThrownBy(result::await).satisfies(anyCauseMatches(TimeoutException.class));
    }

    /**
     * When the user's {@code timeout(...)} method calls {@code future.complete(<defaultRow>)}
     * instead of {@code completeExceptionally(...)}, the job must finish successfully and every
     * timed-out lookup must surface the default payload downstream. Drives the source 1..5 through
     * a forced 3-second lookup delay against a 200 ms timeout, then collects the join result and
     * asserts that all five rows carry the {@code "DEFAULT"} marker supplied by the {@code
     * timeout(...)} fallback.
     */
    @Test
    void testCustomTimeoutReturnsDefaultRow() {
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT,
                        Duration.ofMillis(200));

        createTables(
                /* lookupDelay */ "3s",
                ProviderType.ASYNC_TABLE_FUNCTION_WITH_DEFAULT_RESULT_TIMEOUT);

        List<Row> rows =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "SELECT s.id, d.name FROM source_t AS s\n"
                                                + "LEFT JOIN dim_t FOR SYSTEM_TIME AS OF s.proc"
                                                + " AS d\n"
                                                + "ON s.id = d.id")
                                .collect());

        // source_t is a bounded datagen sequence 1..5, so we expect five rows; every one of them
        // must have been routed through the user's timeout(...) and carry the DEFAULT marker.
        assertThat(rows).hasSize(5);
        assertThat(rows)
                .allSatisfy(
                        row ->
                                assertThat(row.getField(1))
                                        .isEqualTo(
                                                DefaultResultTimeoutAsyncTableFunction
                                                        .DEFAULT_VALUE_MARKER));
    }

    /**
     * Verifies LEFT OUTER lookup-join semantics under the user-defined timeout path: when the
     * user's {@code timeout(...)} completes with an EMPTY collection, every timed-out left row must
     * still flow downstream with the right side padded with NULL — not dropped. Drives the source
     * 1..5 through a 3s lookup delay against a 200 ms timeout and asserts that all five rows arrive
     * with {@code d.name == null}.
     */
    @Test
    void testCustomTimeoutEmptyCollectionPadsNullsForLeftOuterJoin() {
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT,
                        Duration.ofMillis(200));

        createTables(
                /* lookupDelay */ "3s",
                ProviderType.ASYNC_TABLE_FUNCTION_WITH_EMPTY_RESULT_TIMEOUT);

        List<Row> rows =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "SELECT s.id, d.name FROM source_t AS s\n"
                                                + "LEFT JOIN dim_t FOR SYSTEM_TIME AS OF s.proc"
                                                + " AS d\n"
                                                + "ON s.id = d.id")
                                .collect());

        assertThat(rows).hasSize(5);
        assertThat(rows).allSatisfy(row -> assertThat(row.getField(1)).isNull());
        assertThat(
                        rows.stream()
                                .map(row -> ((Number) row.getField(0)).longValue())
                                .collect(Collectors.toList()))
                .containsExactly(1L, 2L, 3L, 4L, 5L);
    }

    /**
     * Verifies that a UDF declaring multiple {@code timeout(...)} overloads compiles and the
     * matching overload (mirroring the {@code eval(future, keys...)} signature) is the one that
     * actually runs at timeout. Decoy overloads — wrong parameter type and wrong arity — must not
     * be invoked; if either were picked the future would complete exceptionally with {@link
     * CustomLookupTimeoutException} and the assertion would fail. The matching overload completes
     * with a fallback row carrying {@link
     * OverloadedTimeoutAsyncTableFunction#OVERLOAD_VALUE_MARKER}, so finishing cleanly with that
     * marker proves the right method was dispatched.
     */
    @Test
    void testTimeoutMethodOverloadResolvesToMatchingSignature() {
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT,
                        Duration.ofMillis(200));

        createTables(
                /* lookupDelay */ "3s", ProviderType.ASYNC_TABLE_FUNCTION_WITH_OVERLOADED_TIMEOUT);

        List<Row> rows =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "SELECT s.id, d.name FROM source_t AS s\n"
                                                + "LEFT JOIN dim_t FOR SYSTEM_TIME AS OF s.proc"
                                                + " AS d\n"
                                                + "ON s.id = d.id")
                                .collect());

        assertThat(rows).hasSize(5);
        assertThat(rows)
                .allSatisfy(
                        row ->
                                assertThat(row.getField(1))
                                        .isEqualTo(
                                                OverloadedTimeoutAsyncTableFunction
                                                        .OVERLOAD_VALUE_MARKER));
    }

    /**
     * The user's {@code timeout(...)} must complete the future <em>before it returns</em>. Codegen
     * enforces this on the lookup-join path the same way as on the correlate path by checking
     * {@code delegates.getCompletableFuture().isDone()} after the call and forcing {@link
     * IllegalStateException} if it's not — symmetric coverage to {@code
     * AsyncCorrelateTimeoutITCase#testTimeoutNotCompletingFutureFailsFast}. The UDF here returns
     * without touching the future, and the job must fail with that codegen-emitted exception.
     */
    @Test
    void testLookupTimeoutNotCompletingFutureFailsFast() {
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT,
                        Duration.ofMillis(200));

        createTables(
                /* lookupDelay */ "3s",
                ProviderType.ASYNC_TABLE_FUNCTION_WITH_NO_COMPLETION_TIMEOUT);

        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink_t\n"
                                + "SELECT s.id, d.name FROM source_t AS s\n"
                                + "LEFT JOIN dim_t FOR SYSTEM_TIME AS OF s.proc AS d\n"
                                + "ON s.id = d.id");

        assertThatThrownBy(result::await)
                .satisfies(anyCauseMatches(IllegalStateException.class, "synchronously"));
    }

    /**
     * An {@link AsyncTableFunction} subclass declaring a {@code timeout(...)} whose parameter list
     * is incompatible with the lookup keys (here: {@code String} where the BIGINT key requires
     * {@code Long}) must be rejected by codegen with a {@link ValidationException} carrying the
     * UDF's fully-qualified class name plus the expected and actual signatures.
     */
    @Test
    void testIncompatibleTimeoutSignatureOnAsyncTableFunctionFailsFast() {
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT,
                        Duration.ofMillis(200));

        createTables(
                /* lookupDelay */ "3s",
                ProviderType.ASYNC_TABLE_FUNCTION_WITH_INCOMPATIBLE_TIMEOUT);

        // Codegen runs during executeSql planning, so ValidationException must surface here —
        // before any job is submitted or task is initialised.
        assertThatThrownBy(
                        () ->
                                tEnv.executeSql(
                                        "INSERT INTO sink_t\n"
                                                + "SELECT s.id, d.name FROM source_t AS s\n"
                                                + "LEFT JOIN dim_t FOR SYSTEM_TIME AS OF s.proc AS d\n"
                                                + "ON s.id = d.id"))
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                IncompatibleTimeoutAsyncTableFunction.class.getName()))
                .satisfies(anyCauseMatches(ValidationException.class, "CompletableFuture"))
                .satisfies(anyCauseMatches(ValidationException.class, "Long"));
    }

    private void createTables(String lookupDelay, ProviderType providerType) {
        // Bounded datagen source — small sequence so the test finishes quickly.
        tEnv.executeSql(
                "CREATE TABLE source_t (\n"
                        + "  id BIGINT,\n"
                        + "  proc AS PROCTIME()\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'fields.id.kind' = 'sequence',\n"
                        + "  'fields.id.start' = '1',\n"
                        + "  'fields.id.end' = '5'\n"
                        + ")");

        // Dim connector backed by an AsyncTableFunction variant selected via provider-type.
        // returns-data=false keeps the test focused on timeout dispatch.
        tEnv.executeSql(
                "CREATE TABLE dim_t (\n"
                        + "  id BIGINT,\n"
                        + "  name STRING,\n"
                        + "  PRIMARY KEY (id) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'timeout-async-lookup',\n"
                        + "  'lookup-delay' = '"
                        + lookupDelay
                        + "',\n"
                        + "  'returns-data' = 'false',\n"
                        + "  'provider-type' = '"
                        + providerType.name()
                        + "'\n"
                        + ")");

        // Blackhole sink — discards everything, satisfies INSERT INTO without external state.
        tEnv.executeSql(
                "CREATE TABLE sink_t (\n"
                        + "  id BIGINT,\n"
                        + "  name STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'blackhole'\n"
                        + ")");
    }
}
