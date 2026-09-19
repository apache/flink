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
import org.apache.flink.table.planner.factories.TimeoutAsyncLookupTableFactory.CustomLookupTimeoutException;
import org.apache.flink.table.planner.factories.TimeoutAsyncLookupTableFactory.IncompatibleTimeoutAsyncTableFunction;
import org.apache.flink.table.planner.factories.TimeoutAsyncLookupTableFactory.ProviderType;
import org.apache.flink.table.planner.runtime.utils.StreamAbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end test that reproduces the timeout behaviour of {@code AsyncLookupJoinRunner} purely via
 * Flink SQL.
 *
 * <p>Topology:
 *
 * <ul>
 *   <li>Source — {@code datagen} bounded sequence (records 1..N)
 *   <li>Dim — custom {@code timeout-async-lookup} connector with a configurable per-lookup sleep,
 *       exposing either an {@link org.apache.flink.table.functions.AsyncLookupFunction} or the
 *       legacy {@link org.apache.flink.table.functions.AsyncTableFunction}
 *   <li>Sink — {@code blackhole}
 * </ul>
 *
 * <p>Both function variants expose a (non-standard) {@code timeout(...)} method that raises {@link
 * CustomLookupTimeoutException}. The user-defined timeout handler runs in place of the framework
 * default {@link java.util.concurrent.TimeoutException TimeoutException} regardless of whether the
 * timeout is set via {@code table.exec.async-lookup.timeout} or a per-join {@code
 * LOOKUP('timeout'=...)} hint.
 *
 * <p>How the user-side {@code timeout()} is reached: {@code FunctionCodeGenerator.scala} now
 * appends a {@code timeout(input, ResultFuture)} override to the generated {@code AsyncFunction}
 * subclass, reusing the same body as {@code asyncInvoke} with {@code eval} rewritten to {@code
 * timeout}. So when the {@code AsyncWaitOperator} fires the timeout timer, it calls into the
 * generated function's {@code timeout(...)}, which delegates to the wrapped UDF's {@code
 * timeout(future, keys...)}. The legacy {@link org.apache.flink.table.functions.AsyncTableFunction
 * AsyncTableFunction} variant matches that signature directly; the new {@link
 * org.apache.flink.table.functions.AsyncLookupFunction AsyncLookupFunction} variant exposes the
 * same shape via the inherited reflective {@code eval(future, keys...)} machinery (see the
 * non-standard {@code timeout(future, keys...)} method declared on the test UDF for symmetry).
 *
 * <p>Each parameterized timeout test runs once per {@link ProviderType}. {@link
 * #testNoTimeoutCompletesSuccessfully()} stays as a single sanity case ensuring the scaffolding
 * does not accidentally raise the custom exception when no timeout fires.
 */
class AsyncLookupJoinTimeoutITCase extends StreamAbstractTestBase {

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
     * Configures the async-lookup timeout via {@code table.exec.async-lookup.timeout} (200 ms) and
     * forces every record to time out by setting the dim's per-lookup delay to 3 s. Asserts that
     * the user-defined {@code timeout(...)} on the UDF runs and surfaces {@link
     * CustomLookupTimeoutException} instead of the framework default {@code TimeoutException}.
     *
     * <p>Runs once per {@link ProviderType} — new {@code AsyncLookupFunction} and legacy {@code
     * AsyncTableFunction}.
     */
    @ParameterizedTest(name = "providerType={0}")
    @EnumSource(
            value = ProviderType.class,
            names = {"ASYNC_LOOKUP_FUNCTION", "ASYNC_TABLE_FUNCTION"})
    void testCustomTimeoutViaConfig(ProviderType providerType) {
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT,
                        Duration.ofMillis(200));

        createTables(/* lookupDelay */ "3s", /* returnsData */ false, providerType);

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
     * Same assertion as {@link #testCustomTimeoutViaConfig(ProviderType)}, but the per-job global
     * timeout is permissive (30 s) and the 200 ms timeout is pinned per-join via the {@code
     * LOOKUP('timeout'=...)} hint. Verifies that the hint path also routes through the user's
     * custom {@code timeout(...)}.
     *
     * <p>Runs once per {@link ProviderType}.
     */
    @ParameterizedTest(name = "providerType={0}")
    @EnumSource(
            value = ProviderType.class,
            names = {"ASYNC_LOOKUP_FUNCTION", "ASYNC_TABLE_FUNCTION"})
    void testCustomTimeoutViaLookupHint(ProviderType providerType) {
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT,
                        Duration.ofSeconds(30));

        createTables(/* lookupDelay */ "3s", /* returnsData */ false, providerType);

        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink_t\n"
                                + "SELECT /*+ LOOKUP('table'='d', 'async'='true', 'timeout'='200 ms') */\n"
                                + "       s.id, d.name FROM source_t AS s\n"
                                + "LEFT JOIN dim_t FOR SYSTEM_TIME AS OF s.proc AS d\n"
                                + "ON s.id = d.id");

        assertThatThrownBy(result::await)
                .satisfies(anyCauseMatches(CustomLookupTimeoutException.class));
    }

    /**
     * Verifies that an {@link org.apache.flink.table.functions.AsyncTableFunction} works correctly
     * even when the user does not declare a {@code timeout(...)} method. Expected behaviour: the
     * operator falls back to the framework default {@link java.util.concurrent.TimeoutException
     * TimeoutException} just like before the custom-timeout feature was added — i.e. the absence of
     * a user-supplied timeout handler must not crash codegen.
     */
    @Test
    void testFallbackToDefaultTimeoutWhenUserTimeoutAbsent() {
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT,
                        Duration.ofMillis(200));

        createTables(
                /* lookupDelay */ "3s",
                /* returnsData */ false,
                ProviderType.ASYNC_TABLE_FUNCTION_WITHOUT_TIMEOUT);

        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink_t\n"
                                + "SELECT s.id, d.name FROM source_t AS s\n"
                                + "LEFT JOIN dim_t FOR SYSTEM_TIME AS OF s.proc AS d\n"
                                + "ON s.id = d.id");

        // Without a user-defined timeout method, the framework default must fire — surfacing
        // java.util.concurrent.TimeoutException, NOT CustomLookupTimeoutException and NOT a
        // codegen-compilation error.
        assertThatThrownBy(result::await).satisfies(anyCauseMatches(TimeoutException.class));
    }

    /**
     * Verifies that a UDF declaring a {@code timeout(...)} method whose parameter list is
     * incompatible with the lookup keys is rejected by codegen with a {@link ValidationException}
     * at SQL compilation time (i.e. before the job is ever submitted). The error message must
     * mention the UDF's fully-qualified class name plus the expected and actual signatures so a
     * user can locate the bad method quickly.
     */
    @Test
    void testIncompatibleTimeoutSignatureFailsFast() {
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT,
                        Duration.ofMillis(200));

        createTables(
                /* lookupDelay */ "3s",
                /* returnsData */ false,
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

    /**
     * When the lookup delay (50 ms) is well below the configured timeout (10 s), no timeout should
     * fire and the async lookup should complete normally. Because the test connector echoes the
     * join key into `d.id` when `returns-data=true`, we can assert more than "job succeeded": each
     * row should preserve the left id and populate the right id with the same value.
     */
    @Test
    void testNoTimeoutCompletesSuccessfully() {
        tEnv.getConfig()
                .set(
                        ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT,
                        Duration.ofSeconds(10));

        createTables(
                /* lookupDelay */ "50ms",
                /* returnsData */ true,
                ProviderType.ASYNC_LOOKUP_FUNCTION);

        List<Row> rows =
                CollectionUtil.iteratorToList(
                        tEnv.executeSql(
                                        "SELECT s.id, d.id, d.name FROM source_t AS s\n"
                                                + "LEFT JOIN dim_t FOR SYSTEM_TIME AS OF s.proc AS d\n"
                                                + "ON s.id = d.id")
                                .collect());

        assertThat(rows).hasSize(5);
        assertThat(rows)
                .allSatisfy(
                        row -> {
                            assertThat(row.getField(1)).isEqualTo(row.getField(0));
                            assertThat(row.getField(2)).isNull();
                        });
    }

    private void createTables(String lookupDelay, boolean returnsData, ProviderType providerType) {
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

        // Custom dim connector exposing async lookup with a configurable delay and provider type.
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
                        + "  'returns-data' = '"
                        + returnsData
                        + "',\n"
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
