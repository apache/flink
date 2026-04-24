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

package org.apache.flink.table.planner.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.legacy.connector.source.AsyncTableFunctionProvider;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A test-only connector that exposes a configurable-delay async lookup, used to reproduce the
 * timeout behaviour of async lookup join (driven by the {@code AsyncWaitOperator} via {@code
 * AsyncLookupJoinRunner}).
 *
 * <p>Two provider modes are supported via the {@code provider-type} option:
 *
 * <ul>
 *   <li>{@code async-lookup-function} (default) — uses {@link AsyncLookupFunctionProvider} with an
 *       {@link AsyncLookupFunction} subclass.
 *   <li>{@code async-table-function} — uses the legacy {@link AsyncTableFunctionProvider} with an
 *       {@link AsyncTableFunction} that exposes an {@code eval(future, keys...)} method.
 * </ul>
 *
 * <p>Each lookup sleeps for {@code lookup-delay} before completing. By comparing this delay against
 * {@code table.exec.async-lookup.timeout} (or a per-join {@code LOOKUP('timeout'=...)} hint), a
 * test can deterministically trigger or avoid an async timeout.
 *
 * <p>Both function variants additionally expose a non-standard {@code timeout(...)} method that
 * completes the result future with {@link CustomLookupTimeoutException}. {@code
 * FunctionCodeGenerator.scala} appends a {@code timeout(input, ResultFuture)} override to the
 * generated {@code AsyncFunction} subclass that mirrors {@code asyncInvoke} with {@code eval}
 * rewritten to {@code timeout}, so the user-side {@code timeout(future, keys...)} declared below is
 * the one that ultimately runs when {@code AsyncWaitOperator} fires the timeout timer.
 */
public class TimeoutAsyncLookupTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "timeout-async-lookup";

    /** Selects which {@link LookupTableSource.LookupRuntimeProvider} the source returns. */
    public enum ProviderType {
        ASYNC_LOOKUP_FUNCTION,
        ASYNC_TABLE_FUNCTION,
        /**
         * Same as {@link #ASYNC_TABLE_FUNCTION} but the underlying UDF deliberately does NOT
         * declare a {@code timeout(...)} method. Used to verify that the framework still works
         * correctly (falls back to the default {@link java.util.concurrent.TimeoutException}) when
         * the user supplies no custom timeout handler.
         */
        ASYNC_TABLE_FUNCTION_WITHOUT_TIMEOUT,
        /**
         * Same as {@link #ASYNC_TABLE_FUNCTION} but the underlying UDF declares a {@code
         * timeout(...)} method whose parameter signature is incompatible with the lookup keys. Used
         * to verify that codegen fails fast with a {@link
         * org.apache.flink.table.api.ValidationException ValidationException} carrying the UDF's
         * FQN plus the expected and actual signatures.
         */
        ASYNC_TABLE_FUNCTION_WITH_INCOMPATIBLE_TIMEOUT,
        /**
         * Same as {@link #ASYNC_TABLE_FUNCTION} but the {@code timeout(...)} method completes the
         * future with a default row instead of an exception. The non-key columns are filled with
         * the literal {@code "DEFAULT"}. Used to verify that the framework correctly forwards a
         * synchronously-completed fallback result down the join pipeline so the job does not fail
         * and the timed-out records carry the default value.
         */
        ASYNC_TABLE_FUNCTION_WITH_DEFAULT_RESULT_TIMEOUT,
        /**
         * Same as {@link #ASYNC_TABLE_FUNCTION} but the {@code timeout(...)} method completes the
         * future with an empty collection. Used to verify that a LEFT OUTER lookup join correctly
         * pads the right side with NULL for every timed-out record (OUTER semantics must survive
         * the timeout path), rather than dropping the left rows or failing the job.
         */
        ASYNC_TABLE_FUNCTION_WITH_EMPTY_RESULT_TIMEOUT,
        /**
         * Same as {@link #ASYNC_TABLE_FUNCTION} but the UDF declares MULTIPLE {@code timeout(...)}
         * overloads — only one matches the lookup-key signature. Used to verify that codegen routes
         * to the matching overload (and that mere presence of extra overloads does not trip
         * validation), via Java's compile-time overload resolution on the generated {@code
         * functionTerm.timeout(...)} call site.
         */
        ASYNC_TABLE_FUNCTION_WITH_OVERLOADED_TIMEOUT,
        /**
         * Same as {@link #ASYNC_TABLE_FUNCTION} but the {@code timeout(...)} method returns without
         * completing the future (neither {@code complete} nor {@code completeExceptionally}). Used
         * to verify that the codegen-emitted {@code isDone()} check fires fast with an {@link
         * IllegalStateException} on the lookup-join path — symmetric coverage to the correlate-path
         * test {@code testTimeoutNotCompletingFutureFailsFast}.
         */
        ASYNC_TABLE_FUNCTION_WITH_NO_COMPLETION_TIMEOUT
    }

    public static final ConfigOption<Duration> LOOKUP_DELAY =
            ConfigOptions.key("lookup-delay")
                    .durationType()
                    .defaultValue(Duration.ZERO)
                    .withDescription(
                            "How long each async lookup blocks before completing. Set this larger "
                                    + "than table.exec.async-lookup.timeout to trigger a timeout.");

    public static final ConfigOption<Boolean> RETURNS_DATA =
            ConfigOptions.key("returns-data")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, the lookup returns a single row echoing the lookup key in "
                                    + "the corresponding columns and leaving the rest as nulls. "
                                    + "If false, the lookup returns an empty collection.");

    public static final ConfigOption<ProviderType> PROVIDER_TYPE =
            ConfigOptions.key("provider-type")
                    .enumType(ProviderType.class)
                    .defaultValue(ProviderType.ASYNC_LOOKUP_FUNCTION)
                    .withDescription(
                            "Which provider implementation to use: ASYNC_LOOKUP_FUNCTION (new "
                                    + "AsyncLookupFunctionProvider, default) or ASYNC_TABLE_FUNCTION "
                                    + "(legacy AsyncTableFunctionProvider).");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(LOOKUP_DELAY);
        options.add(RETURNS_DATA);
        options.add(PROVIDER_TYPE);
        return options;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();
        return new TimeoutAsyncLookupTableSource(
                options.get(LOOKUP_DELAY).toMillis(),
                options.get(RETURNS_DATA),
                options.get(PROVIDER_TYPE),
                context.getPhysicalRowDataType().getLogicalType().getChildren().size());
    }

    private static class TimeoutAsyncLookupTableSource implements LookupTableSource {

        private final long delayMillis;
        private final boolean returnsData;
        private final ProviderType providerType;
        private final int physicalFieldCount;

        TimeoutAsyncLookupTableSource(
                long delayMillis,
                boolean returnsData,
                ProviderType providerType,
                int physicalFieldCount) {
            this.delayMillis = delayMillis;
            this.returnsData = returnsData;
            this.providerType = providerType;
            this.physicalFieldCount = physicalFieldCount;
        }

        @Override
        public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
            int[] lookupKeyIndexes = Arrays.stream(context.getKeys()).mapToInt(k -> k[0]).toArray();
            switch (providerType) {
                case ASYNC_LOOKUP_FUNCTION:
                    return AsyncLookupFunctionProvider.of(
                            new TimeoutAsyncLookupFunction(
                                    delayMillis,
                                    returnsData,
                                    lookupKeyIndexes,
                                    physicalFieldCount));
                case ASYNC_TABLE_FUNCTION:
                    return AsyncTableFunctionProvider.of(
                            new TimeoutAsyncTableFunction(
                                    delayMillis,
                                    returnsData,
                                    lookupKeyIndexes,
                                    physicalFieldCount));
                case ASYNC_TABLE_FUNCTION_WITHOUT_TIMEOUT:
                    return AsyncTableFunctionProvider.of(
                            new NoTimeoutAsyncTableFunction(
                                    delayMillis,
                                    returnsData,
                                    lookupKeyIndexes,
                                    physicalFieldCount));
                case ASYNC_TABLE_FUNCTION_WITH_INCOMPATIBLE_TIMEOUT:
                    return AsyncTableFunctionProvider.of(
                            new IncompatibleTimeoutAsyncTableFunction(
                                    delayMillis,
                                    returnsData,
                                    lookupKeyIndexes,
                                    physicalFieldCount));
                case ASYNC_TABLE_FUNCTION_WITH_DEFAULT_RESULT_TIMEOUT:
                    return AsyncTableFunctionProvider.of(
                            new DefaultResultTimeoutAsyncTableFunction(
                                    delayMillis,
                                    returnsData,
                                    lookupKeyIndexes,
                                    physicalFieldCount));
                case ASYNC_TABLE_FUNCTION_WITH_EMPTY_RESULT_TIMEOUT:
                    return AsyncTableFunctionProvider.of(
                            new EmptyResultTimeoutAsyncTableFunction(
                                    delayMillis,
                                    returnsData,
                                    lookupKeyIndexes,
                                    physicalFieldCount));
                case ASYNC_TABLE_FUNCTION_WITH_OVERLOADED_TIMEOUT:
                    return AsyncTableFunctionProvider.of(
                            new OverloadedTimeoutAsyncTableFunction(
                                    delayMillis,
                                    returnsData,
                                    lookupKeyIndexes,
                                    physicalFieldCount));
                case ASYNC_TABLE_FUNCTION_WITH_NO_COMPLETION_TIMEOUT:
                    return AsyncTableFunctionProvider.of(
                            new NoCompletionTimeoutAsyncTableFunction(
                                    delayMillis,
                                    returnsData,
                                    lookupKeyIndexes,
                                    physicalFieldCount));
                default:
                    throw new IllegalArgumentException("Unknown provider type: " + providerType);
            }
        }

        @Override
        public DynamicTableSource copy() {
            return new TimeoutAsyncLookupTableSource(
                    delayMillis, returnsData, providerType, physicalFieldCount);
        }

        @Override
        public String asSummaryString() {
            return "TimeoutAsyncLookupTableSource(delayMillis="
                    + delayMillis
                    + ", returnsData="
                    + returnsData
                    + ", providerType="
                    + providerType
                    + ")";
        }
    }

    /**
     * Custom exception raised by the non-standard {@code timeout(...)} hooks below. Tests assert
     * this type to confirm that the user-defined timeout handler ran instead of falling back to
     * {@link java.util.concurrent.TimeoutException}.
     */
    public static class CustomLookupTimeoutException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public CustomLookupTimeoutException(String message) {
            super(message);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // AsyncLookupFunction-based variant (new API)
    // ---------------------------------------------------------------------------------------------

    /** An {@link AsyncLookupFunction} that sleeps before completing. */
    public static class TimeoutAsyncLookupFunction extends AsyncLookupFunction {

        private static final long serialVersionUID = 1L;

        private final long delayMillis;
        private final boolean returnsData;
        private final int[] lookupKeyIndexes;
        private final int physicalFieldCount;

        private transient ExecutorService executor;

        public TimeoutAsyncLookupFunction(
                long delayMillis,
                boolean returnsData,
                int[] lookupKeyIndexes,
                int physicalFieldCount) {
            this.delayMillis = delayMillis;
            this.returnsData = returnsData;
            this.lookupKeyIndexes = lookupKeyIndexes;
            this.physicalFieldCount = physicalFieldCount;
        }

        @Override
        public void open(FunctionContext context) {
            this.executor = Executors.newCachedThreadPool();
        }

        @Override
        public CompletableFuture<Collection<RowData>> asyncLookup(RowData keyRow) {
            return CompletableFuture.supplyAsync(
                    () -> {
                        if (delayMillis > 0) {
                            try {
                                Thread.sleep(delayMillis);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new RuntimeException(e);
                            }
                        }
                        if (!returnsData) {
                            return Collections.emptyList();
                        }
                        GenericRowData row = new GenericRowData(physicalFieldCount);
                        for (int i = 0; i < lookupKeyIndexes.length; i++) {
                            int outputIdx = lookupKeyIndexes[i];
                            row.setField(outputIdx, ((GenericRowData) keyRow).getField(i));
                        }
                        return Collections.singletonList(row);
                    },
                    executor);
        }

        public void timeout(CompletableFuture<Collection<RowData>> future, Object... keys) {
            future.completeExceptionally(
                    new CustomLookupTimeoutException(
                            "Custom timeout from TimeoutAsyncLookupFunction for keys "
                                    + Arrays.toString(keys)));
        }

        @Override
        public void close() {
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // AsyncTableFunction-based variant (legacy API)
    // ---------------------------------------------------------------------------------------------

    /**
     * An {@link AsyncTableFunction} that mirrors {@link TimeoutAsyncLookupFunction} but uses the
     * legacy reflection-based {@code eval(future, keys...)} signature.
     */
    public static class TimeoutAsyncTableFunction extends AsyncTableFunction<RowData> {

        private static final long serialVersionUID = 1L;

        private final long delayMillis;
        private final boolean returnsData;
        private final int[] lookupKeyIndexes;
        private final int physicalFieldCount;

        private transient ExecutorService executor;

        public TimeoutAsyncTableFunction(
                long delayMillis,
                boolean returnsData,
                int[] lookupKeyIndexes,
                int physicalFieldCount) {
            this.delayMillis = delayMillis;
            this.returnsData = returnsData;
            this.lookupKeyIndexes = lookupKeyIndexes;
            this.physicalFieldCount = physicalFieldCount;
        }

        @Override
        public void open(FunctionContext context) {
            this.executor = Executors.newCachedThreadPool();
        }

        public void eval(CompletableFuture<Collection<RowData>> future, Object... keys) {
            executor.submit(
                    () -> {
                        try {
                            if (delayMillis > 0) {
                                Thread.sleep(delayMillis);
                            }
                            if (!returnsData) {
                                future.complete(Collections.emptyList());
                                return;
                            }
                            GenericRowData row = new GenericRowData(physicalFieldCount);
                            for (int i = 0; i < lookupKeyIndexes.length; i++) {
                                row.setField(lookupKeyIndexes[i], keys[i]);
                            }
                            future.complete(Collections.singletonList(row));
                        } catch (Throwable t) {
                            future.completeExceptionally(t);
                        }
                    });
        }

        public void timeout(CompletableFuture<Collection<RowData>> future, Object... keys) {
            future.completeExceptionally(
                    new CustomLookupTimeoutException(
                            "Custom timeout from TimeoutAsyncTableFunction for keys "
                                    + Arrays.toString(keys)));
        }

        @Override
        public void close() {
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // AsyncTableFunction-based variant WITHOUT a user-supplied timeout method
    // ---------------------------------------------------------------------------------------------

    /**
     * Same shape as {@link TimeoutAsyncTableFunction} but deliberately omits the {@code
     * timeout(future, keys...)} method. Used to verify that the framework still works correctly
     * when the user does not override the timeout — the operator should fall back to the default
     * {@link java.util.concurrent.TimeoutException} instead of failing during codegen because the
     * generated {@code timeout(...)} forwards to a non-existent method.
     */
    public static class NoTimeoutAsyncTableFunction extends AsyncTableFunction<RowData> {

        private static final long serialVersionUID = 1L;

        private final long delayMillis;
        private final boolean returnsData;
        private final int[] lookupKeyIndexes;
        private final int physicalFieldCount;

        private transient ExecutorService executor;

        public NoTimeoutAsyncTableFunction(
                long delayMillis,
                boolean returnsData,
                int[] lookupKeyIndexes,
                int physicalFieldCount) {
            this.delayMillis = delayMillis;
            this.returnsData = returnsData;
            this.lookupKeyIndexes = lookupKeyIndexes;
            this.physicalFieldCount = physicalFieldCount;
        }

        @Override
        public void open(FunctionContext context) {
            this.executor = Executors.newCachedThreadPool();
        }

        public void eval(CompletableFuture<Collection<RowData>> future, Object... keys) {
            executor.submit(
                    () -> {
                        try {
                            if (delayMillis > 0) {
                                Thread.sleep(delayMillis);
                            }
                            if (!returnsData) {
                                future.complete(Collections.emptyList());
                                return;
                            }
                            GenericRowData row = new GenericRowData(physicalFieldCount);
                            for (int i = 0; i < lookupKeyIndexes.length; i++) {
                                row.setField(lookupKeyIndexes[i], keys[i]);
                            }
                            future.complete(Collections.singletonList(row));
                        } catch (Throwable t) {
                            future.completeExceptionally(t);
                        }
                    });
        }

        // NOTE: Intentionally NO timeout(...) method declared here.

        @Override
        public void close() {
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // AsyncTableFunction-based variant WITH an INCOMPATIBLE timeout(...) signature
    // ---------------------------------------------------------------------------------------------

    /**
     * Same shape as {@link TimeoutAsyncTableFunction} but the {@code timeout(...)} method's second
     * parameter is {@link String} instead of the actual lookup key type ({@link Long} for a BIGINT
     * key). The validation hook in {@link
     * org.apache.flink.table.functions.UserDefinedFunctionHelper#validateAsyncTableFunctionTimeoutClass}
     * must reject this signature with a {@link org.apache.flink.table.api.ValidationException
     * ValidationException} during codegen.
     */
    public static class IncompatibleTimeoutAsyncTableFunction extends AsyncTableFunction<RowData> {

        private static final long serialVersionUID = 1L;

        private final long delayMillis;
        private final boolean returnsData;
        private final int[] lookupKeyIndexes;
        private final int physicalFieldCount;

        private transient ExecutorService executor;

        public IncompatibleTimeoutAsyncTableFunction(
                long delayMillis,
                boolean returnsData,
                int[] lookupKeyIndexes,
                int physicalFieldCount) {
            this.delayMillis = delayMillis;
            this.returnsData = returnsData;
            this.lookupKeyIndexes = lookupKeyIndexes;
            this.physicalFieldCount = physicalFieldCount;
        }

        @Override
        public void open(FunctionContext context) {
            this.executor = Executors.newCachedThreadPool();
        }

        public void eval(CompletableFuture<Collection<RowData>> future, Object... keys) {
            executor.submit(
                    () -> {
                        try {
                            if (delayMillis > 0) {
                                Thread.sleep(delayMillis);
                            }
                            if (!returnsData) {
                                future.complete(Collections.emptyList());
                                return;
                            }
                            GenericRowData row = new GenericRowData(physicalFieldCount);
                            for (int i = 0; i < lookupKeyIndexes.length; i++) {
                                row.setField(lookupKeyIndexes[i], keys[i]);
                            }
                            future.complete(Collections.singletonList(row));
                        } catch (Throwable t) {
                            future.completeExceptionally(t);
                        }
                    });
        }

        // Deliberately incompatible: lookup key is BIGINT (Long), but this expects String.
        public void timeout(CompletableFuture<Collection<RowData>> future, String wrongKey) {
            future.completeExceptionally(
                    new CustomLookupTimeoutException(
                            "Should never be reached — codegen must reject this signature."));
        }

        @Override
        public void close() {
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // AsyncTableFunction-based variant whose timeout(...) returns a DEFAULT row instead of failing
    // ---------------------------------------------------------------------------------------------

    /**
     * Same shape as {@link TimeoutAsyncTableFunction} but the {@code timeout(...)} method completes
     * the result future with a single fallback row. The key columns echo the lookup key, and every
     * non-key column is filled with the marker string {@code "DEFAULT"}. Used to verify that the
     * runtime correctly forwards a {@code future.complete(...)} fallback emitted from the user's
     * {@code timeout(...)} down the join pipeline, so the job finishes successfully and the
     * timed-out records carry the default payload.
     */
    public static class DefaultResultTimeoutAsyncTableFunction extends AsyncTableFunction<RowData> {

        public static final String DEFAULT_VALUE_MARKER = "DEFAULT";

        private static final long serialVersionUID = 1L;

        private final long delayMillis;
        private final boolean returnsData;
        private final int[] lookupKeyIndexes;
        private final int physicalFieldCount;

        private transient ExecutorService executor;

        public DefaultResultTimeoutAsyncTableFunction(
                long delayMillis,
                boolean returnsData,
                int[] lookupKeyIndexes,
                int physicalFieldCount) {
            this.delayMillis = delayMillis;
            this.returnsData = returnsData;
            this.lookupKeyIndexes = lookupKeyIndexes;
            this.physicalFieldCount = physicalFieldCount;
        }

        @Override
        public void open(FunctionContext context) {
            this.executor = Executors.newCachedThreadPool();
        }

        public void eval(CompletableFuture<Collection<RowData>> future, Object... keys) {
            executor.submit(
                    () -> {
                        try {
                            if (delayMillis > 0) {
                                Thread.sleep(delayMillis);
                            }
                            if (!returnsData) {
                                future.complete(Collections.emptyList());
                                return;
                            }
                            GenericRowData row = new GenericRowData(physicalFieldCount);
                            for (int i = 0; i < lookupKeyIndexes.length; i++) {
                                row.setField(lookupKeyIndexes[i], keys[i]);
                            }
                            future.complete(Collections.singletonList(row));
                        } catch (Throwable t) {
                            future.completeExceptionally(t);
                        }
                    });
        }

        /**
         * Completes the future synchronously with a single default-valued row instead of an
         * exception. The key columns echo the lookup keys; every other column receives the marker
         * string so the test can distinguish a fallback row from a real lookup result.
         */
        public void timeout(CompletableFuture<Collection<RowData>> future, Object... keys) {
            GenericRowData row = new GenericRowData(physicalFieldCount);
            boolean[] isKeyColumn = new boolean[physicalFieldCount];
            for (int i = 0; i < lookupKeyIndexes.length; i++) {
                row.setField(lookupKeyIndexes[i], keys[i]);
                isKeyColumn[lookupKeyIndexes[i]] = true;
            }
            for (int i = 0; i < physicalFieldCount; i++) {
                if (!isKeyColumn[i]) {
                    row.setField(i, StringData.fromString(DEFAULT_VALUE_MARKER));
                }
            }
            future.complete(Collections.singletonList(row));
        }

        @Override
        public void close() {
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // AsyncTableFunction-based variant whose timeout(...) completes with an empty collection
    // ---------------------------------------------------------------------------------------------

    /**
     * Same shape as {@link TimeoutAsyncTableFunction} but the {@code timeout(...)} method completes
     * the result future with an EMPTY collection. Used to verify that the LEFT OUTER lookup-join
     * runtime preserves OUTER semantics under the user-defined timeout path — every timed-out left
     * row must be padded with NULL on the right, not dropped, and the job must finish cleanly.
     */
    public static class EmptyResultTimeoutAsyncTableFunction extends AsyncTableFunction<RowData> {

        private static final long serialVersionUID = 1L;

        private final long delayMillis;
        private final boolean returnsData;
        private final int[] lookupKeyIndexes;
        private final int physicalFieldCount;

        private transient ExecutorService executor;

        public EmptyResultTimeoutAsyncTableFunction(
                long delayMillis,
                boolean returnsData,
                int[] lookupKeyIndexes,
                int physicalFieldCount) {
            this.delayMillis = delayMillis;
            this.returnsData = returnsData;
            this.lookupKeyIndexes = lookupKeyIndexes;
            this.physicalFieldCount = physicalFieldCount;
        }

        @Override
        public void open(FunctionContext context) {
            this.executor = Executors.newCachedThreadPool();
        }

        public void eval(CompletableFuture<Collection<RowData>> future, Object... keys) {
            executor.submit(
                    () -> {
                        try {
                            if (delayMillis > 0) {
                                Thread.sleep(delayMillis);
                            }
                            if (!returnsData) {
                                future.complete(Collections.emptyList());
                                return;
                            }
                            GenericRowData row = new GenericRowData(physicalFieldCount);
                            for (int i = 0; i < lookupKeyIndexes.length; i++) {
                                row.setField(lookupKeyIndexes[i], keys[i]);
                            }
                            future.complete(Collections.singletonList(row));
                        } catch (Throwable t) {
                            future.completeExceptionally(t);
                        }
                    });
        }

        public void timeout(CompletableFuture<Collection<RowData>> future, Object... keys) {
            future.complete(Collections.emptyList());
        }

        @Override
        public void close() {
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // AsyncTableFunction-based variant with MULTIPLE timeout(...) overloads
    // ---------------------------------------------------------------------------------------------

    /**
     * Same shape as {@link TimeoutAsyncTableFunction} but declares TWO {@code timeout(...)}
     * overloads — only the {@code (CompletableFuture, Object...)} one matches the lookup-key
     * signature; the arity-3 overload exists purely to confirm that codegen resolves to the
     * matching overload via Java compile-time overload selection on the generated {@code
     * functionTerm.timeout(future, keys...)} call site (and that validation tolerates the extra
     * non-matching method). The matching overload completes with a fallback row whose non-key
     * columns carry the {@link #OVERLOAD_VALUE_MARKER} so the test can assert that it — and not the
     * decoy overload — actually ran.
     */
    public static class OverloadedTimeoutAsyncTableFunction extends AsyncTableFunction<RowData> {

        public static final String OVERLOAD_VALUE_MARKER = "OVERLOAD";

        private static final long serialVersionUID = 1L;

        private final long delayMillis;
        private final boolean returnsData;
        private final int[] lookupKeyIndexes;
        private final int physicalFieldCount;

        private transient ExecutorService executor;

        public OverloadedTimeoutAsyncTableFunction(
                long delayMillis,
                boolean returnsData,
                int[] lookupKeyIndexes,
                int physicalFieldCount) {
            this.delayMillis = delayMillis;
            this.returnsData = returnsData;
            this.lookupKeyIndexes = lookupKeyIndexes;
            this.physicalFieldCount = physicalFieldCount;
        }

        @Override
        public void open(FunctionContext context) {
            this.executor = Executors.newCachedThreadPool();
        }

        public void eval(CompletableFuture<Collection<RowData>> future, Object... keys) {
            executor.submit(
                    () -> {
                        try {
                            if (delayMillis > 0) {
                                Thread.sleep(delayMillis);
                            }
                            if (!returnsData) {
                                future.complete(Collections.emptyList());
                                return;
                            }
                            GenericRowData row = new GenericRowData(physicalFieldCount);
                            for (int i = 0; i < lookupKeyIndexes.length; i++) {
                                row.setField(lookupKeyIndexes[i], keys[i]);
                            }
                            future.complete(Collections.singletonList(row));
                        } catch (Throwable t) {
                            future.completeExceptionally(t);
                        }
                    });
        }

        /** Matching overload — must be picked by codegen's generated dispatch. */
        public void timeout(CompletableFuture<Collection<RowData>> future, Object... keys) {
            GenericRowData row = new GenericRowData(physicalFieldCount);
            boolean[] isKeyColumn = new boolean[physicalFieldCount];
            for (int i = 0; i < lookupKeyIndexes.length; i++) {
                row.setField(lookupKeyIndexes[i], keys[i]);
                isKeyColumn[lookupKeyIndexes[i]] = true;
            }
            for (int i = 0; i < physicalFieldCount; i++) {
                if (!isKeyColumn[i]) {
                    row.setField(i, StringData.fromString(OVERLOAD_VALUE_MARKER));
                }
            }
            future.complete(Collections.singletonList(row));
        }

        /** Decoy overload (wrong arity) — must NOT be invoked. */
        public void timeout(
                CompletableFuture<Collection<RowData>> future, Long key, Long extraArg) {
            future.completeExceptionally(
                    new CustomLookupTimeoutException(
                            "Decoy (arity-3) overload must never run — picked: " + key));
        }

        @Override
        public void close() {
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // AsyncTableFunction-based variant whose timeout(...) returns without completing the future
    // ---------------------------------------------------------------------------------------------

    /**
     * Same shape as {@link TimeoutAsyncTableFunction} but the {@code timeout(...)} method does
     * NOTHING — it returns without completing the future synchronously. Used to verify that the
     * codegen-emitted {@code isDone()} check on the lookup-join path fails fast with an {@link
     * IllegalStateException} (symmetric to the correlate-path test for the same contract).
     *
     * <p>Constructor arguments are kept identical to the other {@code
     * TimeoutAsyncLookupTableSource} variants so {@link TimeoutAsyncLookupTableSource} can
     * instantiate it uniformly; only {@code delayMillis} is actually consulted (to keep the
     * never-completing eval looping until the framework timer fires).
     */
    public static class NoCompletionTimeoutAsyncTableFunction extends AsyncTableFunction<RowData> {

        private static final long serialVersionUID = 1L;

        private final long delayMillis;

        private transient ExecutorService executor;

        public NoCompletionTimeoutAsyncTableFunction(
                long delayMillis,
                boolean returnsData,
                int[] lookupKeyIndexes,
                int physicalFieldCount) {
            this.delayMillis = delayMillis;
        }

        @Override
        public void open(FunctionContext context) {
            this.executor = Executors.newCachedThreadPool();
        }

        public void eval(CompletableFuture<Collection<RowData>> future, Object... keys) {
            // Schedule a sleep that outlives the configured timeout so timeout() is what runs.
            executor.submit(
                    () -> {
                        try {
                            if (delayMillis > 0) {
                                Thread.sleep(delayMillis);
                            }
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        }
                    });
        }

        /**
         * Intentionally returns without completing the future (no {@code complete}, no {@code
         * completeExceptionally}, no throw). The codegen-emitted {@code isDone()} check is the only
         * thing standing between this and a hung ResultHandler.
         */
        public void timeout(CompletableFuture<Collection<RowData>> future, Object... keys) {
            // no-op
        }

        @Override
        public void close() {
            if (executor != null) {
                executor.shutdownNow();
            }
        }
    }
}
