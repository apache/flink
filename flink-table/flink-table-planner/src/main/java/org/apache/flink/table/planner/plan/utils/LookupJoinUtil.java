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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.legacy.table.connector.source.SourceFunctionProvider;
import org.apache.flink.streaming.util.retryable.RetryPredicates;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.LookupJoinHintOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsLookupCustomShuffle;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.FullCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.AsyncLookupFunction;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.legacy.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.legacy.connector.source.TableFunctionProvider;
import org.apache.flink.table.legacy.sources.LookupableTableSource;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.runtime.connector.source.LookupRuntimeProviderContext;
import org.apache.flink.table.runtime.functions.table.lookup.CachingAsyncLookupFunction;
import org.apache.flink.table.runtime.functions.table.lookup.CachingLookupFunction;
import org.apache.flink.table.runtime.functions.table.lookup.fullcache.CacheLoader;
import org.apache.flink.table.runtime.functions.table.lookup.fullcache.LookupFullCache;
import org.apache.flink.table.runtime.functions.table.lookup.fullcache.inputformat.InputFormatCacheLoader;
import org.apache.flink.table.runtime.keyselector.GenericRowDataKeySelector;
import org.apache.flink.table.runtime.operators.join.lookup.ResultRetryStrategy;
import org.apache.flink.table.runtime.operators.join.lookup.RetryableAsyncLookupFunctionDelegator;
import org.apache.flink.table.runtime.operators.join.lookup.RetryableLookupFunctionDelegator;
import org.apache.flink.table.runtime.partitioner.RowDataCustomStreamPartitioner;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.hint.RelHint;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.config.LookupJoinHintOptions.ASYNC_CAPACITY;
import static org.apache.flink.table.api.config.LookupJoinHintOptions.ASYNC_LOOKUP;
import static org.apache.flink.table.api.config.LookupJoinHintOptions.ASYNC_OUTPUT_MODE;
import static org.apache.flink.table.api.config.LookupJoinHintOptions.ASYNC_TIMEOUT;
import static org.apache.flink.table.api.config.LookupJoinHintOptions.FIXED_DELAY;
import static org.apache.flink.table.api.config.LookupJoinHintOptions.MAX_ATTEMPTS;
import static org.apache.flink.table.api.config.LookupJoinHintOptions.RETRY_PREDICATE;
import static org.apache.flink.table.api.config.LookupJoinHintOptions.RETRY_STRATEGY;
import static org.apache.flink.table.api.config.LookupJoinHintOptions.SHUFFLE;
import static org.apache.flink.table.runtime.operators.join.lookup.ResultRetryStrategy.NO_RETRY_STRATEGY;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utilities for lookup joins using {@link LookupTableSource}. */
@Internal
public final class LookupJoinUtil extends FunctionCallUtil {

    /** ShuffleLookupOptions includes shuffle related options. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeName("ShuffleOptions")
    public static class ShuffleLookupOptions {
        public static final String FIELD_NAME_SHUFFLE = "shuffle";
    }

    /** RetryOptions includes retry lookup related options. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeName("RetryLookupOptions")
    public static class RetryLookupOptions {
        public static final String FIELD_NAME_RETRY_PREDICATE = "retry-predicate";
        public static final String FIELD_NAME_RETRY_STRATEGY = "retry-strategy";
        public static final String FIELD_NAME_RETRY_FIXED_DELAY = "fixed-delay";
        public static final String FIELD_NAME_RETRY_MAX_ATTEMPTS = "max-attempts";

        @JsonProperty(FIELD_NAME_RETRY_PREDICATE)
        private final String retryPredicate;

        @JsonProperty(FIELD_NAME_RETRY_STRATEGY)
        private final LookupJoinHintOptions.RetryStrategy retryStrategy;

        @JsonProperty(FIELD_NAME_RETRY_FIXED_DELAY)
        private final Long retryFixedDelay;

        @JsonProperty(FIELD_NAME_RETRY_MAX_ATTEMPTS)
        private final Integer retryMaxAttempts;

        @JsonCreator
        public RetryLookupOptions(
                @JsonProperty(FIELD_NAME_RETRY_PREDICATE) String retryPredicate,
                @JsonProperty(FIELD_NAME_RETRY_STRATEGY)
                        LookupJoinHintOptions.RetryStrategy retryStrategy,
                @JsonProperty(FIELD_NAME_RETRY_FIXED_DELAY) Long retryFixedDelay,
                @JsonProperty(FIELD_NAME_RETRY_MAX_ATTEMPTS) Integer retryMaxAttempts) {
            this.retryPredicate = checkNotNull(retryPredicate);
            this.retryStrategy = checkNotNull(retryStrategy);
            this.retryFixedDelay = checkNotNull(retryFixedDelay);
            this.retryMaxAttempts = checkNotNull(retryMaxAttempts);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RetryLookupOptions that = (RetryLookupOptions) o;
            return Objects.equals(retryPredicate, that.retryPredicate)
                    && retryStrategy == that.retryStrategy
                    && Objects.equals(retryFixedDelay, that.retryFixedDelay)
                    && Objects.equals(retryMaxAttempts, that.retryMaxAttempts);
        }

        @Override
        public int hashCode() {
            return Objects.hash(retryPredicate, retryStrategy, retryFixedDelay, retryMaxAttempts);
        }

        @Override
        public String toString() {
            return retryPredicate
                    + ", "
                    + retryStrategy
                    + ", "
                    + retryFixedDelay
                    + "ms, "
                    + retryMaxAttempts;
        }

        @Nullable
        public static RetryLookupOptions fromJoinHint(@Nullable RelHint lookupJoinHint) {
            if (null != lookupJoinHint) {
                Configuration conf = Configuration.fromMap(lookupJoinHint.kvOptions);
                Duration fixedDelay = conf.get(FIXED_DELAY);
                if (fixedDelay != null) {
                    return new RetryLookupOptions(
                            conf.get(RETRY_PREDICATE),
                            conf.get(RETRY_STRATEGY),
                            fixedDelay.toMillis(),
                            conf.get(MAX_ATTEMPTS));
                }
            }
            return null;
        }

        /**
         * Convert this {@link RetryLookupOptions} to {@link ResultRetryStrategy} in the best effort
         * manner. If invalid {@link LookupJoinHintOptions#RETRY_PREDICATE} or {@link
         * LookupJoinHintOptions#RETRY_STRATEGY} is given, then {@link
         * ResultRetryStrategy#NO_RETRY_STRATEGY} will return.
         */
        @JsonIgnore
        @SuppressWarnings("unchecked")
        public ResultRetryStrategy toRetryStrategy() {
            if (!LookupJoinHintOptions.LOOKUP_MISS_PREDICATE.equalsIgnoreCase(retryPredicate)
                    || retryStrategy != LookupJoinHintOptions.RetryStrategy.FIXED_DELAY) {
                return NO_RETRY_STRATEGY;
            }
            // retry option values have been validated by hint checker
            return ResultRetryStrategy.fixedDelayRetry(
                    this.retryMaxAttempts,
                    this.retryFixedDelay,
                    RetryPredicates.EMPTY_RESULT_PREDICATE);
        }
    }

    private LookupJoinUtil() {
        // no instantiation
    }

    /** Gets lookup keys sorted by index in ascending order. */
    public static int[] getOrderedLookupKeys(Collection<Integer> allLookupKeys) {
        List<Integer> lookupKeyIndicesInOrder = new ArrayList<>(allLookupKeys);
        lookupKeyIndicesInOrder.sort(Integer::compareTo);
        return lookupKeyIndicesInOrder.stream().mapToInt(Integer::intValue).toArray();
    }

    public static AsyncOptions getMergedAsyncOptions(
            RelHint lookupHint, TableConfig config, ChangelogMode inputChangelogMode) {
        Configuration confFromHint;
        if (lookupHint == null) {
            confFromHint = new Configuration();
        } else {
            confFromHint = Configuration.fromMap(lookupHint.kvOptions);
        }
        ExecutionConfigOptions.AsyncOutputMode asyncOutputMode =
                coalesce(
                        confFromHint.get(ASYNC_OUTPUT_MODE),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_OUTPUT_MODE));

        boolean keyOrdered =
                isKeyOrdered(
                        inputChangelogMode,
                        asyncOutputMode,
                        config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_KEY_ORDERED));

        return new AsyncOptions(
                coalesce(
                        confFromHint.get(ASYNC_CAPACITY),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_BUFFER_CAPACITY)),
                coalesce(
                                confFromHint.get(ASYNC_TIMEOUT),
                                config.get(ExecutionConfigOptions.TABLE_EXEC_ASYNC_LOOKUP_TIMEOUT))
                        .toMillis(),
                keyOrdered,
                convert(inputChangelogMode, asyncOutputMode));
    }

    /**
     * This method determines whether async lookup is enabled according to the given lookup keys
     * with considering lookup {@link RelHint} and required upsertMaterialize. Note: it will not
     * create the function instance to avoid potential heavy cost during optimization phase. if
     * required upsertMaterialize is true, will return synchronous lookup function only, otherwise
     * prefers asynchronous lookup function except there's a hint option 'async' = 'false', will
     * raise an error if both candidates not found.
     *
     * <pre>{@code
     * 1. if upsertMaterialize == true : return false
     *
     * 2. preferAsync = except there is a hint option 'async' = 'false'
     *  if (preferAsync) {
     *    return asyncFound ? true : false
     *  } else {
     *    return syncFound ? false : true
     *  }
     * }</pre>
     */
    public static boolean isAsyncLookup(
            RelOptTable temporalTable,
            Collection<Integer> lookupKeys,
            @Nullable RelHint lookupHint,
            boolean upsertMaterialize,
            boolean preferCustomShuffle) {
        // prefer (not require) by default
        boolean preferAsync = preferAsync(lookupHint);
        if (upsertMaterialize) {
            // upsertMaterialize only works on sync lookup mode, async lookup is unsupported.
            return false;
        }
        boolean syncFound = false;
        boolean asyncFound = false;
        if (temporalTable instanceof TableSourceTable) {
            int[] lookupKeyIndicesInOrder = getOrderedLookupKeys(lookupKeys);
            LookupTableSource.LookupRuntimeProvider provider =
                    createLookupRuntimeProvider(
                            temporalTable, lookupKeyIndicesInOrder, preferCustomShuffle);
            if (provider instanceof LookupFunctionProvider
                    || provider instanceof TableFunctionProvider) {
                syncFound = true;
            }
            if (provider instanceof AsyncLookupFunctionProvider
                    || provider instanceof AsyncTableFunctionProvider) {
                asyncFound = true;
            }
        } else if (temporalTable instanceof LegacyTableSourceTable) {
            LegacyTableSourceTable<?> legacyTableSourceTable =
                    (LegacyTableSourceTable<?>) temporalTable;
            LookupableTableSource<?> tableSource =
                    (LookupableTableSource<?>) legacyTableSourceTable.tableSource();
            if (tableSource.isAsyncEnabled()) {
                asyncFound = true;
            } else {
                syncFound = true;
            }
        }
        if (!syncFound && !asyncFound) {
            throw new TableException(
                    String.format(
                            "table %s is neither TableSourceTable not LegacyTableSourceTable",
                            temporalTable.getQualifiedName()));
        }
        return preferAsync ? asyncFound : !syncFound;
    }

    /**
     * Gets required lookup function (async or sync) from temporal table , will raise an error if
     * specified lookup function instance not found.
     */
    public static UserDefinedFunction getLookupFunction(
            RelOptTable temporalTable,
            Collection<Integer> lookupKeys,
            ClassLoader classLoader,
            boolean async,
            ResultRetryStrategy retryStrategy,
            boolean preferCustomShuffle) {
        UserDefinedFunction lookupFunction = null;
        int[] lookupKeyIndicesInOrder = getOrderedLookupKeys(lookupKeys);
        if (temporalTable instanceof TableSourceTable) {
            lookupFunction =
                    findLookupFunctionFromNewSource(
                            (TableSourceTable) temporalTable,
                            lookupKeyIndicesInOrder,
                            retryStrategy,
                            async,
                            classLoader,
                            preferCustomShuffle);
        } else if (temporalTable instanceof LegacyTableSourceTable) {
            lookupFunction =
                    findLookupFunctionFromLegacySource(
                            (LegacyTableSourceTable<?>) temporalTable,
                            lookupKeyIndicesInOrder,
                            async);
        }
        if (null == lookupFunction) {
            StringBuilder errorMsg = new StringBuilder();
            errorMsg.append("Required ")
                    .append(async ? "async" : "sync")
                    .append(" lookup function by planner, but table ")
                    .append(temporalTable.getQualifiedName())
                    .append(
                            "does not offer a valid lookup function neither as TableSourceTable nor LegacyTableSourceTable");
            throw new TableException(errorMsg.toString());
        }
        return lookupFunction;
    }

    /**
     * Evaluates if prefer async lookup by given lookup {@link RelHint}. Returns true except async
     * option in hint is false.
     */
    private static boolean preferAsync(@Nullable RelHint lookupHint) {
        // async option has no default value, prefer async except async option is false
        if (null == lookupHint) {
            return true;
        }
        Configuration conf = Configuration.fromMap(lookupHint.kvOptions);
        Boolean async = conf.get(ASYNC_LOOKUP);
        return null == async || async;
    }

    /**
     * Wraps LookupFunction into a RetryableLookupFunctionDelegator to support retry. Note: only
     * LookupFunction is supported.
     */
    private static LookupFunction wrapSyncRetryDelegator(
            LookupFunctionProvider provider, ResultRetryStrategy retryStrategy) {
        if (retryStrategy != null && retryStrategy != NO_RETRY_STRATEGY) {
            return new RetryableLookupFunctionDelegator(
                    provider.createLookupFunction(), retryStrategy);
        }
        return provider.createLookupFunction();
    }

    /**
     * Wraps AsyncLookupFunction into a RetryableAsyncLookupFunctionDelegator to support retry.
     * Note: only AsyncLookupFunction is supported.
     */
    private static AsyncLookupFunction wrapASyncRetryDelegator(
            AsyncLookupFunctionProvider provider, ResultRetryStrategy retryStrategy) {
        if (retryStrategy != null && retryStrategy != NO_RETRY_STRATEGY) {
            return new RetryableAsyncLookupFunctionDelegator(
                    provider.createAsyncLookupFunction(), retryStrategy);
        }
        return provider.createAsyncLookupFunction();
    }

    private static UserDefinedFunction findLookupFunctionFromNewSource(
            TableSourceTable temporalTable,
            int[] lookupKeyIndicesInOrder,
            ResultRetryStrategy retryStrategy,
            boolean async,
            ClassLoader classLoader,
            boolean preferCustomShuffle) {
        LookupTableSource.LookupRuntimeProvider provider =
                createLookupRuntimeProvider(
                        temporalTable, lookupKeyIndicesInOrder, preferCustomShuffle);

        if (async) {
            if (provider instanceof AsyncLookupFunctionProvider) {
                if (provider instanceof PartialCachingAsyncLookupProvider) {
                    PartialCachingAsyncLookupProvider partialCachingLookupProvider =
                            (PartialCachingAsyncLookupProvider) provider;
                    return new CachingAsyncLookupFunction(
                            partialCachingLookupProvider.getCache(),
                            wrapASyncRetryDelegator(partialCachingLookupProvider, retryStrategy));
                } else {
                    return wrapASyncRetryDelegator(
                            (AsyncLookupFunctionProvider) provider, retryStrategy);
                }
            }
            if (provider instanceof AsyncTableFunctionProvider) {
                return ((AsyncTableFunctionProvider<?>) provider).createAsyncTableFunction();
            }
        } else {
            if (provider instanceof LookupFunctionProvider) {
                if (provider instanceof PartialCachingLookupProvider) {
                    PartialCachingLookupProvider partialCachingLookupProvider =
                            (PartialCachingLookupProvider) provider;
                    return new CachingLookupFunction(
                            partialCachingLookupProvider.getCache(),
                            wrapSyncRetryDelegator(partialCachingLookupProvider, retryStrategy));
                } else if (provider instanceof FullCachingLookupProvider) {
                    FullCachingLookupProvider fullCachingLookupProvider =
                            (FullCachingLookupProvider) provider;
                    RowType tableSourceRowType =
                            FlinkTypeFactory.toLogicalRowType(temporalTable.getRowType());
                    LookupFullCache fullCache =
                            createFullCache(
                                    fullCachingLookupProvider,
                                    lookupKeyIndicesInOrder,
                                    classLoader,
                                    tableSourceRowType);
                    // retry on fullCachingLookupFunction is meaningless
                    return new CachingLookupFunction(
                            fullCache, fullCachingLookupProvider.createLookupFunction());
                } else {
                    return wrapSyncRetryDelegator((LookupFunctionProvider) provider, retryStrategy);
                }
            }
            if (provider instanceof TableFunctionProvider) {
                return ((TableFunctionProvider<?>) provider).createTableFunction();
            }
        }
        return null;
    }

    /** Determine whether to enable shuffle before lookup join. */
    public static boolean enableLookupShuffle(RelHint lookupJoinHint) {
        if (null != lookupJoinHint) {
            Configuration conf = Configuration.fromMap(lookupJoinHint.kvOptions);
            return conf.get(SHUFFLE);
        }
        return false;
    }

    /** Determine whether table provides custom partitioner. */
    public static boolean tableProvidesCustomPartitioner(RelOptTable table) {
        if (table instanceof TableSourceTable) {
            DynamicTableSource dynamicTableSource = ((TableSourceTable) table).tableSource();
            return dynamicTableSource instanceof LookupTableSource
                    && dynamicTableSource instanceof SupportsLookupCustomShuffle;
        }
        return false;
    }

    private static UserDefinedFunction findLookupFunctionFromLegacySource(
            LegacyTableSourceTable temporalTable, int[] lookupKeyIndicesInOrder, boolean async) {
        String[] lookupFieldNamesInOrder =
                IntStream.of(lookupKeyIndicesInOrder)
                        .mapToObj(temporalTable.getRowType().getFieldNames()::get)
                        .toArray(String[]::new);
        LegacyTableSourceTable<?> legacyTableSourceTable =
                (LegacyTableSourceTable<?>) temporalTable;
        LookupableTableSource<?> tableSource =
                (LookupableTableSource<?>) legacyTableSourceTable.tableSource();
        // respect the definition of LookupableTableSource#isAsyncEnabled
        if (async && tableSource.isAsyncEnabled()) {
            return tableSource.getAsyncLookupFunction(lookupFieldNamesInOrder);
        }
        if (!async && !tableSource.isAsyncEnabled()) {
            return tableSource.getLookupFunction(lookupFieldNamesInOrder);
        }
        return null;
    }

    private static LookupTableSource.LookupRuntimeProvider createLookupRuntimeProvider(
            RelOptTable temporalTable, int[] lookupKeyIndicesInOrder, boolean preferCustomShuffle) {
        // TODO: support nested lookup keys in the future,
        //  currently we only support top-level lookup keys
        int[][] indices =
                IntStream.of(lookupKeyIndicesInOrder)
                        .mapToObj(i -> new int[] {i})
                        .toArray(int[][]::new);

        LookupTableSource tableSource =
                (LookupTableSource) ((TableSourceTable) temporalTable).tableSource();
        LookupRuntimeProviderContext providerContext =
                new LookupRuntimeProviderContext(indices, preferCustomShuffle);
        return tableSource.getLookupRuntimeProvider(providerContext);
    }

    /** Try to apply custom partitioner to the input transformation of lookup join. */
    public static Transformation<RowData> tryApplyCustomShufflePartitioner(
            PlannerBase planner,
            RelOptTable table,
            RowType inputRowType,
            Map<Integer, FunctionParam> allLookupKeys,
            Transformation<RowData> inputTransformation,
            ChangelogMode inputChangelogMode,
            TransformationMetadata metadata) {
        Optional<SupportsLookupCustomShuffle.InputDataPartitioner> partitioner =
                ((SupportsLookupCustomShuffle) (((TableSourceTable) table).tableSource()))
                        .getPartitioner();
        // Empty partitioner means the connector expects the input data to remain in its original
        // distribution.
        if (partitioner.isEmpty()) {
            return inputTransformation;
        }
        if (!partitioner.get().isDeterministic()
                && !inputChangelogMode.containsOnly(RowKind.INSERT)) {
            return inputTransformation;
        }
        GenericRowDataKeySelector lookupTableKeySelector =
                (GenericRowDataKeySelector)
                        KeySelectorUtil.getLookupKeysSelectorFromLeftTable(
                                planner.getFlinkContext().getClassLoader(),
                                allLookupKeys,
                                InternalTypeInfo.of(inputRowType));
        RowDataCustomStreamPartitioner streamPartitioner =
                new RowDataCustomStreamPartitioner(partitioner.get(), lookupTableKeySelector);
        return ExecNodeUtil.createPartitionTransformation(
                inputTransformation, metadata, streamPartitioner);
    }

    private static LookupFullCache createFullCache(
            FullCachingLookupProvider provider,
            int[] lookupKeyIndicesInOrder,
            ClassLoader classLoader,
            RowType tableSourceRowType) {

        ScanTableSource.ScanRuntimeProvider scanProvider = provider.getScanRuntimeProvider();
        Preconditions.checkArgument(
                scanProvider.isBounded(),
                "ScanRuntimeProvider that is used for data loading in "
                        + "lookup 'FULL' cache must be bounded.");

        GenericRowDataKeySelector lookupTableKeySelector =
                (GenericRowDataKeySelector)
                        KeySelectorUtil.getRowDataSelector(
                                classLoader,
                                lookupKeyIndicesInOrder,
                                InternalTypeInfo.of(tableSourceRowType),
                                GenericRowData.class);

        if (scanProvider instanceof InputFormatProvider) {
            InputFormat<RowData, ?> inputFormat =
                    ((InputFormatProvider) scanProvider).createInputFormat();
            CacheLoader cacheLoader =
                    new InputFormatCacheLoader(
                            inputFormat,
                            lookupTableKeySelector,
                            InternalSerializers.create(tableSourceRowType));
            return new LookupFullCache(cacheLoader, provider.getCacheReloadTrigger());
        } else if (scanProvider instanceof SourceFunctionProvider) {
            // TODO support SourceFunctions
            throw new UnsupportedOperationException(
                    "Full caching using SourceFunction currently not supported.");
        } else {
            throw new UnsupportedOperationException(
                    "Currently only InputFormatProvider and SourceFunctionProvider are supported as ScanRuntimeProviders for Full caching lookup join.");
        }
    }

    private static boolean isKeyOrdered(
            ChangelogMode inputChangelogMode,
            ExecutionConfigOptions.AsyncOutputMode asyncOutputMode,
            boolean allowKeyOrdered) {
        return allowKeyOrdered
                && !inputChangelogMode.containsOnly(RowKind.INSERT)
                && asyncOutputMode == ExecutionConfigOptions.AsyncOutputMode.ALLOW_UNORDERED;
    }
}
