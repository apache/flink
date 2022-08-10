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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.TableFunctionProvider;
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
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.spec.LookupJoinHintSpec;
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
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexLiteral;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.apache.flink.table.runtime.operators.join.lookup.ResultRetryStrategy.NO_RETRY_STRATEGY;

/** Utilities for lookup joins using {@link LookupTableSource}. */
@Internal
public final class LookupJoinUtil {

    /** A field used as an equal condition when querying content from a dimension table. */
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    @JsonSubTypes({
        @JsonSubTypes.Type(value = ConstantLookupKey.class),
        @JsonSubTypes.Type(value = FieldRefLookupKey.class)
    })
    public static class LookupKey {
        private LookupKey() {
            // sealed class
        }
    }

    /** A {@link LookupKey} whose value is constant. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeName("Constant")
    public static class ConstantLookupKey extends LookupKey {
        public static final String FIELD_NAME_SOURCE_TYPE = "sourceType";
        public static final String FIELD_NAME_LITERAL = "literal";

        @JsonProperty(FIELD_NAME_SOURCE_TYPE)
        public final LogicalType sourceType;

        @JsonProperty(FIELD_NAME_LITERAL)
        public final RexLiteral literal;

        @JsonCreator
        public ConstantLookupKey(
                @JsonProperty(FIELD_NAME_SOURCE_TYPE) LogicalType sourceType,
                @JsonProperty(FIELD_NAME_LITERAL) RexLiteral literal) {
            this.sourceType = sourceType;
            this.literal = literal;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ConstantLookupKey that = (ConstantLookupKey) o;
            return Objects.equals(sourceType, that.sourceType)
                    && Objects.equals(literal, that.literal);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sourceType, literal);
        }
    }

    /** A {@link LookupKey} whose value comes from the left table field. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonTypeName("FieldRef")
    public static class FieldRefLookupKey extends LookupKey {
        public static final String FIELD_NAME_INDEX = "index";

        @JsonProperty(FIELD_NAME_INDEX)
        public final int index;

        @JsonCreator
        public FieldRefLookupKey(@JsonProperty(FIELD_NAME_INDEX) int index) {
            this.index = index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FieldRefLookupKey that = (FieldRefLookupKey) o;
            return index == that.index;
        }

        @Override
        public int hashCode() {
            return Objects.hash(index);
        }
    }

    private LookupJoinUtil() {
        // no instantiation
    }

    /** AsyncLookupOptions includes async related options. */
    public static class AsyncLookupOptions {
        public final int asyncBufferCapacity;
        public final long asyncTimeout;
        public final ExecutionConfigOptions.AsyncOutputMode asyncOutputMode;

        public AsyncLookupOptions(
                int asyncBufferCapacity,
                long asyncTimeout,
                ExecutionConfigOptions.AsyncOutputMode asyncOutputMode) {
            this.asyncBufferCapacity = asyncBufferCapacity;
            this.asyncTimeout = asyncTimeout;
            this.asyncOutputMode = asyncOutputMode;
        }
    }

    private static class LookupFunctionCandidates {
        UserDefinedFunction syncLookupFunction;
        UserDefinedFunction asyncLookupFunction;
    }

    /** Gets lookup keys sorted by index in ascending order. */
    public static int[] getOrderedLookupKeys(Collection<Integer> allLookupKeys) {
        List<Integer> lookupKeyIndicesInOrder = new ArrayList<>(allLookupKeys);
        lookupKeyIndicesInOrder.sort(Integer::compareTo);
        return lookupKeyIndicesInOrder.stream().mapToInt(Integer::intValue).toArray();
    }

    /**
     * Gets lookup function (async or sync) from temporal table according to the given lookup keys
     * with considering {@link LookupJoinHintSpec} and required upsertMaterialize. Note: if required
     * upsertMaterialize is true, will return synchronous lookup function only, otherwise prefers
     * asynchronous lookup function except there's a hint option 'async' = 'false', will raise an
     * error if both candidates not found.
     *
     * <pre>{@code
     * 1. if upsertMaterialize == true : require sync lookup or else error
     *
     * 2. preferAsync = except there is a hint option 'async' = 'false'
     *  if (preferAsync) {
     *    async lookup != null ? async : sync or else error
     *  } else {
     *    sync lookup != null ? sync : async or else error
     *  }
     * }</pre>
     */
    public static UserDefinedFunction getLookupFunction(
            RelOptTable temporalTable,
            Collection<Integer> lookupKeys,
            ClassLoader classLoader,
            LookupJoinHintSpec joinHintSpec,
            boolean upsertMaterialize) {
        // async & sync lookup candidates
        LookupFunctionCandidates lookupFunctionCandidates = new LookupFunctionCandidates();

        // prefer (not require) by default
        boolean asyncLookup = LookupJoinUtil.preferAsync(joinHintSpec);
        boolean require = false;
        if (upsertMaterialize) {
            // upsertMaterialize only works on sync lookup mode, async lookup is unsupported.
            require = true;
            asyncLookup = false;
        }

        int[] lookupKeyIndicesInOrder = getOrderedLookupKeys(lookupKeys);
        if (temporalTable instanceof TableSourceTable) {
            findLookupFunctionFromNewSource(
                    (TableSourceTable) temporalTable,
                    lookupKeyIndicesInOrder,
                    joinHintSpec,
                    classLoader,
                    lookupFunctionCandidates);
        }
        if (temporalTable instanceof LegacyTableSourceTable) {
            findLookupFunctionFromLegacySource(
                    (LegacyTableSourceTable<?>) temporalTable,
                    lookupKeyIndicesInOrder,
                    lookupFunctionCandidates);
        }
        UserDefinedFunction selectLookupFunction =
                selectLookupFunction(
                        lookupFunctionCandidates.asyncLookupFunction,
                        lookupFunctionCandidates.syncLookupFunction,
                        require,
                        asyncLookup);

        if (null == selectLookupFunction) {
            StringBuilder errorMsg = new StringBuilder();
            if (require) {
                errorMsg.append("Required ")
                        .append(asyncLookup ? "async" : "sync")
                        .append(" lookup function by planner, but ");
            }
            errorMsg.append("table ")
                    .append(temporalTable.getQualifiedName())
                    .append(
                            "does not offer a valid lookup function neither as TableSourceTable nor LegacyTableSourceTable");
            throw new TableException(errorMsg.toString());
        }
        return selectLookupFunction;
    }

    /**
     * Wraps LookupFunction into a RetryableLookupFunctionDelegator to support retry. Note: only
     * LookupFunction is supported.
     */
    private static LookupFunction wrapSyncRetryDelegator(
            LookupFunctionProvider provider, LookupJoinHintSpec joinHintSpec) {
        if (joinHintSpec != null) {
            ResultRetryStrategy retryStrategy = joinHintSpec.toRetryStrategy();
            if (retryStrategy != NO_RETRY_STRATEGY) {
                return new RetryableLookupFunctionDelegator(
                        provider.createLookupFunction(), joinHintSpec.toRetryStrategy());
            }
        }
        return provider.createLookupFunction();
    }

    /**
     * Wraps AsyncLookupFunction into a RetryableAsyncLookupFunctionDelegator to support retry.
     * Note: only AsyncLookupFunction is supported.
     */
    private static AsyncLookupFunction wrapASyncRetryDelegator(
            AsyncLookupFunctionProvider provider, LookupJoinHintSpec joinHintSpec) {
        if (joinHintSpec != null) {
            ResultRetryStrategy retryStrategy = joinHintSpec.toRetryStrategy();
            if (retryStrategy != NO_RETRY_STRATEGY) {
                return new RetryableAsyncLookupFunctionDelegator(
                        provider.createAsyncLookupFunction(), joinHintSpec.toRetryStrategy());
            }
        }
        return provider.createAsyncLookupFunction();
    }

    private static void findLookupFunctionFromNewSource(
            TableSourceTable temporalTable,
            int[] lookupKeyIndicesInOrder,
            LookupJoinHintSpec joinHintSpec,
            ClassLoader classLoader,
            LookupFunctionCandidates lookupFunctionCandidates) {
        UserDefinedFunction syncLookupFunction = null;
        UserDefinedFunction asyncLookupFunction = null;

        // TODO: support nested lookup keys in the future,
        //  currently we only support top-level lookup keys
        int[][] indices =
                IntStream.of(lookupKeyIndicesInOrder)
                        .mapToObj(i -> new int[] {i})
                        .toArray(int[][]::new);

        LookupTableSource tableSource = (LookupTableSource) temporalTable.tableSource();
        LookupRuntimeProviderContext providerContext = new LookupRuntimeProviderContext(indices);
        LookupTableSource.LookupRuntimeProvider provider =
                tableSource.getLookupRuntimeProvider(providerContext);

        if (provider instanceof LookupFunctionProvider) {
            if (provider instanceof PartialCachingLookupProvider) {
                PartialCachingLookupProvider partialCachingLookupProvider =
                        (PartialCachingLookupProvider) provider;
                syncLookupFunction =
                        new CachingLookupFunction(
                                partialCachingLookupProvider.getCache(),
                                wrapSyncRetryDelegator(partialCachingLookupProvider, joinHintSpec));
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
                syncLookupFunction =
                        new CachingLookupFunction(
                                fullCache, fullCachingLookupProvider.createLookupFunction());
            } else {
                syncLookupFunction =
                        wrapSyncRetryDelegator((LookupFunctionProvider) provider, joinHintSpec);
            }
        }
        if (provider instanceof AsyncLookupFunctionProvider) {
            if (provider instanceof PartialCachingAsyncLookupProvider) {
                PartialCachingAsyncLookupProvider partialCachingLookupProvider =
                        (PartialCachingAsyncLookupProvider) provider;
                asyncLookupFunction =
                        new CachingAsyncLookupFunction(
                                partialCachingLookupProvider.getCache(),
                                wrapASyncRetryDelegator(
                                        partialCachingLookupProvider, joinHintSpec));
            } else {
                asyncLookupFunction =
                        wrapASyncRetryDelegator(
                                (AsyncLookupFunctionProvider) provider, joinHintSpec);
            }
        }
        if (provider instanceof TableFunctionProvider) {
            syncLookupFunction = ((TableFunctionProvider<?>) provider).createTableFunction();
        }
        if (provider instanceof AsyncTableFunctionProvider) {
            asyncLookupFunction =
                    ((AsyncTableFunctionProvider<?>) provider).createAsyncTableFunction();
        }
        setLookupFunctions(lookupFunctionCandidates, asyncLookupFunction, syncLookupFunction);
    }

    private static void setLookupFunctions(
            LookupFunctionCandidates lookupFunctionCandidates,
            UserDefinedFunction asyncLookupFunction,
            UserDefinedFunction syncLookupFunction) {
        if (asyncLookupFunction != null) {
            lookupFunctionCandidates.asyncLookupFunction = asyncLookupFunction;
        }
        if (syncLookupFunction != null) {
            lookupFunctionCandidates.syncLookupFunction = syncLookupFunction;
        }
    }

    private static void findLookupFunctionFromLegacySource(
            LegacyTableSourceTable temporalTable,
            int[] lookupKeyIndicesInOrder,
            LookupFunctionCandidates lookupFunctionCandidates) {
        UserDefinedFunction syncLookupFunction = null;
        UserDefinedFunction asyncLookupFunction = null;
        String[] lookupFieldNamesInOrder =
                IntStream.of(lookupKeyIndicesInOrder)
                        .mapToObj(temporalTable.getRowType().getFieldNames()::get)
                        .toArray(String[]::new);
        LegacyTableSourceTable<?> legacyTableSourceTable =
                (LegacyTableSourceTable<?>) temporalTable;
        LookupableTableSource<?> tableSource =
                (LookupableTableSource<?>) legacyTableSourceTable.tableSource();
        if (tableSource.isAsyncEnabled()) {
            asyncLookupFunction = tableSource.getAsyncLookupFunction(lookupFieldNamesInOrder);
        }
        syncLookupFunction = tableSource.getLookupFunction(lookupFieldNamesInOrder);
        setLookupFunctions(lookupFunctionCandidates, asyncLookupFunction, syncLookupFunction);
    }

    private static UserDefinedFunction selectLookupFunction(
            UserDefinedFunction asyncLookupFunction,
            UserDefinedFunction syncLookupFunction,
            boolean require,
            boolean async) {
        if (require) {
            return async ? asyncLookupFunction : syncLookupFunction;
        } else {
            if (async) {
                // prefer async
                return null != asyncLookupFunction ? asyncLookupFunction : syncLookupFunction;
            }
            // prefer sync
            return null != syncLookupFunction ? syncLookupFunction : asyncLookupFunction;
        }
    }

    public static boolean preferAsync(LookupJoinHintSpec lookupJoinHintSpec) {
        // async option has no default value, prefer async except async option is false
        return null == lookupJoinHintSpec
                || null == lookupJoinHintSpec.getAsync()
                || lookupJoinHintSpec.isAsync();
    }

    public static boolean isAsyncLookup(
            RelOptTable temporalTable,
            Collection<Integer> lookupKeys,
            LookupJoinHintSpec lookupJoinHintSpec) {
        boolean preferAsync = preferAsync(lookupJoinHintSpec);
        if (temporalTable instanceof TableSourceTable) {
            LookupTableSource.LookupRuntimeProvider provider =
                    getLookupRuntimeProvider(temporalTable, lookupKeys);
            return preferAsync
                    && (provider instanceof AsyncLookupFunctionProvider
                            || provider instanceof AsyncTableFunctionProvider);
        } else if (temporalTable instanceof LegacyTableSourceTable) {
            LegacyTableSourceTable<?> legacyTableSourceTable =
                    (LegacyTableSourceTable<?>) temporalTable;
            LookupableTableSource<?> lookupableTableSource =
                    (LookupableTableSource<?>) legacyTableSourceTable.tableSource();
            return preferAsync && lookupableTableSource.isAsyncEnabled();
        }
        throw new TableException(
                String.format(
                        "table %s is neither TableSourceTable not LegacyTableSourceTable",
                        temporalTable.getQualifiedName()));
    }

    private static LookupTableSource.LookupRuntimeProvider getLookupRuntimeProvider(
            RelOptTable temporalTable, Collection<Integer> lookupKeys) {
        int[] lookupKeyIndicesInOrder = getOrderedLookupKeys(lookupKeys);
        // TODO: support nested lookup keys in the future,
        //  currently we only support top-level lookup keys
        int[][] indices =
                IntStream.of(lookupKeyIndicesInOrder)
                        .mapToObj(i -> new int[] {i})
                        .toArray(int[][]::new);
        LookupTableSource tableSource =
                (LookupTableSource) ((TableSourceTable) temporalTable).tableSource();
        LookupRuntimeProviderContext providerContext = new LookupRuntimeProviderContext(indices);
        return tableSource.getLookupRuntimeProvider(providerContext);
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
}
