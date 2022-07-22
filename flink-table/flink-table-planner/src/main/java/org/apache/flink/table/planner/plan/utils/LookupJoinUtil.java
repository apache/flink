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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.source.AsyncTableFunctionProvider;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.lookup.AsyncLookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingAsyncLookupProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.planner.codegen.LookupJoinCodeGenerator;
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.runtime.connector.source.LookupRuntimeProviderContext;
import org.apache.flink.table.runtime.operators.join.lookup.LookupCacheHandler;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonSubTypes;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rex.RexLiteral;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

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

    /** Gets lookup keys sorted by index in ascending order. */
    public static int[] getOrderedLookupKeys(Collection<Integer> allLookupKeys) {
        List<Integer> lookupKeyIndicesInOrder = new ArrayList<>(allLookupKeys);
        lookupKeyIndicesInOrder.sort(Integer::compareTo);
        return lookupKeyIndicesInOrder.stream().mapToInt(Integer::intValue).toArray();
    }

    /** Gets LookupFunction from temporal table according to the given lookup keys. */
    public static UserDefinedFunction getLookupFunction(
            RelOptTable temporalTable, Collection<Integer> lookupKeys) {
        if (temporalTable instanceof TableSourceTable) {
            LookupTableSource tableSource = getLookupTableSource(temporalTable);
            LookupTableSource.LookupRuntimeProvider provider =
                    tableSource.getLookupRuntimeProvider(createLookupContext(lookupKeys));
            if (provider instanceof LookupFunctionProvider) {
                return ((LookupFunctionProvider) provider).createLookupFunction();
            } else if (provider instanceof AsyncLookupFunctionProvider) {
                return ((AsyncLookupFunctionProvider) provider).createAsyncLookupFunction();
            } else if (provider instanceof TableFunctionProvider) {
                return ((TableFunctionProvider<?>) provider).createTableFunction();
            } else if (provider instanceof AsyncTableFunctionProvider) {
                return ((AsyncTableFunctionProvider<?>) provider).createAsyncTableFunction();
            }
        }

        if (temporalTable instanceof LegacyTableSourceTable) {
            int[] lookupKeyIndicesInOrder = getOrderedLookupKeys(lookupKeys);
            String[] lookupFieldNamesInOrder =
                    IntStream.of(lookupKeyIndicesInOrder)
                            .mapToObj(temporalTable.getRowType().getFieldNames()::get)
                            .toArray(String[]::new);
            LegacyTableSourceTable<?> legacyTableSourceTable =
                    (LegacyTableSourceTable<?>) temporalTable;
            LookupableTableSource<?> tableSource =
                    (LookupableTableSource<?>) legacyTableSourceTable.tableSource();
            if (tableSource.isAsyncEnabled()) {
                return tableSource.getAsyncLookupFunction(lookupFieldNamesInOrder);
            } else {
                return tableSource.getLookupFunction(lookupFieldNamesInOrder);
            }
        }
        throw new TableException(
                String.format(
                        "table %s is neither TableSourceTable not LegacyTableSourceTable",
                        temporalTable.getQualifiedName()));
    }

    public static Optional<LookupCacheHandler> getPartialLookupCacheHandler(
            RelOptTable temporalTable,
            ReadableConfig tableConfig,
            ClassLoader classLoader,
            RowType leftTableRowType,
            RowType rightTableRowType,
            Map<Integer, LookupKey> lookupKeys,
            boolean enableObjectReuse) {
        // Legacy table source does not support lookup caching
        if (temporalTable instanceof LegacyTableSourceTable) {
            return Optional.empty();
        }
        LookupTableSource lookupTableSource = getLookupTableSource(temporalTable);
        LookupTableSource.LookupRuntimeProvider provider =
                lookupTableSource.getLookupRuntimeProvider(
                        createLookupContext(lookupKeys.keySet()));
        if (provider instanceof PartialCachingLookupProvider) {
            LookupCache cache = ((PartialCachingLookupProvider) provider).getCache();
            if (cache == null) {
                return Optional.empty();
            } else {
                return buildLookupCacheHandler(
                        temporalTable,
                        tableConfig,
                        classLoader,
                        leftTableRowType,
                        rightTableRowType,
                        lookupKeys,
                        enableObjectReuse,
                        cache);
            }
        } else if (provider instanceof PartialCachingAsyncLookupProvider) {
            LookupCache cache = ((PartialCachingAsyncLookupProvider) provider).getCache();
            if (cache == null) {
                return Optional.empty();
            } else {
                return buildLookupCacheHandler(
                        temporalTable,
                        tableConfig,
                        classLoader,
                        leftTableRowType,
                        rightTableRowType,
                        lookupKeys,
                        enableObjectReuse,
                        cache);
            }
        }
        return Optional.empty();
    }

    private static Optional<LookupCacheHandler> buildLookupCacheHandler(
            RelOptTable temporalTable,
            ReadableConfig tableConfig,
            ClassLoader classLoader,
            RowType leftTableRowType,
            RowType rightTableRowType,
            Map<Integer, LookupKey> lookupKeys,
            boolean enableObjectReuse,
            LookupCache cache) {
        RowType keyRowType =
                RowType.of(
                        lookupKeys.values().stream()
                                .filter(key -> key instanceof FieldRefLookupKey)
                                .map(key -> ((FieldRefLookupKey) key).index)
                                .map(leftTableRowType::getTypeAt)
                                .toArray(LogicalType[]::new));
        if (enableObjectReuse) {
            return Optional.of(
                    new LookupCacheHandler(
                            cache,
                            StringUtils.join(temporalTable.getQualifiedName(), "."),
                            LookupJoinCodeGenerator.generateLeftTableKeyProjection(
                                    tableConfig,
                                    classLoader,
                                    leftTableRowType,
                                    keyRowType,
                                    lookupKeys)));
        } else {
            return Optional.of(
                    new LookupCacheHandler(
                            cache,
                            StringUtils.join(temporalTable.getQualifiedName(), "."),
                            LookupJoinCodeGenerator.generateLeftTableKeyProjection(
                                    tableConfig,
                                    classLoader,
                                    leftTableRowType,
                                    keyRowType,
                                    lookupKeys),
                            new RowDataSerializer(keyRowType),
                            new RowDataSerializer(rightTableRowType)));
        }
    }

    private static LookupTableSource getLookupTableSource(RelOptTable temporalTable) {
        return ((LookupTableSource) ((TableSourceTable) temporalTable).tableSource());
    }

    private static LookupTableSource.LookupContext createLookupContext(
            Collection<Integer> lookupKeys) {
        int[] lookupKeyIndicesInOrder = getOrderedLookupKeys(lookupKeys);
        // TODO: support nested lookup keys in the future,
        //  currently we only support top-level lookup keys
        int[][] indices =
                IntStream.of(lookupKeyIndicesInOrder)
                        .mapToObj(i -> new int[] {i})
                        .toArray(int[][]::new);
        return new LookupRuntimeProviderContext(indices);
    }
}
