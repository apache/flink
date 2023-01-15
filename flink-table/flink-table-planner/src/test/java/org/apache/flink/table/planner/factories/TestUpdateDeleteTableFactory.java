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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsDeletePushDown;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.table.data.RowData.createFieldGetter;

/** A factory to create table to support update/delete for test purpose. */
public class TestUpdateDeleteTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "test-update-delete";

    private static final ConfigOption<String> DATA_ID =
            ConfigOptions.key("data-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The data id used to read the rows.");

    private static final ConfigOption<Boolean> ONLY_ACCEPT_EQUAL_PREDICATE =
            ConfigOptions.key("only-accept-equal-predicate")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether only accept when the all predicates in filter is equal expression for delete statement.");

    private static final ConfigOption<Boolean> SUPPORT_DELETE_PUSH_DOWN =
            ConfigOptions.key("support-delete-push-down")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Whether the table supports delete push down.");

    private static final AtomicInteger idCounter = new AtomicInteger(0);
    private static final Map<String, Collection<RowData>> registeredRowData = new HashMap<>();

    public static String registerRowData(Collection<RowData> data) {
        String id = String.valueOf(idCounter.incrementAndGet());
        registeredRowData.put(id, data);
        return id;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        String dataId =
                helper.getOptions().getOptional(DATA_ID).orElse(String.valueOf(idCounter.get()));
        if (helper.getOptions().get(SUPPORT_DELETE_PUSH_DOWN)) {
            return new SupportsDeletePushDownSink(
                    dataId,
                    helper.getOptions().get(ONLY_ACCEPT_EQUAL_PREDICATE),
                    context.getCatalogTable());
        } else {
            return new TestSink();
        }
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        String dataId =
                helper.getOptions().getOptional(DATA_ID).orElse(String.valueOf(idCounter.get()));
        return new TestTableSource(dataId);
    }

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
        return new HashSet<>(
                Arrays.asList(DATA_ID, ONLY_ACCEPT_EQUAL_PREDICATE, SUPPORT_DELETE_PUSH_DOWN));
    }

    /** A test table source which supports reading metadata. */
    private static class TestTableSource implements ScanTableSource {
        private final String dataId;

        public TestTableSource(String dataId) {
            this.dataId = dataId;
        }

        @Override
        public DynamicTableSource copy() {
            return new TestTableSource(dataId);
        }

        @Override
        public String asSummaryString() {
            return "test table source";
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
            return new SourceFunctionProvider() {
                @Override
                public SourceFunction<RowData> createSourceFunction() {
                    Collection<RowData> rows = registeredRowData.get(dataId);
                    if (rows != null) {
                        return new FromElementsFunction<>(rows);
                    } else {
                        return new FromElementsFunction<>();
                    }
                }

                @Override
                public boolean isBounded() {
                    return true;
                }
            };
        }
    }

    /** A common test sink. */
    private static class TestSink implements DynamicTableSink {

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return ChangelogMode.insertOnly();
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            return null;
        }

        @Override
        public DynamicTableSink copy() {
            return null;
        }

        @Override
        public String asSummaryString() {
            return null;
        }
    }

    /** A sink that supports delete push down. */
    public static class SupportsDeletePushDownSink extends TestSink
            implements SupportsDeletePushDown {

        private final String dataId;
        private final boolean onlyAcceptEqualPredicate;
        private final ResolvedCatalogTable resolvedCatalogTable;
        private final RowData.FieldGetter[] fieldGetters;
        private final List<String> columns;

        private List<Tuple2<String, Object>> equalPredicates;

        public SupportsDeletePushDownSink(
                String dataId,
                boolean onlyAcceptEqualPredicate,
                ResolvedCatalogTable resolvedCatalogTable) {
            this.dataId = dataId;
            this.onlyAcceptEqualPredicate = onlyAcceptEqualPredicate;
            this.resolvedCatalogTable = resolvedCatalogTable;
            this.fieldGetters = getAllFieldGetter(resolvedCatalogTable.getResolvedSchema());
            this.columns = resolvedCatalogTable.getResolvedSchema().getColumnNames();
        }

        @Override
        public DynamicTableSink copy() {
            return new SupportsDeletePushDownSink(
                    dataId, onlyAcceptEqualPredicate, resolvedCatalogTable);
        }

        @Override
        public String asSummaryString() {
            return "SupportDeletePushDownSink";
        }

        @Override
        public boolean applyDeleteFilters(List<ResolvedExpression> filters) {
            if (onlyAcceptEqualPredicate) {
                Optional<List<Tuple2<String, Object>>> optionalEqualPredicates =
                        getEqualPredicates(filters);
                if (optionalEqualPredicates.isPresent()) {
                    equalPredicates = optionalEqualPredicates.get();
                    return true;
                }
                return false;
            }
            return true;
        }

        @Override
        public Optional<Long> executeDeletion() {
            if (onlyAcceptEqualPredicate) {
                Collection<RowData> existingRows = registeredRowData.get(dataId);
                long rowsBefore = existingRows.size();
                existingRows.removeIf(
                        rowData ->
                                satisfyEqualPredicate(
                                        equalPredicates, rowData, fieldGetters, columns));
                return Optional.of(rowsBefore - existingRows.size());
            }
            return Optional.empty();
        }
    }

    /**
     * Get a list of equal predicate from a list of filter, each contains [column, value]. Return
     * Optional.empty() if it contains any non-equal predicate.
     */
    private static Optional<List<Tuple2<String, Object>>> getEqualPredicates(
            List<ResolvedExpression> filters) {
        List<Tuple2<String, Object>> equalPredicates = new ArrayList<>();
        for (ResolvedExpression expression : filters) {
            if (!(expression instanceof CallExpression)) {
                return Optional.empty();
            }
            CallExpression callExpression = (CallExpression) expression;
            if (callExpression.getFunctionDefinition() != BuiltInFunctionDefinitions.EQUALS) {
                return Optional.empty();
            }
            String colName = getColumnName(callExpression);
            Object value = getColumnValue(callExpression);
            equalPredicates.add(Tuple2.of(colName, value));
        }
        return Optional.of(equalPredicates);
    }

    private static String getColumnName(CallExpression comp) {
        return ((FieldReferenceExpression) comp.getChildren().get(0)).getName();
    }

    private static Object getColumnValue(CallExpression comp) {
        ValueLiteralExpression valueLiteralExpression =
                (ValueLiteralExpression) comp.getChildren().get(1);
        return valueLiteralExpression
                .getValueAs(valueLiteralExpression.getOutputDataType().getConversionClass())
                .get();
    }

    /** Check the rowData satisfies the equal predicate. */
    private static boolean satisfyEqualPredicate(
            List<Tuple2<String, Object>> equalPredicates,
            RowData rowData,
            RowData.FieldGetter[] fieldGetters,
            List<String> columns) {
        for (Tuple2<String, Object> equalPredicate : equalPredicates) {
            String colName = equalPredicate.f0;
            Object value = equalPredicate.f1;
            int colIndex = columns.indexOf(colName);
            if (!(Objects.equals(value, fieldGetters[colIndex].getFieldOrNull(rowData)))) {
                return false;
            }
        }
        return true;
    }

    private static RowData.FieldGetter[] getAllFieldGetter(ResolvedSchema resolvedSchema) {
        List<DataType> dataTypes = resolvedSchema.getColumnDataTypes();
        RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[dataTypes.size()];
        for (int i = 0; i < dataTypes.size(); i++) {
            fieldGetters[i] = createFieldGetter(dataTypes.get(i).getLogicalType(), i);
        }
        return fieldGetters;
    }
}
