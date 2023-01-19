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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsDeletePushDown;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsRowLevelModificationScan;
import org.apache.flink.table.data.GenericRowData;
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
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.jetbrains.annotations.Nullable;

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

    private static final ConfigOption<Boolean> MIX_DELETE =
            ConfigOptions.key("mix-delete")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether the table support both delete push down and row-level delete. "
                                    + "Note: for supporting delete push down, only the filter pushed is empty, can the filter be accepted.");

    private static final ConfigOption<SupportsRowLevelDelete.RowLevelDeleteMode> DELETE_MODE =
            ConfigOptions.key("delete-mode")
                    .enumType(SupportsRowLevelDelete.RowLevelDeleteMode.class)
                    .defaultValue(SupportsRowLevelDelete.RowLevelDeleteMode.DELETED_ROWS)
                    .withDescription("The delete mode for row level delete.");

    private static final ConfigOption<List<String>> REQUIRED_COLUMNS_FOR_DELETE =
            ConfigOptions.key("required-columns-for-delete")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "The columns' name for the required columns in row-level delete");

    private static final List<Column.MetadataColumn> META_COLUMNS =
            Arrays.asList(
                    Column.metadata("g", DataTypes.STRING(), null, true),
                    Column.metadata("meta_f1", DataTypes.INT().notNull(), null, false),
                    Column.metadata("meta_f2", DataTypes.STRING().notNull(), "meta_k2", false));

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
        SupportsRowLevelDelete.RowLevelDeleteMode deleteMode = helper.getOptions().get(DELETE_MODE);
        List<String> requireCols = helper.getOptions().get(REQUIRED_COLUMNS_FOR_DELETE);
        if (helper.getOptions().get(MIX_DELETE)) {
            return new SupportsDeleteSink(
                    context.getObjectIdentifier(),
                    context.getCatalogTable(),
                    deleteMode,
                    dataId,
                    requireCols);
        } else {
            if (helper.getOptions().get(SUPPORT_DELETE_PUSH_DOWN)) {
                return new SupportsDeletePushDownSink(
                        dataId,
                        helper.getOptions().get(ONLY_ACCEPT_EQUAL_PREDICATE),
                        context.getCatalogTable());
            } else {
                return new SupportsRowLevelDeleteSink(
                        context.getObjectIdentifier(),
                        context.getCatalogTable(),
                        deleteMode,
                        dataId,
                        requireCols);
            }
        }
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        String dataId =
                helper.getOptions().getOptional(DATA_ID).orElse(String.valueOf(idCounter.get()));
        return new TestTableSource(dataId, context.getObjectIdentifier());
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
                Arrays.asList(
                        DATA_ID,
                        ONLY_ACCEPT_EQUAL_PREDICATE,
                        SUPPORT_DELETE_PUSH_DOWN,
                        MIX_DELETE,
                        DELETE_MODE,
                        REQUIRED_COLUMNS_FOR_DELETE));
    }

    /** A test table source which supports reading metadata. */
    private static class TestTableSource
            implements ScanTableSource, SupportsReadingMetadata, SupportsRowLevelModificationScan {
        private final String dataId;
        private final ObjectIdentifier tableIdentifier;

        public TestTableSource(String dataId, ObjectIdentifier tableIdentifier) {
            this.dataId = dataId;
            this.tableIdentifier = tableIdentifier;
        }

        @Override
        public DynamicTableSource copy() {
            return new TestTableSource(dataId, tableIdentifier);
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

        @Override
        public Map<String, DataType> listReadableMetadata() {
            Map<String, DataType> metaData = new HashMap<>();
            META_COLUMNS.forEach(
                    column ->
                            metaData.put(
                                    column.getMetadataKey().orElse(column.getName()),
                                    column.getDataType()));
            return metaData;
        }

        @Override
        public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {}

        @Override
        public RowLevelModificationScanContext applyRowLevelModificationScan(
                RowLevelModificationType rowLevelModificationType,
                @Nullable RowLevelModificationScanContext previousContext) {
            TestScanContext scanContext =
                    previousContext == null
                            ? new TestScanContext()
                            : (TestScanContext) previousContext;
            scanContext.scanTables.add(tableIdentifier);
            return scanContext;
        }
    }

    /** A test scan context for row-level modification scan. */
    private static class TestScanContext implements RowLevelModificationScanContext {
        private final Set<ObjectIdentifier> scanTables = new HashSet<>();
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
            return new TestSink();
        }

        @Override
        public String asSummaryString() {
            return "Test Sink";
        }
    }

    /** A sink that supports delete push down. */
    private static class SupportsRowLevelDeleteSink extends TestSink
            implements SupportsRowLevelDelete {

        private final ObjectIdentifier tableIdentifier;
        private final ResolvedCatalogTable resolvedCatalogTable;
        private final RowLevelDeleteMode deleteMode;
        protected final String dataId;
        private final List<String> requireColumnsForDelete;

        private boolean isDelete;

        public SupportsRowLevelDeleteSink(
                ObjectIdentifier tableIdentifier,
                ResolvedCatalogTable resolvedCatalogTable,
                RowLevelDeleteMode deleteMode,
                String dataId,
                List<String> requireColumnsForDelete) {
            this(
                    tableIdentifier,
                    resolvedCatalogTable,
                    deleteMode,
                    dataId,
                    requireColumnsForDelete,
                    false);
        }

        public SupportsRowLevelDeleteSink(
                ObjectIdentifier tableIdentifier,
                ResolvedCatalogTable resolvedCatalogTable,
                RowLevelDeleteMode deleteMode,
                String dataId,
                List<String> requireColumnsForDelete,
                boolean isDelete) {
            this.tableIdentifier = tableIdentifier;
            this.resolvedCatalogTable = resolvedCatalogTable;
            this.deleteMode = deleteMode;
            this.dataId = dataId;
            this.requireColumnsForDelete = requireColumnsForDelete;
            this.isDelete = isDelete;
        }

        @Override
        public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
            return ChangelogMode.all();
        }

        @Override
        public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
            return new DataStreamSinkProvider() {
                @Override
                public DataStreamSink<?> consumeDataStream(
                        ProviderContext providerContext, DataStream<RowData> dataStream) {
                    return dataStream
                            .addSink(
                                    new DeleteDataSinkFunction(
                                            dataId,
                                            getAllFieldGetter(
                                                    resolvedCatalogTable.getResolvedSchema()),
                                            deleteMode))
                            .setParallelism(1);
                }
            };
        }

        @Override
        public DynamicTableSink copy() {
            return new SupportsRowLevelDeleteSink(
                    tableIdentifier,
                    resolvedCatalogTable,
                    deleteMode,
                    dataId,
                    requireColumnsForDelete,
                    isDelete);
        }

        @Override
        public String asSummaryString() {
            return "support row-level delete sink";
        }

        @Override
        public RowLevelDeleteInfo applyRowLevelDelete(
                @Nullable RowLevelModificationScanContext context) {
            // the context should contain the object identifier of the table to be written
            Preconditions.checkArgument(context instanceof TestScanContext);
            TestScanContext scanContext = (TestScanContext) context;
            Preconditions.checkArgument(
                    scanContext.scanTables.contains(tableIdentifier),
                    String.format(
                            "The scan context should contains the object identifier for table %s in row-level delete.",
                            tableIdentifier));

            this.isDelete = true;
            return new RowLevelDeleteInfo() {
                @Override
                public Optional<List<Column>> requiredColumns() {
                    List<Column> requiredCols = null;
                    if (requireColumnsForDelete != null) {
                        requiredCols =
                                getRequiredColumns(
                                        requireColumnsForDelete,
                                        resolvedCatalogTable.getResolvedSchema());
                    }
                    return Optional.ofNullable(requiredCols);
                }

                @Override
                public RowLevelDeleteMode getRowLevelDeleteMode() {
                    return deleteMode;
                }
            };
        }
    }

    /** The sink for delete existing data. */
    private static class DeleteDataSinkFunction extends RichSinkFunction<RowData> {
        private final String dataId;
        private final RowData.FieldGetter[] fieldGetters;
        private final SupportsRowLevelDelete.RowLevelDeleteMode deleteMode;

        private transient Collection<RowData> data;
        private transient List<RowData> newData;

        DeleteDataSinkFunction(
                String dataId,
                RowData.FieldGetter[] fieldGetters,
                SupportsRowLevelDelete.RowLevelDeleteMode deleteMode) {
            this.dataId = dataId;
            this.fieldGetters = fieldGetters;
            this.deleteMode = deleteMode;
        }

        @Override
        public void open(Configuration parameters) {
            data = registeredRowData.get(dataId);
            newData = new ArrayList<>();
        }

        @Override
        public void invoke(RowData value, Context context) {
            if (deleteMode == SupportsRowLevelDelete.RowLevelDeleteMode.DELETED_ROWS) {
                consumeDeletedRows(value);
            } else if (deleteMode == SupportsRowLevelDelete.RowLevelDeleteMode.REMAINING_ROWS) {
                consumeRemainingRows(value);
            } else {
                throw new TableException(String.format("Unknown delete mode: %s.", deleteMode));
            }
        }

        private void consumeDeletedRows(RowData deletedRow) {
            Preconditions.checkState(
                    deletedRow.getRowKind() == RowKind.DELETE,
                    String.format(
                            "The RowKind for the coming rows should be %s in delete mode %s.",
                            RowKind.DELETE, DELETE_MODE));
            data.removeIf(rowData -> equal(rowData, deletedRow, fieldGetters));
        }

        private void consumeRemainingRows(RowData remainingRow) {
            Preconditions.checkState(
                    remainingRow.getRowKind() == RowKind.INSERT,
                    String.format(
                            "The RowKind for the coming rows should be %s in delete mode %s.",
                            RowKind.INSERT, DELETE_MODE));
            newData.add(copyRowData(remainingRow, fieldGetters));
        }

        @Override
        public void finish() {
            if (deleteMode == SupportsRowLevelDelete.RowLevelDeleteMode.REMAINING_ROWS) {
                registeredRowData.put(dataId, newData);
            }
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

    /** A sink that supports both delete push down and row-level delete. */
    private static class SupportsDeleteSink extends SupportsRowLevelDeleteSink
            implements SupportsDeletePushDown {

        public SupportsDeleteSink(
                ObjectIdentifier tableIdentifier,
                ResolvedCatalogTable resolvedCatalogTable,
                SupportsRowLevelDelete.RowLevelDeleteMode deleteMode,
                String dataId,
                List<String> requireColumnsForDelete) {
            super(
                    tableIdentifier,
                    resolvedCatalogTable,
                    deleteMode,
                    dataId,
                    requireColumnsForDelete);
        }

        @Override
        public boolean applyDeleteFilters(List<ResolvedExpression> filters) {
            // only accept when the filters are empty
            return filters.isEmpty();
        }

        @Override
        public Optional<Long> executeDeletion() {
            Collection<RowData> oldRows = registeredRowData.get(dataId);
            if (oldRows != null) {
                registeredRowData.put(dataId, new ArrayList<>());
                return Optional.of((long) oldRows.size());
            }
            return Optional.empty();
        }
    }

    private static RowData.FieldGetter[] getAllFieldGetter(ResolvedSchema resolvedSchema) {
        List<DataType> dataTypes = resolvedSchema.getColumnDataTypes();
        RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[dataTypes.size()];
        for (int i = 0; i < dataTypes.size(); i++) {
            fieldGetters[i] = createFieldGetter(dataTypes.get(i).getLogicalType(), i);
        }
        return fieldGetters;
    }

    private static boolean equal(
            RowData value1, RowData value2, RowData.FieldGetter[] fieldGetters) {
        for (RowData.FieldGetter fieldGetter : fieldGetters) {
            if (!Objects.equals(
                    fieldGetter.getFieldOrNull(value1), fieldGetter.getFieldOrNull(value2))) {
                return false;
            }
        }
        return true;
    }

    private static RowData copyRowData(RowData rowData, RowData.FieldGetter[] fieldGetters) {
        Object[] values = new Object[fieldGetters.length];
        for (int i = 0; i < fieldGetters.length; i++) {
            values[i] = fieldGetters[i].getFieldOrNull(rowData);
        }
        return GenericRowData.of(values);
    }

    private static List<Column> getRequiredColumns(
            List<String> requiredColName, ResolvedSchema schema) {
        List<Column> requiredCols = new ArrayList<>();
        for (String colName : requiredColName) {
            Optional<Column> optionalColumn = schema.getColumn(colName);
            if (optionalColumn.isPresent()) {
                requiredCols.add(optionalColumn.get());
            } else {
                Column metaCol = null;
                for (Column.MetadataColumn metadataColumn : META_COLUMNS) {
                    String metaColName =
                            metadataColumn.getMetadataKey().orElse(metadataColumn.getName());
                    if (metaColName.equals(colName)) {
                        metaCol = metadataColumn;
                        break;
                    }
                }
                if (metaCol == null) {
                    throw new TableException(
                            String.format("Can't find the required column: `%s`.", colName));
                }
                requiredCols.add(metaCol);
            }
        }
        return requiredCols;
    }
}
