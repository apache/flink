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
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
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
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsRowLevelModificationScan;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import com.google.common.base.Objects;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.table.data.RowData.createFieldGetter;

/** Test table factory for delete & update. */
public final class TestUpdateDeleteTableFactory
        implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    private static final AtomicInteger idCounter = new AtomicInteger(0);
    private static final Map<String, Collection<RowData>> registeredRowData = new HashMap<>();

    public static String registerRowData(Collection<RowData> data) {
        String id = String.valueOf(idCounter.incrementAndGet());
        registeredRowData.put(id, data);
        return id;
    }

    private static final ConfigOption<Boolean> SUPPORT_DELETE_PUSH_DOWN =
            ConfigOptions.key("support-delete-push-down")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to return the sink which supports delete push down");
    private static final ConfigOption<Boolean> ONLY_ACCEPT_EMPTY_FILTER =
            ConfigOptions.key("only-accept-empty-filter")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether only accept empty filter for delete statement.");

    private static final ConfigOption<SupportsRowLevelDelete.RowLevelDeleteMode> DELETE_MODE =
            ConfigOptions.key("delete-mode")
                    .enumType(SupportsRowLevelDelete.RowLevelDeleteMode.class)
                    .defaultValue(SupportsRowLevelDelete.RowLevelDeleteMode.DELETED_ROWS)
                    .withDescription("The delete mode for row level delete.");

    private static final ConfigOption<SupportsRowLevelUpdate.RowLevelUpdateMode> UPDATE_MODE =
            ConfigOptions.key("update-mode")
                    .enumType(SupportsRowLevelUpdate.RowLevelUpdateMode.class)
                    .defaultValue(SupportsRowLevelUpdate.RowLevelUpdateMode.UPDATED_ROWS)
                    .withDescription("The delete mode for row level delete.");

    private static final ConfigOption<List<String>> REQUIRED_COLUMNS_FOR_DELETE =
            ConfigOptions.key("required-columns-for-delete")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "The columns' name for the required columns in row-level delete");

    private static final ConfigOption<List<String>> REQUIRED_COLUMNS_FOR_UPDATE =
            ConfigOptions.key("required-columns-for-update")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "The columns' name for the required columns in row-level update");

    private static final ConfigOption<Boolean> ONLY_REQUIRE_UPDATED_COLUMNS_FOR_UPDATE =
            ConfigOptions.key("only_require_updated_columns_for_update")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to only require the updated columns for update statement. ");

    private static final ConfigOption<String> DATA_ID =
            ConfigOptions.key("data-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The data id used to read the rows.");

    private static final List<Column.MetadataColumn> META_COLUMNS =
            Arrays.asList(
                    Column.metadata("g", DataTypes.STRING(), null, true),
                    Column.metadata("meta_f1", DataTypes.INT().notNull(), null, false),
                    Column.metadata("meta_f2", DataTypes.STRING().notNull(), "meta_k2", false));

    public static final String IDENTIFIER = "test-update-delete";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        String dataId =
                helper.getOptions().getOptional(DATA_ID).orElse(String.valueOf(idCounter.get()));

        return new TestTableSource(dataId, context.getObjectIdentifier());
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        String dataId =
                helper.getOptions().getOptional(DATA_ID).orElse(String.valueOf(idCounter.get()));

        if (helper.getOptions().get(SUPPORT_DELETE_PUSH_DOWN)) {
            return new TestTableSinkSupportDeletePushDown(
                    context.getObjectIdentifier(),
                    context.getCatalogTable(),
                    helper.getOptions().get(REQUIRED_COLUMNS_FOR_DELETE),
                    helper.getOptions().get(REQUIRED_COLUMNS_FOR_UPDATE),
                    helper.getOptions().get(DELETE_MODE),
                    helper.getOptions().get(UPDATE_MODE),
                    helper.getOptions().get(ONLY_REQUIRE_UPDATED_COLUMNS_FOR_UPDATE),
                    helper.getOptions().get(ONLY_ACCEPT_EMPTY_FILTER),
                    dataId);
        } else {
            return new TestTableSink(
                    context.getObjectIdentifier(),
                    context.getCatalogTable(),
                    helper.getOptions().get(REQUIRED_COLUMNS_FOR_DELETE),
                    helper.getOptions().get(REQUIRED_COLUMNS_FOR_UPDATE),
                    helper.getOptions().get(DELETE_MODE),
                    helper.getOptions().get(UPDATE_MODE),
                    helper.getOptions().get(ONLY_REQUIRE_UPDATED_COLUMNS_FOR_UPDATE),
                    dataId);
        }
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
                        DELETE_MODE,
                        UPDATE_MODE,
                        REQUIRED_COLUMNS_FOR_DELETE,
                        REQUIRED_COLUMNS_FOR_UPDATE,
                        ONLY_REQUIRE_UPDATED_COLUMNS_FOR_UPDATE,
                        SUPPORT_DELETE_PUSH_DOWN,
                        DATA_ID,
                        ONLY_ACCEPT_EMPTY_FILTER));
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
        public DynamicTableSource copy() {
            return new TestTableSource(dataId, tableIdentifier);
        }

        @Override
        public String asSummaryString() {
            return "test-table-source";
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

    private static class TestScanContext implements RowLevelModificationScanContext {
        private final Set<ObjectIdentifier> scanTables = new HashSet<>();
    }

    /** A test table sink which supports both row-level delete and row-level update. */
    private static class TestTableSink
            implements DynamicTableSink, SupportsRowLevelDelete, SupportsRowLevelUpdate {

        private final ObjectIdentifier objectIdentifier;
        private final List<String> requireColumnsForDelete;
        private final List<String> requireColumnsForUpdate;
        private final RowLevelDeleteMode deleteMode;
        private final RowLevelUpdateMode updateMode;
        private final boolean onlyRequireUpdatedCol;
        private final ResolvedCatalogTable resolvedCatalogTable;
        protected final String dataId;

        private boolean isDelete;
        private boolean isUpdate;

        public TestTableSink(
                ObjectIdentifier objectIdentifier,
                ResolvedCatalogTable resolvedCatalogTable,
                List<String> requireColumnsForDelete,
                List<String> requireColumnsForUpdate,
                RowLevelDeleteMode deleteMode,
                RowLevelUpdateMode updateMode,
                boolean onlyRequireUpdatedCol,
                String dataId) {
            this(
                    objectIdentifier,
                    resolvedCatalogTable,
                    requireColumnsForDelete,
                    requireColumnsForUpdate,
                    deleteMode,
                    updateMode,
                    onlyRequireUpdatedCol,
                    dataId,
                    false,
                    false);
        }

        public TestTableSink(
                ObjectIdentifier objectIdentifier,
                ResolvedCatalogTable resolvedCatalogTable,
                List<String> requireColumnsForDelete,
                List<String> requireColumnsForUpdate,
                RowLevelDeleteMode deleteMode,
                RowLevelUpdateMode updateMode,
                boolean onlyRequireUpdatedCol,
                String dataId,
                boolean isDelete,
                boolean isUpdate) {
            this.objectIdentifier = objectIdentifier;
            this.resolvedCatalogTable = resolvedCatalogTable;
            this.requireColumnsForDelete = requireColumnsForDelete;
            this.requireColumnsForUpdate = requireColumnsForUpdate;
            this.deleteMode = deleteMode;
            this.updateMode = updateMode;
            this.onlyRequireUpdatedCol = onlyRequireUpdatedCol;
            this.dataId = dataId;
            this.isDelete = isDelete;
            this.isUpdate = isUpdate;
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
                    if (isDelete) {
                        return dataStream
                                .addSink(
                                        new DeleteDataSinkFunction(
                                                dataId,
                                                getAllFieldGetter(
                                                        resolvedCatalogTable.getResolvedSchema()),
                                                deleteMode))
                                .setParallelism(1);
                    } else if (isUpdate) {
                        return dataStream
                                .addSink(
                                        new UpdateDataSinkFunction(
                                                dataId,
                                                getPrimaryKeyFieldGetter(
                                                        resolvedCatalogTable.getResolvedSchema()),
                                                getAllFieldGetter(
                                                        resolvedCatalogTable.getResolvedSchema()),
                                                updateMode))
                                .setParallelism(1);
                    } else {
                        // otherwise, do nothing
                        return dataStream.addSink(new DiscardingSink<>());
                    }
                }
            };
        }

        /** The sink for delete existing data. */
        private static class DeleteDataSinkFunction extends RichSinkFunction<RowData> {
            private final String dataId;
            private final RowData.FieldGetter[] fieldGetters;
            private final RowLevelDeleteMode deleteMode;
            private transient Collection<RowData> data;
            private transient List<RowData> newData;

            DeleteDataSinkFunction(
                    String dataId,
                    RowData.FieldGetter[] fieldGetters,
                    RowLevelDeleteMode deleteMode) {
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
                if (deleteMode == RowLevelDeleteMode.DELETED_ROWS) {
                    consumeDeletedRows(value);
                } else if (deleteMode == RowLevelDeleteMode.REMAINING_ROWS) {
                    consumeRemainingRows(value);
                } else {
                    throw new TableException(String.format("Unknown delete mode: %s.", deleteMode));
                }
            }

            private void consumeDeletedRows(RowData deletedRow) {
                Preconditions.checkState(
                        deletedRow.getRowKind() == RowKind.DELETE,
                        "The RowKind for the deleted rows should be " + RowKind.DELETE);
                data.removeIf(rowData -> equal(rowData, deletedRow, fieldGetters));
            }

            private void consumeRemainingRows(RowData remainingRow) {
                Preconditions.checkState(
                        remainingRow.getRowKind() == RowKind.INSERT,
                        "The RowKind for the deleted rows should be " + RowKind.DELETE);
                newData.add(copyRowData(remainingRow, fieldGetters));
            }

            @Override
            public void finish() {
                if (deleteMode == RowLevelDeleteMode.REMAINING_ROWS) {
                    registeredRowData.put(dataId, newData);
                }
            }
        }

        /** The sink for update existing data. */
        private static class UpdateDataSinkFunction extends RichSinkFunction<RowData> {
            private final String dataId;
            private final RowData.FieldGetter[] primaryKeyFieldGetters;
            private final RowData.FieldGetter[] allFieldGetters;
            private final RowLevelUpdateMode updateMode;
            private transient RowData[] oldRows;
            private transient List<Tuple2<Integer, RowData>> updatedRows;
            private transient List<RowData> allNewRows;

            public UpdateDataSinkFunction(
                    String dataId,
                    RowData.FieldGetter[] primaryKeyFieldGetters,
                    RowData.FieldGetter[] allFieldGetters,
                    RowLevelUpdateMode updateMode) {
                this.dataId = dataId;
                this.primaryKeyFieldGetters = primaryKeyFieldGetters;
                this.updateMode = updateMode;
                this.allFieldGetters = allFieldGetters;
            }

            @Override
            public void open(Configuration parameters) {
                oldRows = registeredRowData.get(dataId).toArray(new RowData[0]);
                updatedRows = new ArrayList<>();
                allNewRows = new ArrayList<>();
            }

            @Override
            public void invoke(RowData value, Context context) {
                if (updateMode == RowLevelUpdateMode.UPDATED_ROWS) {
                    consumeUpdatedRows(value);
                } else if (updateMode == RowLevelUpdateMode.ALL_ROWS) {
                    consumeAllRows(value);
                } else {
                    throw new TableException("Unknown update mode " + updateMode);
                }
            }

            private void consumeUpdatedRows(RowData updatedRow) {
                Preconditions.checkArgument(
                        updatedRow.getRowKind() == RowKind.UPDATE_AFTER,
                        "The RowKind for the updated rows should be " + RowKind.UPDATE_AFTER);

                for (int i = 0; i < oldRows.length; i++) {
                    if (equal(oldRows[i], updatedRow, primaryKeyFieldGetters)) {
                        updatedRows.add(new Tuple2<>(i, copyRowData(updatedRow, allFieldGetters)));
                    }
                }
            }

            private void consumeAllRows(RowData rowData) {
                Preconditions.checkArgument(
                        rowData.getRowKind() == RowKind.INSERT,
                        "The RowKind for the updated rows should be " + RowKind.INSERT);
                allNewRows.add(copyRowData(rowData, allFieldGetters));
            }

            @Override
            public void finish() throws Exception {
                if (updateMode == RowLevelUpdateMode.UPDATED_ROWS) {
                    commitForUpdatedRows();
                } else if (updateMode == RowLevelUpdateMode.ALL_ROWS) {
                    commitForAllRows();
                } else {
                    throw new TableException("Unknown update mode " + updateMode);
                }
            }

            private void commitForUpdatedRows() {
                List<RowData> newRows = Arrays.asList(oldRows);
                for (Tuple2<Integer, RowData> updatedRow : updatedRows) {
                    newRows.set(updatedRow.f0, updatedRow.f1);
                }
                registeredRowData.put(dataId, newRows);
            }

            private void commitForAllRows() {
                registeredRowData.put(dataId, allNewRows);
            }
        }

        @Override
        public DynamicTableSink copy() {
            return new TestTableSink(
                    objectIdentifier,
                    resolvedCatalogTable,
                    requireColumnsForDelete,
                    requireColumnsForUpdate,
                    deleteMode,
                    updateMode,
                    onlyRequireUpdatedCol,
                    dataId);
        }

        @Override
        public String asSummaryString() {
            return "test-table-sink";
        }

        @Override
        public RowLevelDeleteInfo applyRowLevelDelete(RowLevelModificationScanContext context) {
            Preconditions.checkArgument(context instanceof TestScanContext);
            TestScanContext scanContext = (TestScanContext) context;
            Preconditions.checkArgument(
                    scanContext.scanTables.contains(objectIdentifier),
                    "The scan context should contains the object identifier for row-level delete.");
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

        @Override
        public RowLevelUpdateInfo applyRowLevelUpdate(
                List<Column> updatedColumns, @Nullable RowLevelModificationScanContext context) {
            checkScanContext(context);
            this.isUpdate = true;
            return new RowLevelUpdateInfo() {

                @Override
                public Optional<List<Column>> requiredColumns() {
                    List<Column> requiredCols = null;
                    if (requireColumnsForUpdate != null) {
                        requiredCols =
                                getRequiredColumns(
                                        requireColumnsForUpdate,
                                        resolvedCatalogTable.getResolvedSchema());
                    }
                    return Optional.ofNullable(requiredCols);
                }

                @Override
                public RowLevelUpdateMode getRowLevelUpdateMode() {
                    return updateMode;
                }
            };
        }

        private void checkScanContext(RowLevelModificationScanContext context) {
            Preconditions.checkArgument(context instanceof TestScanContext);
            TestScanContext scanContext = (TestScanContext) context;
            Preconditions.checkArgument(
                    scanContext.scanTables.contains(objectIdentifier),
                    "The scan context should contains the object identifier for row-level delete.");
        }
    }

    private static class TestTableSinkSupportDeletePushDown extends TestTableSink
            implements SupportsDeletePushDown {

        private final boolean onlyAcceptEmptyFilter;

        public TestTableSinkSupportDeletePushDown(
                ObjectIdentifier objectIdentifier,
                ResolvedCatalogTable resolvedCatalogTable,
                List<String> requireColumnsForDelete,
                List<String> requireColumnsForUpdate,
                RowLevelDeleteMode deleteMode,
                RowLevelUpdateMode updateMode,
                boolean onlyRequireUpdatedCol,
                boolean onlyAcceptEmptyFilter,
                String dataId) {
            super(
                    objectIdentifier,
                    resolvedCatalogTable,
                    requireColumnsForDelete,
                    requireColumnsForUpdate,
                    deleteMode,
                    updateMode,
                    onlyRequireUpdatedCol,
                    dataId);
            this.onlyAcceptEmptyFilter = onlyAcceptEmptyFilter;
        }

        @Override
        public boolean applyDeleteFilters(List<ResolvedExpression> filters) {
            if (onlyAcceptEmptyFilter) {
                return filters.size() == 0;
            } else {
                return true;
            }
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

    private static RowData.FieldGetter[] getPrimaryKeyFieldGetter(ResolvedSchema resolvedSchema) {
        int[] indexes = resolvedSchema.getPrimaryKeyIndexes();
        RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[indexes.length];
        List<DataType> dataTypes = resolvedSchema.getColumnDataTypes();
        for (int i = 0; i < fieldGetters.length; i++) {
            int colIndex = indexes[i];
            fieldGetters[i] = createFieldGetter(dataTypes.get(colIndex).getLogicalType(), colIndex);
        }
        return fieldGetters;
    }

    private static boolean equal(
            RowData value1, RowData value2, RowData.FieldGetter[] fieldGetters) {
        for (RowData.FieldGetter fieldGetter : fieldGetters) {
            if (!Objects.equal(
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
