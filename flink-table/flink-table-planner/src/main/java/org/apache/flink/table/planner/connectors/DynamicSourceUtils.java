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

package org.apache.flink.table.planner.connectors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Column.ComputedColumn;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.ScanTableSource.ScanRuntimeProvider;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsRowLevelModificationScan;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter;
import org.apache.flink.table.planner.plan.abilities.source.ReadingMetadataSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.utils.RowLevelModificationContextUtils;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.RowKind;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsExplicitCast;

/** Utilities for dealing with {@link DynamicTableSource}. */
@Internal
public final class DynamicSourceUtils {

    /**
     * Converts a given {@link DataStream} to a {@link RelNode}. It adds helper projections if
     * necessary.
     */
    public static RelNode convertDataStreamToRel(
            boolean isBatchMode,
            ReadableConfig config,
            FlinkRelBuilder relBuilder,
            ContextResolvedTable contextResolvedTable,
            DataStream<?> dataStream,
            DataType physicalDataType,
            boolean isTopLevelRecord,
            ChangelogMode changelogMode) {
        final DynamicTableSource tableSource =
                new ExternalDynamicSource<>(
                        contextResolvedTable.getIdentifier(),
                        dataStream,
                        physicalDataType,
                        isTopLevelRecord,
                        changelogMode);
        final FlinkStatistic statistic =
                FlinkStatistic.unknown(contextResolvedTable.getResolvedSchema()).build();
        return convertSourceToRel(
                isBatchMode,
                config,
                relBuilder,
                contextResolvedTable,
                statistic,
                Collections.emptyList(),
                tableSource);
    }

    /**
     * Converts a given {@link DynamicTableSource} to a {@link RelNode}. It adds helper projections
     * if necessary.
     */
    public static RelNode convertSourceToRel(
            boolean isBatchMode,
            ReadableConfig config,
            FlinkRelBuilder relBuilder,
            ContextResolvedTable contextResolvedTable,
            FlinkStatistic statistic,
            List<RelHint> hints,
            DynamicTableSource tableSource) {
        final String tableDebugName = contextResolvedTable.getIdentifier().asSummaryString();
        final ResolvedCatalogTable resolvedCatalogTable = contextResolvedTable.getResolvedTable();

        final List<SourceAbilitySpec> sourceAbilities = new ArrayList<>();
        // 1. prepare table source
        prepareDynamicSource(
                tableDebugName,
                resolvedCatalogTable,
                tableSource,
                isBatchMode,
                config,
                sourceAbilities);

        // 2. push table scan
        pushTableScan(
                isBatchMode,
                relBuilder,
                contextResolvedTable,
                statistic,
                hints,
                tableSource,
                sourceAbilities);

        // 3. push project for non-physical columns
        final ResolvedSchema schema = contextResolvedTable.getResolvedSchema();
        if (!schema.getColumns().stream().allMatch(Column::isPhysical)) {
            pushMetadataProjection(relBuilder, schema);
            pushGeneratedProjection(relBuilder, schema);
        }

        // 4. push watermark assigner
        if (!isBatchMode && !schema.getWatermarkSpecs().isEmpty()) {
            pushWatermarkAssigner(relBuilder, schema);
        }

        return relBuilder.build();
    }

    /**
     * Prepares the given {@link DynamicTableSource}. It check whether the source is compatible with
     * the given schema and applies initial parameters.
     */
    public static void prepareDynamicSource(
            String tableDebugName,
            ResolvedCatalogTable table,
            DynamicTableSource source,
            boolean isBatchMode,
            ReadableConfig config,
            List<SourceAbilitySpec> sourceAbilities) {
        final ResolvedSchema schema = table.getResolvedSchema();

        validateAndApplyMetadata(tableDebugName, schema, source, sourceAbilities);

        if (source instanceof ScanTableSource) {
            validateScanSource(
                    tableDebugName, schema, (ScanTableSource) source, isBatchMode, config);
            prepareRowLevelModificationScan(source);
        }

        // lookup table source is validated in LookupJoin node
    }

    // TODO: isUpsertSource(), isSourceChangeEventsDuplicate()

    /**
     * Returns a list of required metadata columns. Ordered by the iteration order of {@link
     * SupportsReadingMetadata#listReadableMetadata()}.
     *
     * <p>This method assumes that source and schema have been validated via {@link
     * #prepareDynamicSource(String, ResolvedCatalogTable, DynamicTableSource, boolean,
     * ReadableConfig, List)}.
     */
    public static List<MetadataColumn> createRequiredMetadataColumns(
            ResolvedSchema schema, DynamicTableSource source) {

        final Map<String, MetadataColumn> metadataKeysToMetadataColumns =
                createMetadataKeysToMetadataColumnsMap(schema);
        final Map<String, DataType> metadataMap = extractMetadataMap(source);

        // reorder the column
        return metadataMap.keySet().stream()
                .filter(metadataKeysToMetadataColumns::containsKey)
                .map(metadataKeysToMetadataColumns::get)
                .collect(Collectors.toList());
    }

    /**
     * Returns a map record the mapping relation between metadataKeys to metadataColumns in input
     * schema.
     */
    public static Map<String, MetadataColumn> createMetadataKeysToMetadataColumnsMap(
            ResolvedSchema schema) {
        final List<MetadataColumn> metadataColumns = extractMetadataColumns(schema);

        Map<String, MetadataColumn> metadataKeysToMetadataColumns = new HashMap<>();

        for (MetadataColumn column : metadataColumns) {
            String metadataKey = column.getMetadataKey().orElse(column.getName());
            // After resolving, every metadata column has the unique metadata key.
            metadataKeysToMetadataColumns.put(metadataKey, column);
        }

        return metadataKeysToMetadataColumns;
    }

    /**
     * Returns the {@link DataType} that a source should produce as the input into the runtime.
     *
     * <p>The format looks as follows: {@code PHYSICAL COLUMNS + METADATA COLUMNS}
     *
     * <p>Physical columns use the table schema's name. Metadata column use the metadata key as
     * name.
     */
    public static RowType createProducedType(ResolvedSchema schema, DynamicTableSource source) {
        final Map<String, DataType> metadataMap = extractMetadataMap(source);

        final Stream<RowField> physicalFields =
                ((RowType) schema.toPhysicalRowDataType().getLogicalType()).getFields().stream();

        final Stream<RowField> metadataFields =
                createRequiredMetadataColumns(schema, source).stream()
                        .map(
                                k ->
                                        new RowField(
                                                // Use the alias to ensure that physical and
                                                // metadata columns don't collide
                                                k.getName(),
                                                metadataMap
                                                        .get(k.getMetadataKey().orElse(k.getName()))
                                                        .getLogicalType()));

        final List<RowField> rowFields =
                Stream.concat(physicalFields, metadataFields).collect(Collectors.toList());

        return new RowType(false, rowFields);
    }

    /** Returns true if the table is an upsert source. */
    public static boolean isUpsertSource(
            ResolvedSchema resolvedSchema, DynamicTableSource tableSource) {
        if (!(tableSource instanceof ScanTableSource)) {
            return false;
        }
        ChangelogMode mode = ((ScanTableSource) tableSource).getChangelogMode();
        boolean isUpsertMode =
                mode.contains(RowKind.UPDATE_AFTER) && !mode.contains(RowKind.UPDATE_BEFORE);
        boolean hasPrimaryKey = resolvedSchema.getPrimaryKey().isPresent();
        return isUpsertMode && hasPrimaryKey;
    }

    /** Returns true if the table source produces duplicate change events. */
    public static boolean isSourceChangeEventsDuplicate(
            ResolvedSchema resolvedSchema,
            DynamicTableSource tableSource,
            TableConfig tableConfig) {
        if (!(tableSource instanceof ScanTableSource)) {
            return false;
        }
        ChangelogMode mode = ((ScanTableSource) tableSource).getChangelogMode();
        boolean isCDCSource =
                !mode.containsOnly(RowKind.INSERT) && !isUpsertSource(resolvedSchema, tableSource);
        boolean changeEventsDuplicate =
                tableConfig.get(ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE);
        boolean hasPrimaryKey = resolvedSchema.getPrimaryKey().isPresent();
        return isCDCSource && changeEventsDuplicate && hasPrimaryKey;
    }

    // --------------------------------------------------------------------------------------------

    /** Creates a specialized node for assigning watermarks. */
    private static void pushWatermarkAssigner(FlinkRelBuilder relBuilder, ResolvedSchema schema) {
        final ExpressionConverter converter = new ExpressionConverter(relBuilder);
        final RelDataType inputRelDataType = relBuilder.peek().getRowType();

        // schema resolver has checked before that only one spec exists
        final WatermarkSpec watermarkSpec = schema.getWatermarkSpecs().get(0);

        final String rowtimeColumn = watermarkSpec.getRowtimeAttribute();
        final int rowtimeColumnIdx = inputRelDataType.getFieldNames().indexOf(rowtimeColumn);

        final RexNode watermarkRexNode = watermarkSpec.getWatermarkExpression().accept(converter);

        relBuilder.watermark(rowtimeColumnIdx, watermarkRexNode);
    }

    /** Creates a projection that adds computed columns and finalizes the table schema. */
    private static void pushGeneratedProjection(FlinkRelBuilder relBuilder, ResolvedSchema schema) {
        final ExpressionConverter converter = new ExpressionConverter(relBuilder);
        final List<RexNode> projection =
                schema.getColumns().stream()
                        .map(
                                c -> {
                                    if (c instanceof ComputedColumn) {
                                        final ComputedColumn computedColumn = (ComputedColumn) c;
                                        return computedColumn.getExpression().accept(converter);
                                    } else {
                                        return relBuilder.field(c.getName());
                                    }
                                })
                        .collect(Collectors.toList());

        relBuilder.projectNamed(
                projection,
                schema.getColumns().stream().map(Column::getName).collect(Collectors.toList()),
                true);
    }

    /**
     * Creates a projection that reorders physical and metadata columns according to the given
     * schema. It casts metadata columns into the expected data type to be accessed by computed
     * columns in the next step. Computed columns are ignored here.
     *
     * @see SupportsReadingMetadata
     */
    private static void pushMetadataProjection(FlinkRelBuilder relBuilder, ResolvedSchema schema) {
        final RexBuilder rexBuilder = relBuilder.getRexBuilder();

        final List<String> fieldNames =
                schema.getColumns().stream()
                        .filter(c -> !(c instanceof ComputedColumn))
                        .map(Column::getName)
                        .collect(Collectors.toList());

        final List<RexNode> fieldNodes =
                schema.getColumns().stream()
                        .filter(c -> !(c instanceof ComputedColumn))
                        .map(
                                c -> {
                                    final RelDataType relDataType =
                                            relBuilder
                                                    .getTypeFactory()
                                                    .createFieldTypeFromLogicalType(
                                                            c.getDataType().getLogicalType());
                                    if (c instanceof MetadataColumn) {
                                        final MetadataColumn metadataColumn = (MetadataColumn) c;
                                        String columnName = metadataColumn.getName();
                                        return rexBuilder.makeAbstractCast(
                                                relDataType, relBuilder.field(columnName));
                                    } else {
                                        return relBuilder.field(c.getName());
                                    }
                                })
                        .collect(Collectors.toList());

        relBuilder.projectNamed(fieldNodes, fieldNames, true);
    }

    private static void pushTableScan(
            boolean isBatchMode,
            FlinkRelBuilder relBuilder,
            ContextResolvedTable contextResolvedTable,
            FlinkStatistic statistic,
            List<RelHint> hints,
            DynamicTableSource tableSource,
            List<SourceAbilitySpec> sourceAbilities) {
        final RowType producedType =
                createProducedType(contextResolvedTable.getResolvedSchema(), tableSource);
        final RelDataType producedRelDataType =
                relBuilder.getTypeFactory().buildRelNodeRowType(producedType);

        final TableSourceTable tableSourceTable =
                new TableSourceTable(
                        relBuilder.getRelOptSchema(),
                        producedRelDataType,
                        statistic,
                        tableSource,
                        !isBatchMode,
                        contextResolvedTable,
                        ShortcutUtils.unwrapContext(relBuilder),
                        ShortcutUtils.unwrapTypeFactory(relBuilder),
                        sourceAbilities.toArray(new SourceAbilitySpec[0]));

        final LogicalTableScan scan =
                LogicalTableScan.create(relBuilder.getCluster(), tableSourceTable, hints);
        relBuilder.push(scan);
    }

    private static Map<String, DataType> extractMetadataMap(DynamicTableSource source) {
        if (source instanceof SupportsReadingMetadata) {
            return ((SupportsReadingMetadata) source).listReadableMetadata();
        }
        return Collections.emptyMap();
    }

    public static List<MetadataColumn> extractMetadataColumns(ResolvedSchema schema) {
        return schema.getColumns().stream()
                .filter(MetadataColumn.class::isInstance)
                .map(MetadataColumn.class::cast)
                .collect(Collectors.toList());
    }

    public static void validateAndApplyMetadata(
            String tableDebugName,
            ResolvedSchema schema,
            DynamicTableSource source,
            List<SourceAbilitySpec> sourceAbilities) {
        final List<MetadataColumn> metadataColumns = extractMetadataColumns(schema);

        if (metadataColumns.isEmpty()) {
            return;
        }

        if (!(source instanceof SupportsReadingMetadata)) {
            throw new ValidationException(
                    String.format(
                            "Table '%s' declares metadata columns, but the underlying %s doesn't implement "
                                    + "the %s interface. Therefore, metadata cannot be read from the given source.",
                            source.asSummaryString(),
                            DynamicTableSource.class.getSimpleName(),
                            SupportsReadingMetadata.class.getSimpleName()));
        }

        final SupportsReadingMetadata metadataSource = (SupportsReadingMetadata) source;

        final Map<String, DataType> metadataMap = metadataSource.listReadableMetadata();
        metadataColumns.forEach(
                c -> {
                    final String metadataKey = c.getMetadataKey().orElse(c.getName());
                    final LogicalType metadataType = c.getDataType().getLogicalType();
                    final DataType expectedMetadataDataType = metadataMap.get(metadataKey);
                    // check that metadata key is valid
                    if (expectedMetadataDataType == null) {
                        throw new ValidationException(
                                String.format(
                                        "Invalid metadata key '%s' in column '%s' of table '%s'. "
                                                + "The %s class '%s' supports the following metadata keys for reading:\n%s",
                                        metadataKey,
                                        c.getName(),
                                        tableDebugName,
                                        DynamicTableSource.class.getSimpleName(),
                                        source.getClass().getName(),
                                        String.join("\n", metadataMap.keySet())));
                    }
                    // check that types are compatible
                    if (!supportsExplicitCast(
                            expectedMetadataDataType.getLogicalType(), metadataType)) {
                        if (metadataKey.equals(c.getName())) {
                            throw new ValidationException(
                                    String.format(
                                            "Invalid data type for metadata column '%s' of table '%s'. "
                                                    + "The column cannot be declared as '%s' because the type must be "
                                                    + "castable from metadata type '%s'.",
                                            c.getName(),
                                            tableDebugName,
                                            expectedMetadataDataType.getLogicalType(),
                                            metadataType));
                        } else {
                            throw new ValidationException(
                                    String.format(
                                            "Invalid data type for metadata column '%s' with metadata key '%s' of table '%s'. "
                                                    + "The column cannot be declared as '%s' because the type must be "
                                                    + "castable from metadata type '%s'.",
                                            c.getName(),
                                            metadataKey,
                                            tableDebugName,
                                            expectedMetadataDataType.getLogicalType(),
                                            metadataType));
                        }
                    }
                });

        final List<String> metadataKeys =
                createRequiredMetadataColumns(schema, source).stream()
                        .map(column -> column.getMetadataKey().orElse(column.getName()))
                        .collect(Collectors.toList());
        final DataType producedDataType =
                TypeConversions.fromLogicalToDataType(createProducedType(schema, source));
        sourceAbilities.add(
                new ReadingMetadataSpec(metadataKeys, (RowType) producedDataType.getLogicalType()));
        metadataSource.applyReadableMetadata(metadataKeys, producedDataType);
    }

    private static void validateScanSource(
            String tableDebugName,
            ResolvedSchema schema,
            ScanTableSource scanSource,
            boolean isBatchMode,
            ReadableConfig config) {
        final ScanRuntimeProvider provider =
                scanSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        final ChangelogMode changelogMode = scanSource.getChangelogMode();

        validateWatermarks(tableDebugName, schema);

        if (isBatchMode) {
            validateScanSourceForBatch(tableDebugName, changelogMode, provider);
        } else {
            validateScanSourceForStreaming(
                    tableDebugName, schema, scanSource, changelogMode, config);
        }
    }

    private static void validateScanSourceForStreaming(
            String tableDebugName,
            ResolvedSchema schema,
            ScanTableSource scanSource,
            ChangelogMode changelogMode,
            ReadableConfig config) {
        // sanity check for produced ChangelogMode
        final boolean hasChangelogMode = changelogMode != null;
        final boolean hasUpdateBefore =
                hasChangelogMode && changelogMode.contains(RowKind.UPDATE_BEFORE);
        final boolean hasUpdateAfter =
                hasChangelogMode && changelogMode.contains(RowKind.UPDATE_AFTER);
        if (!hasUpdateBefore && hasUpdateAfter) {
            // only UPDATE_AFTER
            if (!schema.getPrimaryKey().isPresent()) {
                throw new TableException(
                        String.format(
                                "Table '%s' produces a changelog stream that contains UPDATE_AFTER but no UPDATE_BEFORE. "
                                        + "This requires defining a primary key constraint on the table.",
                                tableDebugName));
            }
        } else if (hasUpdateBefore && !hasUpdateAfter) {
            // only UPDATE_BEFORE
            throw new ValidationException(
                    String.format(
                            "Invalid source for table '%s'. A %s doesn't support a changelog stream that contains "
                                    + "UPDATE_BEFORE but no UPDATE_AFTER. Please adapt the implementation of class '%s'.",
                            tableDebugName,
                            ScanTableSource.class.getSimpleName(),
                            scanSource.getClass().getName()));
        } else if (hasChangelogMode && !changelogMode.containsOnly(RowKind.INSERT)) {
            // CDC mode (non-upsert mode and non-insert-only mode)
            final boolean changeEventsDuplicate =
                    config.get(ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE);
            if (changeEventsDuplicate && !schema.getPrimaryKey().isPresent()) {
                throw new TableException(
                        String.format(
                                "Configuration '%s' is enabled which requires the changelog sources to define a PRIMARY KEY. "
                                        + "However, table '%s' doesn't have a primary key.",
                                ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE.key(),
                                tableDebugName));
            }
        }
    }

    private static void validateScanSourceForBatch(
            String tableDebugName, ChangelogMode changelogMode, ScanRuntimeProvider provider) {
        // batch only supports bounded source
        if (!provider.isBounded()) {
            throw new ValidationException(
                    String.format(
                            "Querying an unbounded table '%s' in batch mode is not allowed. "
                                    + "The table source is unbounded.",
                            tableDebugName));
        }
        // batch only supports INSERT only source
        if (!changelogMode.containsOnly(RowKind.INSERT)) {
            throw new TableException(
                    String.format(
                            "Querying a table in batch mode is currently only possible for INSERT-only table sources. "
                                    + "But the source for table '%s' produces other changelog messages than just INSERT.",
                            tableDebugName));
        }
    }

    private static void validateWatermarks(String tableDebugName, ResolvedSchema schema) {
        if (schema.getWatermarkSpecs().isEmpty()) {
            return;
        }

        if (schema.getWatermarkSpecs().size() > 1) {
            throw new TableException(
                    String.format(
                            "Currently only at most one WATERMARK declaration is supported for table '%s'.",
                            tableDebugName));
        }

        final String rowtimeAttribute = schema.getWatermarkSpecs().get(0).getRowtimeAttribute();
        if (rowtimeAttribute.contains(".")) {
            throw new TableException(
                    String.format(
                            "A nested field '%s' cannot be declared as rowtime attribute for table '%s' right now.",
                            rowtimeAttribute, tableDebugName));
        }
    }

    private static void prepareRowLevelModificationScan(DynamicTableSource dynamicTableSource) {
        // if the modification type has been set and the dynamic source supports row-level
        // modification scan
        if (RowLevelModificationContextUtils.getModificationType() != null
                && dynamicTableSource instanceof SupportsRowLevelModificationScan) {
            SupportsRowLevelModificationScan modificationScan =
                    (SupportsRowLevelModificationScan) dynamicTableSource;
            // get the previous scan context
            RowLevelModificationScanContext scanContext =
                    RowLevelModificationContextUtils.getScanContext();
            // pass the previous scan context to current table soruce and
            // get a new scan context
            RowLevelModificationScanContext newScanContext =
                    modificationScan.applyRowLevelModificationScan(
                            RowLevelModificationContextUtils.getModificationType(), scanContext);
            // set the scan context
            RowLevelModificationContextUtils.setScanContext(newScanContext);
        }
    }

    private DynamicSourceUtils() {
        // no instantiation
    }
}
