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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.Column.MetadataColumn;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.CollectModifyOperation;
import org.apache.flink.table.operations.ExternalModifyOperation;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.abilities.sink.OverwriteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.abilities.sink.WritingMetadataSpec;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformations;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapContext;
import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsAvoidingCast;
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsExplicitCast;
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsImplicitCast;

/** Utilities for dealing with {@link DynamicTableSink}. */
@Internal
public final class DynamicSinkUtils {

    /** Converts an {@link TableResult#collect()} sink to a {@link RelNode}. */
    public static RelNode convertCollectToRel(
            FlinkRelBuilder relBuilder,
            RelNode input,
            CollectModifyOperation collectModifyOperation) {
        final DataTypeFactory dataTypeFactory =
                unwrapContext(relBuilder).getCatalogManager().getDataTypeFactory();
        final ResolvedSchema childSchema = collectModifyOperation.getChild().getResolvedSchema();
        final ResolvedSchema schema =
                ResolvedSchema.physical(
                        childSchema.getColumnNames(), childSchema.getColumnDataTypes());
        final CatalogTable unresolvedTable = new InlineCatalogTable(schema);
        final ResolvedCatalogTable catalogTable = new ResolvedCatalogTable(unresolvedTable, schema);

        final DataType consumedDataType = fixCollectDataType(dataTypeFactory, schema);

        final CollectDynamicSink tableSink =
                new CollectDynamicSink(
                        collectModifyOperation.getTableIdentifier(), consumedDataType);
        collectModifyOperation.setSelectResultProvider(tableSink.getSelectResultProvider());
        return convertSinkToRel(
                relBuilder,
                input,
                Collections.emptyMap(), // dynamicOptions
                collectModifyOperation.getTableIdentifier(),
                Collections.emptyMap(), // staticPartitions
                false,
                tableSink,
                catalogTable);
    }

    /** Temporary solution until we drop legacy types. */
    private static DataType fixCollectDataType(
            DataTypeFactory dataTypeFactory, ResolvedSchema schema) {
        final DataType fixedDataType =
                DataTypeUtils.transform(
                        dataTypeFactory,
                        schema.toSourceRowDataType(),
                        TypeTransformations.legacyRawToTypeInfoRaw(),
                        TypeTransformations.legacyToNonLegacy());
        // TODO erase the conversion class earlier when dropping legacy code, esp. FLINK-22321
        return TypeConversions.fromLogicalToDataType(fixedDataType.getLogicalType());
    }

    /**
     * Converts an external sink (i.e. further {@link DataStream} transformations) to a {@link
     * RelNode}.
     */
    public static RelNode convertExternalToRel(
            FlinkRelBuilder relBuilder,
            RelNode input,
            ExternalModifyOperation externalModifyOperation) {
        final ResolvedSchema schema = externalModifyOperation.getResolvedSchema();
        final CatalogTable unresolvedTable = new InlineCatalogTable(schema);
        final ResolvedCatalogTable catalogTable = new ResolvedCatalogTable(unresolvedTable, schema);
        final DynamicTableSink tableSink =
                new ExternalDynamicSink(
                        externalModifyOperation.getChangelogMode().orElse(null),
                        externalModifyOperation.getPhysicalDataType());
        return convertSinkToRel(
                relBuilder,
                input,
                Collections.emptyMap(),
                externalModifyOperation.getTableIdentifier(),
                Collections.emptyMap(),
                false,
                tableSink,
                catalogTable);
    }

    /**
     * Converts a given {@link DynamicTableSink} to a {@link RelNode}. It adds helper projections if
     * necessary.
     */
    public static RelNode convertSinkToRel(
            FlinkRelBuilder relBuilder,
            RelNode input,
            CatalogSinkModifyOperation sinkModifyOperation,
            DynamicTableSink sink,
            ResolvedCatalogTable table) {
        return convertSinkToRel(
                relBuilder,
                input,
                sinkModifyOperation.getDynamicOptions(),
                sinkModifyOperation.getTableIdentifier(),
                sinkModifyOperation.getStaticPartitions(),
                sinkModifyOperation.isOverwrite(),
                sink,
                table);
    }

    private static RelNode convertSinkToRel(
            FlinkRelBuilder relBuilder,
            RelNode input,
            Map<String, String> dynamicOptions,
            ObjectIdentifier sinkIdentifier,
            Map<String, String> staticPartitions,
            boolean isOverwrite,
            DynamicTableSink sink,
            ResolvedCatalogTable table) {
        final DataTypeFactory dataTypeFactory =
                unwrapContext(relBuilder).getCatalogManager().getDataTypeFactory();
        final FlinkTypeFactory typeFactory = unwrapTypeFactory(relBuilder);
        final ResolvedSchema schema = table.getResolvedSchema();

        List<SinkAbilitySpec> sinkAbilitySpecs = new ArrayList<>();

        // 1. prepare table sink
        prepareDynamicSink(
                sinkIdentifier, staticPartitions, isOverwrite, sink, table, sinkAbilitySpecs);
        sinkAbilitySpecs.forEach(spec -> spec.apply(sink));

        // 2. validate the query schema to the sink's table schema and apply cast if possible
        final RelNode query =
                validateSchemaAndApplyImplicitCast(
                        input, schema, sinkIdentifier, dataTypeFactory, typeFactory);
        relBuilder.push(query);

        // 3. convert the sink's table schema to the consumed data type of the sink
        final List<Integer> metadataColumns = extractPersistedMetadataColumns(schema);
        if (!metadataColumns.isEmpty()) {
            pushMetadataProjection(relBuilder, typeFactory, schema, sink);
        }

        List<RelHint> hints = new ArrayList<>();
        if (!dynamicOptions.isEmpty()) {
            hints.add(RelHint.builder("OPTIONS").hintOptions(dynamicOptions).build());
        }
        final RelNode finalQuery = relBuilder.build();

        return LogicalSink.create(
                finalQuery,
                hints,
                sinkIdentifier,
                table,
                sink,
                staticPartitions,
                sinkAbilitySpecs.toArray(new SinkAbilitySpec[0]));
    }

    /**
     * Checks if the given query can be written into the given sink's table schema.
     *
     * <p>It checks whether field types are compatible (types should be equal including precisions).
     * If types are not compatible, but can be implicitly cast, a cast projection will be applied.
     * Otherwise, an exception will be thrown.
     */
    public static RelNode validateSchemaAndApplyImplicitCast(
            RelNode query,
            ResolvedSchema sinkSchema,
            @Nullable ObjectIdentifier sinkIdentifier,
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory) {
        final RowType queryType = FlinkTypeFactory.toLogicalRowType(query.getRowType());
        final List<RowField> queryFields = queryType.getFields();

        final RowType sinkType =
                (RowType)
                        fixSinkDataType(dataTypeFactory, sinkSchema.toSinkRowDataType())
                                .getLogicalType();
        final List<RowField> sinkFields = sinkType.getFields();

        if (queryFields.size() != sinkFields.size()) {
            throw createSchemaMismatchException(
                    "Different number of columns.", sinkIdentifier, queryFields, sinkFields);
        }

        boolean requiresCasting = false;
        for (int i = 0; i < sinkFields.size(); i++) {
            final LogicalType queryColumnType = queryFields.get(i).getType();
            final LogicalType sinkColumnType = sinkFields.get(i).getType();
            if (!supportsImplicitCast(queryColumnType, sinkColumnType)) {
                throw createSchemaMismatchException(
                        String.format(
                                "Incompatible types for sink column '%s' at position %s.",
                                sinkFields.get(i).getName(), i),
                        sinkIdentifier,
                        queryFields,
                        sinkFields);
            }
            if (!supportsAvoidingCast(queryColumnType, sinkColumnType)) {
                requiresCasting = true;
            }
        }

        if (requiresCasting) {
            final RelDataType castRelDataType = typeFactory.buildRelNodeRowType(sinkType);
            return RelOptUtil.createCastRel(query, castRelDataType, true);
        }
        return query;
    }

    // --------------------------------------------------------------------------------------------

    /**
     * Creates a projection that reorders physical and metadata columns according to the consumed
     * data type of the sink. It casts metadata columns into the expected data type.
     *
     * @see SupportsWritingMetadata
     */
    private static void pushMetadataProjection(
            FlinkRelBuilder relBuilder,
            FlinkTypeFactory typeFactory,
            ResolvedSchema schema,
            DynamicTableSink sink) {
        final RexBuilder rexBuilder = relBuilder.getRexBuilder();
        final List<Column> columns = schema.getColumns();

        final List<Integer> physicalColumns = extractPhysicalColumns(schema);

        final Map<String, Integer> keyToMetadataColumn =
                extractPersistedMetadataColumns(schema).stream()
                        .collect(
                                Collectors.toMap(
                                        pos -> {
                                            final MetadataColumn metadataColumn =
                                                    (MetadataColumn) columns.get(pos);
                                            return metadataColumn
                                                    .getMetadataKey()
                                                    .orElse(metadataColumn.getName());
                                        },
                                        Function.identity()));

        final List<Integer> metadataColumns =
                createRequiredMetadataKeys(schema, sink).stream()
                        .map(keyToMetadataColumn::get)
                        .collect(Collectors.toList());

        final List<String> fieldNames =
                Stream.concat(
                                physicalColumns.stream().map(columns::get).map(Column::getName),
                                metadataColumns.stream()
                                        .map(columns::get)
                                        .map(MetadataColumn.class::cast)
                                        .map(c -> c.getMetadataKey().orElse(c.getName())))
                        .collect(Collectors.toList());

        final Map<String, DataType> metadataMap = extractMetadataMap(sink);

        final List<RexNode> fieldNodes =
                Stream.concat(
                                physicalColumns.stream()
                                        .map(
                                                pos -> {
                                                    final int posAdjusted =
                                                            adjustByVirtualColumns(columns, pos);
                                                    return relBuilder.field(posAdjusted);
                                                }),
                                metadataColumns.stream()
                                        .map(
                                                pos -> {
                                                    final MetadataColumn metadataColumn =
                                                            (MetadataColumn) columns.get(pos);
                                                    final String metadataKey =
                                                            metadataColumn
                                                                    .getMetadataKey()
                                                                    .orElse(
                                                                            metadataColumn
                                                                                    .getName());

                                                    final LogicalType expectedType =
                                                            metadataMap
                                                                    .get(metadataKey)
                                                                    .getLogicalType();
                                                    final RelDataType expectedRelDataType =
                                                            typeFactory
                                                                    .createFieldTypeFromLogicalType(
                                                                            expectedType);

                                                    final int posAdjusted =
                                                            adjustByVirtualColumns(columns, pos);
                                                    return rexBuilder.makeAbstractCast(
                                                            expectedRelDataType,
                                                            relBuilder.field(posAdjusted));
                                                }))
                        .collect(Collectors.toList());

        relBuilder.projectNamed(fieldNodes, fieldNames, true);
    }

    /**
     * Prepares the given {@link DynamicTableSink}. It check whether the sink is compatible with the
     * INSERT INTO clause and applies initial parameters.
     */
    private static void prepareDynamicSink(
            ObjectIdentifier sinkIdentifier,
            Map<String, String> staticPartitions,
            boolean isOverwrite,
            DynamicTableSink sink,
            ResolvedCatalogTable table,
            List<SinkAbilitySpec> sinkAbilitySpecs) {
        validatePartitioning(sinkIdentifier, staticPartitions, sink, table.getPartitionKeys());

        validateAndApplyOverwrite(sinkIdentifier, isOverwrite, sink, sinkAbilitySpecs);

        validateAndApplyMetadata(sinkIdentifier, sink, table.getResolvedSchema(), sinkAbilitySpecs);
    }

    /**
     * Returns a list of required metadata keys. Ordered by the iteration order of {@link
     * SupportsWritingMetadata#listWritableMetadata()}.
     *
     * <p>This method assumes that sink and schema have been validated via {@link
     * #prepareDynamicSink}.
     */
    private static List<String> createRequiredMetadataKeys(
            ResolvedSchema schema, DynamicTableSink sink) {
        final List<Column> tableColumns = schema.getColumns();
        final List<Integer> metadataColumns = extractPersistedMetadataColumns(schema);

        final Set<String> requiredMetadataKeys =
                metadataColumns.stream()
                        .map(tableColumns::get)
                        .map(MetadataColumn.class::cast)
                        .map(c -> c.getMetadataKey().orElse(c.getName()))
                        .collect(Collectors.toSet());

        final Map<String, DataType> metadataMap = extractMetadataMap(sink);

        return metadataMap.keySet().stream()
                .filter(requiredMetadataKeys::contains)
                .collect(Collectors.toList());
    }

    private static ValidationException createSchemaMismatchException(
            String cause,
            @Nullable ObjectIdentifier sinkIdentifier,
            List<RowField> queryFields,
            List<RowField> sinkFields) {
        final String querySchema =
                queryFields.stream()
                        .map(f -> f.getName() + ": " + f.getType().asSummaryString())
                        .collect(Collectors.joining(", ", "[", "]"));
        final String sinkSchema =
                sinkFields.stream()
                        .map(
                                sinkField ->
                                        sinkField.getName()
                                                + ": "
                                                + sinkField.getType().asSummaryString())
                        .collect(Collectors.joining(", ", "[", "]"));
        final String tableName;
        if (sinkIdentifier != null) {
            tableName = "registered table '" + sinkIdentifier.asSummaryString() + "'";
        } else {
            tableName = "unregistered table";
        }

        return new ValidationException(
                String.format(
                        "Column types of query result and sink for %s do not match.\n"
                                + "Cause: %s\n\n"
                                + "Query schema: %s\n"
                                + "Sink schema:  %s",
                        tableName, cause, querySchema, sinkSchema));
    }

    private static DataType fixSinkDataType(
            DataTypeFactory dataTypeFactory, DataType sinkDataType) {
        // we ignore NULL constraint, the NULL constraint will be checked during runtime
        // see StreamExecSink and BatchExecSink
        return DataTypeUtils.transform(
                dataTypeFactory,
                sinkDataType,
                TypeTransformations.legacyRawToTypeInfoRaw(),
                TypeTransformations.legacyToNonLegacy(),
                TypeTransformations.toNullable());
    }

    private static void validatePartitioning(
            ObjectIdentifier sinkIdentifier,
            Map<String, String> staticPartitions,
            DynamicTableSink sink,
            List<String> partitionKeys) {
        if (!partitionKeys.isEmpty()) {
            if (!(sink instanceof SupportsPartitioning)) {
                throw new TableException(
                        String.format(
                                "Table '%s' is a partitioned table, but the underlying %s doesn't "
                                        + "implement the %s interface.",
                                sinkIdentifier.asSummaryString(),
                                DynamicTableSink.class.getSimpleName(),
                                SupportsPartitioning.class.getSimpleName()));
            }
        }

        staticPartitions
                .keySet()
                .forEach(
                        p -> {
                            if (!partitionKeys.contains(p)) {
                                throw new ValidationException(
                                        String.format(
                                                "Static partition column '%s' should be in the partition keys list %s for table '%s'.",
                                                p,
                                                partitionKeys,
                                                sinkIdentifier.asSummaryString()));
                            }
                        });
    }

    private static void validateAndApplyOverwrite(
            ObjectIdentifier sinkIdentifier,
            boolean isOverwrite,
            DynamicTableSink sink,
            List<SinkAbilitySpec> sinkAbilitySpecs) {
        if (!isOverwrite) {
            return;
        }
        if (!(sink instanceof SupportsOverwrite)) {
            throw new ValidationException(
                    String.format(
                            "INSERT OVERWRITE requires that the underlying %s of table '%s' "
                                    + "implements the %s interface.",
                            DynamicTableSink.class.getSimpleName(),
                            sinkIdentifier.asSummaryString(),
                            SupportsOverwrite.class.getSimpleName()));
        }
        sinkAbilitySpecs.add(new OverwriteSpec(true));
    }

    private static List<Integer> extractPhysicalColumns(ResolvedSchema schema) {
        final List<Column> columns = schema.getColumns();
        return IntStream.range(0, schema.getColumnCount())
                .filter(pos -> columns.get(pos).isPhysical())
                .boxed()
                .collect(Collectors.toList());
    }

    private static List<Integer> extractPersistedMetadataColumns(ResolvedSchema schema) {
        final List<Column> columns = schema.getColumns();
        return IntStream.range(0, schema.getColumnCount())
                .filter(
                        pos -> {
                            final Column column = columns.get(pos);
                            return column instanceof MetadataColumn && column.isPersisted();
                        })
                .boxed()
                .collect(Collectors.toList());
    }

    private static int adjustByVirtualColumns(List<Column> columns, int pos) {
        return pos
                - (int) IntStream.range(0, pos).filter(i -> !columns.get(i).isPersisted()).count();
    }

    private static Map<String, DataType> extractMetadataMap(DynamicTableSink sink) {
        if (sink instanceof SupportsWritingMetadata) {
            return ((SupportsWritingMetadata) sink).listWritableMetadata();
        }
        return Collections.emptyMap();
    }

    private static void validateAndApplyMetadata(
            ObjectIdentifier sinkIdentifier,
            DynamicTableSink sink,
            ResolvedSchema schema,
            List<SinkAbilitySpec> sinkAbilitySpecs) {
        final List<Column> columns = schema.getColumns();
        final List<Integer> metadataColumns = extractPersistedMetadataColumns(schema);

        if (metadataColumns.isEmpty()) {
            return;
        }

        if (!(sink instanceof SupportsWritingMetadata)) {
            throw new ValidationException(
                    String.format(
                            "Table '%s' declares persistable metadata columns, but the underlying %s "
                                    + "doesn't implement the %s interface. If the column should not "
                                    + "be persisted, it can be declared with the VIRTUAL keyword.",
                            sinkIdentifier.asSummaryString(),
                            DynamicTableSink.class.getSimpleName(),
                            SupportsWritingMetadata.class.getSimpleName()));
        }

        final Map<String, DataType> metadataMap =
                ((SupportsWritingMetadata) sink).listWritableMetadata();
        metadataColumns.forEach(
                pos -> {
                    final MetadataColumn metadataColumn = (MetadataColumn) columns.get(pos);
                    final String metadataKey =
                            metadataColumn.getMetadataKey().orElse(metadataColumn.getName());
                    final LogicalType metadataType = metadataColumn.getDataType().getLogicalType();
                    final DataType expectedMetadataDataType = metadataMap.get(metadataKey);
                    // check that metadata key is valid
                    if (expectedMetadataDataType == null) {
                        throw new ValidationException(
                                String.format(
                                        "Invalid metadata key '%s' in column '%s' of table '%s'. "
                                                + "The %s class '%s' supports the following metadata keys for writing:\n%s",
                                        metadataKey,
                                        metadataColumn.getName(),
                                        sinkIdentifier.asSummaryString(),
                                        DynamicTableSink.class.getSimpleName(),
                                        sink.getClass().getName(),
                                        String.join("\n", metadataMap.keySet())));
                    }
                    // check that types are compatible
                    if (!supportsExplicitCast(
                            metadataType, expectedMetadataDataType.getLogicalType())) {
                        if (metadataKey.equals(metadataColumn.getName())) {
                            throw new ValidationException(
                                    String.format(
                                            "Invalid data type for metadata column '%s' of table '%s'. "
                                                    + "The column cannot be declared as '%s' because the type must be "
                                                    + "castable to metadata type '%s'.",
                                            metadataColumn.getName(),
                                            sinkIdentifier.asSummaryString(),
                                            metadataType,
                                            expectedMetadataDataType.getLogicalType()));
                        } else {
                            throw new ValidationException(
                                    String.format(
                                            "Invalid data type for metadata column '%s' with metadata key '%s' of table '%s'. "
                                                    + "The column cannot be declared as '%s' because the type must be "
                                                    + "castable to metadata type '%s'.",
                                            metadataColumn.getName(),
                                            metadataKey,
                                            sinkIdentifier.asSummaryString(),
                                            metadataType,
                                            expectedMetadataDataType.getLogicalType()));
                        }
                    }
                });

        sinkAbilitySpecs.add(
                new WritingMetadataSpec(
                        createRequiredMetadataKeys(schema, sink),
                        createConsumedType(schema, sink)));
    }

    /**
     * Returns the {@link DataType} that a sink should consume as the output from the runtime.
     *
     * <p>The format looks as follows: {@code PHYSICAL COLUMNS + PERSISTED METADATA COLUMNS}
     */
    private static RowType createConsumedType(ResolvedSchema schema, DynamicTableSink sink) {
        final Map<String, DataType> metadataMap = extractMetadataMap(sink);

        final Stream<RowField> physicalFields =
                schema.getColumns().stream()
                        .filter(Column::isPhysical)
                        .map(c -> new RowField(c.getName(), c.getDataType().getLogicalType()));

        final Stream<RowField> metadataFields =
                createRequiredMetadataKeys(schema, sink).stream()
                        .map(k -> new RowField(k, metadataMap.get(k).getLogicalType()));

        final List<RowField> rowFields =
                Stream.concat(physicalFields, metadataFields).collect(Collectors.toList());

        return new RowType(false, rowFields);
    }

    private DynamicSinkUtils() {
        // no instantiation
    }
}
