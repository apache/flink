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

package org.apache.flink.table.planner.sinks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableColumn.MetadataColumn;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalSink;
import org.apache.flink.table.planner.plan.schema.CatalogSourceTable;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformations;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsAvoidingCast;
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsExplicitCast;
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsImplicitCast;

/**
 * Utilities for dealing with {@link DynamicTableSink}.
 */
@Internal
public final class DynamicSinkUtils {

	/**
	 * Similar to {@link CatalogSourceTable#toRel(RelOptTable.ToRelContext)}, converts a given
	 * {@link DynamicTableSink} to a {@link RelNode}. It adds helper projections if necessary.
	 */
	public static RelNode toRel(
			FlinkRelBuilder relBuilder,
			RelNode input,
			CatalogSinkModifyOperation sinkOperation,
			DynamicTableSink sink,
			CatalogTable table) {
		final FlinkTypeFactory typeFactory = ShortcutUtils.unwrapTypeFactory(relBuilder);
		final TableSchema schema = table.getSchema();

		// 1. prepare table sink
		prepareDynamicSink(sinkOperation, sink, table);

		// 2. validate the query schema to the sink's table schema and apply cast if possible
		final RelNode query = validateSchemaAndApplyImplicitCast(
			input,
			schema,
			sinkOperation.getTableIdentifier(),
			typeFactory);
		relBuilder.push(query);

		// 3. convert the sink's table schema to the consumed data type of the sink
		final List<Integer> metadataColumns = extractPersistedMetadataColumns(schema);
		if (!metadataColumns.isEmpty()) {
			pushMetadataProjection(relBuilder, typeFactory, schema, sink);
		}

		final RelNode finalQuery = relBuilder.build();

		return LogicalSink.create(
			finalQuery,
			sinkOperation.getTableIdentifier(),
			table,
			sink,
			sinkOperation.getStaticPartitions());
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
			TableSchema sinkSchema,
			@Nullable ObjectIdentifier sinkIdentifier,
			FlinkTypeFactory typeFactory) {
		final RowType queryType = FlinkTypeFactory.toLogicalRowType(query.getRowType());
		final List<RowField> queryFields = queryType.getFields();

		final RowType sinkType = (RowType) fixSinkDataType(sinkSchema.toPersistedRowDataType()).getLogicalType();
		final List<RowField> sinkFields = sinkType.getFields();

		if (queryFields.size() != sinkFields.size()) {
			throw createSchemaMismatchException(
				"Different number of columns.",
				sinkIdentifier,
				queryFields,
				sinkFields);
		}

		boolean requiresCasting = false;
		for (int i = 0; i < sinkFields.size(); i++) {
			final LogicalType queryColumnType = queryFields.get(i).getType();
			final LogicalType sinkColumnType = sinkFields.get(i).getType();
			if (!supportsImplicitCast(queryColumnType, sinkColumnType)) {
				throw createSchemaMismatchException(
					String.format(
						"Incompatible types for sink column '%s' at position %s.",
						sinkFields.get(i).getName(),
						i),
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
	 * Creates a projection that reorders physical and metadata columns according to the consumed data
	 * type of the sink. It casts metadata columns into the expected data type.
	 *
	 * @see SupportsWritingMetadata
	 */
	private static void pushMetadataProjection(
			FlinkRelBuilder relBuilder,
			FlinkTypeFactory typeFactory,
			TableSchema schema,
			DynamicTableSink sink) {
		final RexBuilder rexBuilder = relBuilder.getRexBuilder();
		final List<TableColumn> tableColumns = schema.getTableColumns();

		final List<Integer> physicalColumns = extractPhysicalColumns(schema);

		final Map<String, Integer> keyToMetadataColumn = extractPersistedMetadataColumns(schema)
			.stream()
			.collect(
				Collectors.toMap(
					pos -> {
						final MetadataColumn metadataColumn = (MetadataColumn) tableColumns.get(pos);
						return metadataColumn.getMetadataAlias().orElse(metadataColumn.getName());
					},
					Function.identity()
				)
			);

		final List<Integer> metadataColumns = createRequiredMetadataKeys(schema, sink)
			.stream()
			.map(keyToMetadataColumn::get)
			.collect(Collectors.toList());

		final List<String> fieldNames = Stream
			.concat(
				physicalColumns.stream()
					.map(tableColumns::get)
					.map(TableColumn::getName),
				metadataColumns.stream()
					.map(tableColumns::get)
					.map(MetadataColumn.class::cast)
					.map(c -> c.getMetadataAlias().orElse(c.getName()))
			)
			.collect(Collectors.toList());

		final Map<String, DataType> metadataMap = extractMetadataMap(sink);

		final List<RexNode> fieldNodes = Stream
			.concat(
				physicalColumns
					.stream()
					.map(pos -> {
						final int posAdjusted = adjustByVirtualColumns(tableColumns, pos);
						return relBuilder.field(posAdjusted);
					}),
				metadataColumns
					.stream()
					.map(pos -> {
						final MetadataColumn metadataColumn = (MetadataColumn) tableColumns.get(pos);
						final String metadataKey = metadataColumn.getMetadataAlias().orElse(metadataColumn.getName());

						final LogicalType expectedType = metadataMap.get(metadataKey).getLogicalType();
						final RelDataType expectedRelDataType = typeFactory.createFieldTypeFromLogicalType(expectedType);

						final int posAdjusted = adjustByVirtualColumns(tableColumns, pos);
						return rexBuilder.makeAbstractCast(expectedRelDataType, relBuilder.field(posAdjusted));
					})
			)
			.collect(Collectors.toList());

		relBuilder.projectNamed(fieldNodes, fieldNames, true);
	}

	/**
	 * Prepares the given {@link DynamicTableSink}. It check whether the sink is compatible with the
	 * INSERT INTO clause and applies initial parameters.
	 */
	private static void prepareDynamicSink(
			CatalogSinkModifyOperation sinkOperation,
			DynamicTableSink sink,
			CatalogTable table) {
		final ObjectIdentifier sinkIdentifier = sinkOperation.getTableIdentifier();

		validatePartitioning(sinkOperation, sinkIdentifier, sink, table.getPartitionKeys());

		validateAndApplyOverwrite(sinkOperation, sinkIdentifier, sink);

		validateAndApplyMetadata(sinkIdentifier, sink, table.getSchema());
	}

	/**
	 * Returns a list of required metadata keys. Ordered by the iteration order of
	 * {@link SupportsWritingMetadata#listWritableMetadata()}.
	 *
	 * <p>This method assumes that sink and schema have been validated via
	 * {@link #prepareDynamicSink(CatalogSinkModifyOperation, DynamicTableSink, CatalogTable)}.
	 */
	private static List<String> createRequiredMetadataKeys(TableSchema schema, DynamicTableSink sink) {
		final List<TableColumn> tableColumns = schema.getTableColumns();
		final List<Integer> metadataColumns = extractPersistedMetadataColumns(schema);

		final Set<String> requiredMetadataKeys = metadataColumns
			.stream()
			.map(tableColumns::get)
			.map(MetadataColumn.class::cast)
			.map(c -> c.getMetadataAlias().orElse(c.getName()))
			.collect(Collectors.toSet());

		final Map<String, DataType> metadataMap = extractMetadataMap(sink);

		return metadataMap.keySet()
			.stream()
			.filter(requiredMetadataKeys::contains)
			.collect(Collectors.toList());
	}

	private static ValidationException createSchemaMismatchException(
			String cause,
			@Nullable ObjectIdentifier sinkIdentifier,
			List<RowField> queryFields,
			List<RowField> sinkFields) {
		final String querySchema = queryFields.stream()
			.map(f -> f.getName() + ": " + f.getType().asSummaryString())
			.collect(Collectors.joining(", ", "[", "]"));
		final String sinkSchema = sinkFields.stream()
			.map(sinkField -> sinkField.getName() + ": " + sinkField.getType().asSummaryString())
			.collect(Collectors.joining(", ", "[", "]"));
		final String tableName;
		if (sinkIdentifier != null) {
			tableName = "registered table '" + sinkIdentifier.asSummaryString() + "'";
		} else {
			tableName = "unregistered table";
		}

		return new ValidationException(
			String.format(
				"Column types of query result and sink for %s do not match.\n" +
					"Cause: %s\n\n" +
					"Query schema: %s\n" +
					"Sink schema:  %s",
				tableName,
				cause,
				querySchema,
				sinkSchema
			)
		);
	}

	private static DataType fixSinkDataType(DataType sinkDataType) {
		// we recognize legacy decimal is the same to default decimal
		// we ignore NULL constraint, the NULL constraint will be checked during runtime
		// see StreamExecSink and BatchExecSink
		return DataTypeUtils.transform(
			sinkDataType,
			TypeTransformations.legacyDecimalToDefaultDecimal(),
			TypeTransformations.legacyRawToTypeInfoRaw(),
			TypeTransformations.toNullable());
	}

	private static void validatePartitioning(
			CatalogSinkModifyOperation sinkOperation,
			ObjectIdentifier sinkIdentifier,
			DynamicTableSink sink,
			List<String> partitionKeys) {
		if (!partitionKeys.isEmpty()) {
			if (!(sink instanceof SupportsPartitioning)) {
				throw new TableException(
					String.format(
						"Table '%s' is a partitioned table, but the underlying %s doesn't " +
							"implement the %s interface.",
						sinkIdentifier.asSummaryString(),
						DynamicTableSink.class.getSimpleName(),
						SupportsPartitioning.class.getSimpleName()
					)
				);
			}
		}

		final Map<String, String> staticPartitions = sinkOperation.getStaticPartitions();
		staticPartitions.keySet().forEach(p -> {
			if (!partitionKeys.contains(p)) {
				throw new ValidationException(
					String.format(
						"Static partition column '%s' should be in the partition keys list %s for table '%s'.",
						p,
						partitionKeys,
						sinkIdentifier.asSummaryString()
					)
				);
			}
		});
	}

	private static void validateAndApplyOverwrite(
			CatalogSinkModifyOperation sinkOperation,
			ObjectIdentifier sinkIdentifier,
			DynamicTableSink sink) {
		if (!sinkOperation.isOverwrite()) {
			return;
		}
		if (!(sink instanceof SupportsOverwrite)) {
			throw new ValidationException(
				String.format(
					"INSERT OVERWRITE requires that the underlying %s of table '%s' " +
						"implements the %s interface.",
					DynamicTableSink.class.getSimpleName(),
					sinkIdentifier.asSummaryString(),
					SupportsOverwrite.class.getSimpleName()
				)
			);
		}
		final SupportsOverwrite overwriteSink = (SupportsOverwrite) sink;
		overwriteSink.applyOverwrite(sinkOperation.isOverwrite());
	}

	private static List<Integer> extractPhysicalColumns(TableSchema schema) {
		final List<TableColumn> tableColumns = schema.getTableColumns();
		return IntStream.range(0, schema.getFieldCount())
			.filter(pos -> tableColumns.get(pos).isPhysical())
			.boxed()
			.collect(Collectors.toList());
	}

	private static List<Integer> extractPersistedMetadataColumns(TableSchema schema) {
		final List<TableColumn> tableColumns = schema.getTableColumns();
		return IntStream.range(0, schema.getFieldCount())
			.filter(pos -> {
				final TableColumn tableColumn = tableColumns.get(pos);
				return tableColumn instanceof MetadataColumn && tableColumn.isPersisted();
			})
			.boxed()
			.collect(Collectors.toList());
	}

	private static int adjustByVirtualColumns(List<TableColumn> tableColumns, int pos) {
		return pos - (int) IntStream.range(0, pos).filter(i -> !tableColumns.get(i).isPersisted()).count();
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
			TableSchema schema) {
		final List<TableColumn> tableColumns = schema.getTableColumns();
		final List<Integer> metadataColumns = extractPersistedMetadataColumns(schema);

		if (metadataColumns.isEmpty()) {
			return;
		}

		if (!(sink instanceof SupportsWritingMetadata)) {
			throw new ValidationException(
				String.format(
					"Table '%s' declares persistable metadata columns, but the underlying %s " +
						"doesn't implement the %s interface. If the column should not " +
						"be persisted, it can be declared with the VIRTUAL keyword.",
					sinkIdentifier.asSummaryString(),
					DynamicTableSink.class.getSimpleName(),
					SupportsWritingMetadata.class.getSimpleName()
				)
			);
		}

		final SupportsWritingMetadata metadataSink = (SupportsWritingMetadata) sink;

		final Map<String, DataType> metadataMap = ((SupportsWritingMetadata) sink).listWritableMetadata();
		metadataColumns.forEach(pos -> {
			final MetadataColumn metadataColumn = (MetadataColumn) tableColumns.get(pos);
			final String metadataKey = metadataColumn.getMetadataAlias().orElse(metadataColumn.getName());
			final LogicalType metadataType = metadataColumn.getType().getLogicalType();
			final DataType expectedMetadataDataType = metadataMap.get(metadataKey);
			// check that metadata key is valid
			if (expectedMetadataDataType == null) {
				throw new ValidationException(
					String.format(
						"Invalid metadata key '%s' in column '%s' of table '%s'. " +
							"The %s class '%s' supports the following metadata keys for writing:\n%s",
						metadataKey,
						metadataColumn.getName(),
						sinkIdentifier.asSummaryString(),
						DynamicTableSink.class.getSimpleName(),
						sink.getClass().getName(),
						String.join("\n", metadataMap.keySet())
					)
				);
			}
			// check that types are compatible
			if (!supportsExplicitCast(metadataType, expectedMetadataDataType.getLogicalType())) {
				if (metadataKey.equals(metadataColumn.getName())) {
					throw new ValidationException(
						String.format(
							"Invalid data type for metadata column '%s' of table '%s'. " +
								"The column cannot be declared as '%s' because the type must be " +
								"castable to metadata type '%s'.",
							metadataColumn.getName(),
							sinkIdentifier.asSummaryString(),
							metadataType,
							expectedMetadataDataType.getLogicalType()
						)
					);
				} else {
					throw new ValidationException(
						String.format(
							"Invalid data type for metadata column '%s' with metadata key '%s' of table '%s'. " +
								"The column cannot be declared as '%s' because the type must be " +
								"castable to metadata type '%s'.",
							metadataColumn.getName(),
							metadataKey,
							sinkIdentifier.asSummaryString(),
							metadataType,
							expectedMetadataDataType.getLogicalType()
						)
					);
				}
			}
		});

		metadataSink.applyWritableMetadata(
			createRequiredMetadataKeys(schema, sink),
			TypeConversions.fromLogicalToDataType(createConsumedType(schema, sink)));
	}

	/**
	 * Returns the {@link DataType} that a sink should consume as the output from the runtime.
	 *
	 * <p>The format looks as follows: {@code PHYSICAL COLUMNS + PERSISTED METADATA COLUMNS}
	 */
	private static RowType createConsumedType(TableSchema schema, DynamicTableSink sink) {
		final Map<String, DataType> metadataMap = extractMetadataMap(sink);

		final Stream<RowField> physicalFields = schema
			.getTableColumns()
			.stream()
			.filter(TableColumn::isPhysical)
			.map(c -> new RowField(c.getName(), c.getType().getLogicalType()));

		final Stream<RowField> metadataFields = createRequiredMetadataKeys(schema, sink)
			.stream()
			.map(k -> new RowField(k, metadataMap.get(k).getLogicalType()));

		final List<RowField> rowFields = Stream.concat(physicalFields, metadataFields)
			.collect(Collectors.toList());

		return new RowType(false, rowFields);
	}

	private DynamicSinkUtils() {
		// no instantiation
	}
}
