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

package org.apache.flink.table.planner.plan.schema;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableColumn.ComputedColumn;
import org.apache.flink.table.api.TableColumn.MetadataColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.SqlExprToRexConverter;
import org.apache.flink.table.planner.calcite.SqlExprToRexConverterFactory;
import org.apache.flink.table.planner.catalog.CatalogSchemaTable;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.sources.DynamicSourceUtils.createProducedType;
import static org.apache.flink.table.planner.sources.DynamicSourceUtils.prepareDynamicSource;

/**
 * A {@link FlinkPreparingTableBase} implementation which defines the interfaces required to translate
 * the Calcite {@link RelOptTable} to the Flink specific {@link TableSourceTable}.
 *
 * <p>This table is only used to translate the {@link CatalogTable} into {@link TableSourceTable} during
 * the last phase of the SQL-to-rel conversion, it is not necessary anymore once the SQL node was converted
 * to a relational expression.
 */
public final class CatalogSourceTable extends FlinkPreparingTableBase {

	private final CatalogSchemaTable schemaTable;

	private final CatalogTable catalogTable;

	public CatalogSourceTable(
			RelOptSchema relOptSchema,
			List<String> names,
			RelDataType rowType,
			CatalogSchemaTable schemaTable,
			CatalogTable catalogTable) {
		super(relOptSchema, rowType, names, schemaTable.getStatistic());
		this.schemaTable = schemaTable;
		this.catalogTable = catalogTable;
	}

	@Override
	public RelNode toRel(ToRelContext toRelContext) {
		final RelOptCluster cluster = toRelContext.getCluster();
		final List<RelHint> hints = toRelContext.getTableHints();
		final FlinkContext context = ShortcutUtils.unwrapContext(cluster);
		final FlinkTypeFactory typeFactory = ShortcutUtils.unwrapTypeFactory(cluster);
		final FlinkRelBuilder relBuilder = FlinkRelBuilder.of(cluster, relOptSchema);

		// 0. finalize catalog table
		final Map<String, String> hintedOptions = FlinkHints.getHintedOptions(hints);
		final CatalogTable catalogTable = createFinalCatalogTable(context, hintedOptions);

		// 1. create and prepare table source
		final DynamicTableSource tableSource = createDynamicTableSource(context, catalogTable);
		prepareDynamicSource(
			schemaTable.getTableIdentifier(),
			catalogTable,
			tableSource,
			schemaTable.isStreamingMode(),
			context.getTableConfig());

		// 2. push table scan
		pushTableScan(
			relBuilder,
			cluster,
			catalogTable,
			tableSource,
			typeFactory,
			hints);

		// 3. push project for non-physical columns
		final TableSchema schema = catalogTable.getSchema();
		if (!TableSchemaUtils.containsPhysicalColumnsOnly(schema)) {
			pushMetadataProjection(relBuilder, typeFactory, schema);
			pushGeneratedProjection(context, relBuilder, schema);
		}

		// 4. push watermark assigner
		if (schemaTable.isStreamingMode() && !schema.getWatermarkSpecs().isEmpty()) {
			pushWatermarkAssigner(context, relBuilder, schema);
		}

		return relBuilder.build();
	}

	/**
	 * Creates a specialized node for assigning watermarks.
	 */
	private void pushWatermarkAssigner(
			FlinkContext context,
			FlinkRelBuilder relBuilder,
			TableSchema schema) {
		final RelDataType inputRelDataType = relBuilder.peek().getRowType();
		final SqlExprToRexConverterFactory factory = context.getSqlExprToRexConverterFactory();
		final SqlExprToRexConverter converter = factory.create(inputRelDataType);

		final WatermarkSpec watermarkSpec = schema.getWatermarkSpecs().get(0);

		final String rowtimeColumn = watermarkSpec.getRowtimeAttribute();
		final int rowtimeColumnIdx = inputRelDataType.getFieldNames().indexOf(rowtimeColumn);

		final RexNode watermarkRexNode = converter.convertToRexNode(watermarkSpec.getWatermarkExpr());

		relBuilder.watermark(rowtimeColumnIdx, watermarkRexNode);
	}

	/**
	 * Creates a projection that adds computed columns and finalizes the the table schema.
	 */
	private void pushGeneratedProjection(
			FlinkContext context,
			FlinkRelBuilder relBuilder,
			TableSchema schema) {
		final SqlExprToRexConverterFactory factory = context.getSqlExprToRexConverterFactory();
		final SqlExprToRexConverter converter = factory.create(relBuilder.peek().getRowType());

		final List<RexNode> projection = schema
			.getTableColumns()
			.stream()
			.map(c -> {
				if (c instanceof ComputedColumn) {
					final ComputedColumn computedColumn = (ComputedColumn) c;
					return converter.convertToRexNode(computedColumn.getExpression());
				} else {
					return relBuilder.field(c.getName());
				}
			})
			.collect(Collectors.toList());

		relBuilder.projectNamed(projection, Arrays.asList(schema.getFieldNames()), true);
	}

	/**
	 * Creates a projection that reorders physical and metadata columns according to the given schema.
	 * It casts metadata columns into the expected data type to be accessed by computed columns in the next step.
	 * Computed columns are ignored here.
	 *
	 * @see SupportsReadingMetadata
	 */
	private void pushMetadataProjection(
			FlinkRelBuilder relBuilder,
			FlinkTypeFactory typeFactory,
			TableSchema schema) {
		final RexBuilder rexBuilder = relBuilder.getRexBuilder();

		final List<String> fieldNames = schema
			.getTableColumns()
			.stream()
			.filter(c -> !(c instanceof ComputedColumn))
			.map(TableColumn::getName)
			.collect(Collectors.toList());

		final List<RexNode> fieldNodes = schema
			.getTableColumns()
			.stream()
			.filter(c -> !(c instanceof ComputedColumn))
			.map(c -> {
				final RelDataType relDataType = typeFactory.createFieldTypeFromLogicalType(c.getType().getLogicalType());
				if (c instanceof MetadataColumn) {
					final MetadataColumn metadataColumn = (MetadataColumn) c;
					final String metadataKey = metadataColumn.getMetadataAlias().orElse(metadataColumn.getName());
					return rexBuilder.makeAbstractCast(relDataType, relBuilder.field(metadataKey));
				} else {
					return relBuilder.field(c.getName());
				}
			})
			.collect(Collectors.toList());

		relBuilder.projectNamed(fieldNodes, fieldNames, true);
	}

	private void pushTableScan(
			FlinkRelBuilder relBuilder,
			RelOptCluster cluster,
			CatalogTable catalogTable,
			DynamicTableSource tableSource,
			FlinkTypeFactory typeFactory,
			List<RelHint> hints) {
		final RowType producedType = createProducedType(catalogTable.getSchema(), tableSource);
		final RelDataType producedRelDataType = typeFactory.buildRelNodeRowType(producedType);

		final TableSourceTable tableSourceTable = new TableSourceTable(
			relOptSchema,
			schemaTable.getTableIdentifier(),
			producedRelDataType,
			statistic,
			tableSource,
			schemaTable.isStreamingMode(),
			catalogTable,
			new String[0]);

		final LogicalTableScan scan = LogicalTableScan.create(cluster, tableSourceTable, hints);
		relBuilder.push(scan);
	}

	private CatalogTable createFinalCatalogTable(
			FlinkContext context,
			Map<String, String> hintedOptions) {
		if (hintedOptions.isEmpty()) {
			return catalogTable;
		}
		final ReadableConfig config = context.getTableConfig().getConfiguration();
		if (!config.get(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED)) {
			throw new ValidationException(
				String.format(
					"The '%s' hint is allowed only when the config option '%s' is set to true.",
					FlinkHints.HINT_NAME_OPTIONS,
					TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED.key()
				)
			);
		}
		return catalogTable.copy(FlinkHints.mergeTableOptions(hintedOptions, catalogTable.getOptions()));
	}

	private DynamicTableSource createDynamicTableSource(
			FlinkContext context,
			CatalogTable catalogTable) {
		final ReadableConfig config = context.getTableConfig().getConfiguration();
		return FactoryUtil.createTableSource(
			schemaTable.getCatalog(),
			schemaTable.getTableIdentifier(),
			catalogTable,
			config,
			Thread.currentThread().getContextClassLoader(),
			schemaTable.isTemporary());
	}
}
