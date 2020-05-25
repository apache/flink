/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.catalog;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.factories.TableFactoryUtil;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.factories.TableSourceFactoryContextImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.SqlExprToRexConverterFactory;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.sources.TableSourceUtil;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.TableSourceValidation;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TemporalTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.List;
import java.util.Optional;

/**
 * Represents a wrapper for {@link CatalogBaseTable} in {@link org.apache.calcite.schema.Schema}.
 *
 * <p>This table would be converted to
 * {@link org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase}
 * based on its internal source type during sql-to-rel conversion.
 *
 * <p>See
 * {@link org.apache.flink.table.planner.plan.FlinkCalciteCatalogReader#getTable(List)}
 * for details.
 */
public class CatalogSchemaTable extends AbstractTable implements TemporalTable {
	//~ Instance fields --------------------------------------------------------

	private final ObjectIdentifier tableIdentifier;
	private final CatalogBaseTable catalogBaseTable;
	private final FlinkStatistic statistic;
	private final SqlExprToRexConverterFactory converterFactory;
	private final boolean isStreamingMode;
	private final boolean isTemporary;
	private final Catalog catalog;

	//~ Constructors -----------------------------------------------------------

	/**
	 * Create a CatalogSchemaTable instance.
	 *
	 * @param tableIdentifier Table identifier
	 * @param catalogBaseTable CatalogBaseTable instance which exists in the catalog
	 * @param statistic Table statistics
	 * @param catalog The catalog which the schema table belongs to
	 * @param converterFactory The SQL expression converter factory is used to derive correct result
	 *                         type of computed column, because the date type of computed column
	 *                         from catalog table is not trusted.
	 * @param isStreaming If the table is for streaming mode
	 * @param isTemporary If the table is temporary
	 */
	public CatalogSchemaTable(
			ObjectIdentifier tableIdentifier,
			CatalogBaseTable catalogBaseTable,
			FlinkStatistic statistic,
			Catalog catalog,
			SqlExprToRexConverterFactory converterFactory,
			boolean isStreaming,
			boolean isTemporary) {
		this.tableIdentifier = tableIdentifier;
		this.catalogBaseTable = catalogBaseTable;
		this.statistic = statistic;
		this.catalog = catalog;
		this.converterFactory = converterFactory;
		this.isStreamingMode = isStreaming;
		this.isTemporary = isTemporary;
	}

	//~ Methods ----------------------------------------------------------------

	public Catalog getCatalog() {
		return catalog;
	}

	public ObjectIdentifier getTableIdentifier() {
		return tableIdentifier;
	}

	public CatalogBaseTable getCatalogTable() {
		return catalogBaseTable;
	}

	public boolean isTemporary() {
		return isTemporary;
	}

	public boolean isStreamingMode() {
		return isStreamingMode;
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		return getRowType(typeFactory, catalogBaseTable, isStreamingMode);
	}

	@Override
	public FlinkStatistic getStatistic() {
		return statistic;
	}

	private RelDataType getRowType(RelDataTypeFactory typeFactory,
			CatalogBaseTable catalogBaseTable,
			boolean isStreamingMode) {
		final FlinkTypeFactory flinkTypeFactory = (FlinkTypeFactory) typeFactory;
		TableSchema tableSchema = catalogBaseTable.getSchema();
		final DataType[] fieldDataTypes = tableSchema.getFieldDataTypes();
		if (!isStreamingMode
			&& catalogBaseTable instanceof ConnectorCatalogTable
			&& ((ConnectorCatalogTable) catalogBaseTable).getTableSource().isPresent()) {
			// If the table source is bounded, materialize the time attributes to normal TIMESTAMP type.
			// Now for ConnectorCatalogTable, there is no way to
			// deduce if it is bounded in the table environment, so the data types in TableSchema
			// always patched with TimeAttribute.
			// See ConnectorCatalogTable#calculateSourceSchema
			// for details.

			// Remove the patched time attributes type to let the TableSourceTable handle it.
			// We should remove this logic if the isBatch flag in ConnectorCatalogTable is fixed.
			// TODO: Fix FLINK-14844.
			for (int i = 0; i < fieldDataTypes.length; i++) {
				LogicalType lt = fieldDataTypes[i].getLogicalType();
				if (lt instanceof TimestampType
					&& (((TimestampType) lt).getKind() == TimestampKind.PROCTIME
					|| ((TimestampType) lt).getKind() == TimestampKind.ROWTIME)) {
					int precision = ((TimestampType) lt).getPrecision();
					fieldDataTypes[i] = DataTypes.TIMESTAMP(precision);
				}
			}
		}

		// The following block is a workaround to support tables defined by TableEnvironment.connect() and
		// the actual table sources implement DefinedProctimeAttribute/DefinedRowtimeAttributes.
		// It should be removed after we remove DefinedProctimeAttribute/DefinedRowtimeAttributes.
		Optional<TableSource<?>> sourceOpt = findAndCreateTableSource();
		if (isStreamingMode
			&& tableSchema.getTableColumns().stream().noneMatch(TableColumn::isGenerated)
			&& tableSchema.getWatermarkSpecs().isEmpty()
			&& sourceOpt.isPresent()) {
			TableSource<?> source = sourceOpt.get();
			if (TableSourceValidation.hasProctimeAttribute(source)
					|| TableSourceValidation.hasRowtimeAttribute(source)) {
				// If the table is defined by TableEnvironment.connect(), and use the legacy proctime and rowtime
				// descriptors, the TableSchema should fallback to ConnectorCatalogTable#calculateSourceSchema
				tableSchema = ConnectorCatalogTable.calculateSourceSchema(source, false);
			}
		}

		return TableSourceUtil.getSourceRowTypeFromSchema(
				converterFactory,
				flinkTypeFactory,
				tableSchema,
				isStreamingMode);
	}

	@Override
	public String getSysStartFieldName() {
		return "sys_start";
	}

	@Override
	public String getSysEndFieldName() {
		return "sys_end";
	}

	private Optional<TableSource<?>> findAndCreateTableSource() {
		Optional<TableSource<?>> tableSource = Optional.empty();
		try {
			if (catalogBaseTable instanceof CatalogTableImpl) {
				// Use an empty config for TableSourceFactoryContextImpl since we can't fetch the
				// actual TableConfig here. And currently the empty config do not affect the logic.
				ReadableConfig config = new TableConfig().getConfiguration();
				TableSourceFactory.Context context =
					new TableSourceFactoryContextImpl(tableIdentifier, (CatalogTable) catalogBaseTable, config);
				TableSource<?> source = TableFactoryUtil.findAndCreateTableSource(context);
				if (source instanceof StreamTableSource) {
					if (!isStreamingMode && !((StreamTableSource) source).isBounded()) {
						throw new ValidationException("Cannot query on an unbounded source in batch mode, but " +
							tableIdentifier.asSummaryString() + " is unbounded.");
					}
					tableSource = Optional.of(source);
				} else {
					throw new ValidationException("Catalog tables only support " +
						"StreamTableSource and InputFormatTableSource.");
				}
			}
		} catch (Exception e) {
			tableSource = Optional.empty();
		}
		return tableSource;
	}
}
