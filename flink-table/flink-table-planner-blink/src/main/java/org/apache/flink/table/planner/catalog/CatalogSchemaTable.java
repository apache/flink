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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ConnectorCatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.QueryOperationCatalogView;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.sources.TableSourceUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TemporalTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import scala.Option;

import static java.lang.String.format;
import static org.apache.flink.table.util.CatalogTableStatisticsConverter.convertToTableStats;

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
	private final Catalog catalog;
	private final CatalogBaseTable catalogBaseTable;
	private final boolean isStreamingMode;
	private final boolean isTemporary;

	//~ Constructors -----------------------------------------------------------

	public CatalogSchemaTable(
			ObjectIdentifier tableIdentifier,
			Catalog catalog,
			CatalogBaseTable catalogBaseTable,
			boolean isStreaming,
			boolean isTemporary) {
		this.tableIdentifier = tableIdentifier;
		this.catalog = catalog;
		this.catalogBaseTable = catalogBaseTable;
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
		if (isTemporary || catalogBaseTable instanceof QueryOperationCatalogView) {
			return FlinkStatistic.UNKNOWN();
		}
		if (catalogBaseTable instanceof CatalogTable) {
			return FlinkStatistic.builder()
				.tableStats(extractTableStats(catalog, tableIdentifier))
				.build();
		} else {
			throw new TableException("Unsupported table type: " + catalogBaseTable);
		}
	}

	private static TableStats extractTableStats(Catalog catalog,
			ObjectIdentifier objectIdentifier) {
		final ObjectPath tablePath = objectIdentifier.toObjectPath();
		try {
			CatalogTableStatistics tableStatistics = catalog.getTableStatistics(tablePath);
			CatalogColumnStatistics columnStatistics = catalog.getTableColumnStatistics(tablePath);
			return convertToTableStats(tableStatistics, columnStatistics);
		} catch (TableNotExistException e) {
			throw new ValidationException(format(
				"Could not access table partitions for table: [%s, %s, %s]",
				objectIdentifier.getCatalogName(),
				tablePath.getDatabaseName(),
				tablePath.getObjectName()), e);
		}
	}

	private static RelDataType getRowType(RelDataTypeFactory typeFactory,
			CatalogBaseTable catalogBaseTable,
			boolean isStreamingMode) {
		final FlinkTypeFactory flinkTypeFactory = (FlinkTypeFactory) typeFactory;
		TableSchema tableSchema = catalogBaseTable.getSchema();
		final String[] fieldNames = tableSchema.getFieldNames();
		final DataType[] fieldDataTypes = tableSchema.getFieldDataTypes();
		if (catalogBaseTable instanceof ConnectorCatalogTable
			&& ((ConnectorCatalogTable) catalogBaseTable).getTableSource().isPresent()) {
			final ConnectorCatalogTable<?, ?> connectorTable = (ConnectorCatalogTable) catalogBaseTable;
			if (!isStreamingMode) {
				// If the table source is bounded, materialize the time attributes to normal TIMESTAMP type.
				// Now for ConnectorCatalogTable, there is no way to
				// deduce if it is bounded in the table environment, so the data types in TableSchema
				// always patched with TimeAttribute.
				// See ConnectorCatalogTable#calculateSourceSchema
				// for details.

				// Remove the patched time attributes type to let the TableSourceTable handle it.
				// We should remove this logic if the isBatch flag in ConnectorCatalogTable is fixed.
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
			return TableSourceUtil.getSourceRowType(flinkTypeFactory,
				fieldNames,
				fieldDataTypes,
				connectorTable.getTableSource().get(),
				Option.empty(),
				isStreamingMode);
		} else if (catalogBaseTable instanceof CatalogTable
			&& tableSchema.getWatermarkSpecs().size() > 0) {
			return TableSourceUtil.getSourceRowType(flinkTypeFactory,
				fieldNames,
				fieldDataTypes,
				tableSchema.getWatermarkSpecs().get(0),
				Option.empty(),
				isStreamingMode);
		}
		List<LogicalType> fieldLogicalTypes = Arrays.stream(fieldDataTypes).map(
			LogicalTypeDataTypeConverter::fromDataTypeToLogicalType).collect(Collectors.toList());
		return flinkTypeFactory.buildRelNodeRowType(
			JavaScalaConversionUtil.toScala(Arrays.asList(fieldNames)),
			JavaScalaConversionUtil.toScala(fieldLogicalTypes));
	}

	@Override
	public String getSysStartFieldName() {
		return "sys_start";
	}

	@Override
	public String getSysEndFieldName() {
		return "sys_end";
	}
}
