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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.calcite.FlinkRelBuilder;
import org.apache.flink.table.calcite.FlinkTypeFactory;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

/**
 * A bridge between a Flink's specific {@link TableOperationCatalogView} and a Calcite's
 * {@link org.apache.calcite.schema.Table}. It implements {@link TranslatableTable} interface. This enables
 * direct translation from {@link org.apache.flink.table.operations.TableOperation} to {@link RelNode}.
 *
 * <p>NOTE: Due to legacy inconsistency in null handling in the {@link TableSchema} the translation might introduce
 * additional cast to comply with manifested schema in
 * {@link TableOperationCatalogViewTable#getRowType(RelDataTypeFactory)}.
 */
@Internal
public class TableOperationCatalogViewTable extends AbstractTable implements TranslatableTable {
	private final TableOperationCatalogView catalogView;
	private final RelProtoDataType rowType;

	public static TableOperationCatalogViewTable createCalciteTable(TableOperationCatalogView catalogView) {
		return new TableOperationCatalogViewTable(catalogView, typeFactory -> {
			TableSchema tableSchema = catalogView.getSchema();
			return ((FlinkTypeFactory) typeFactory).buildLogicalRowType(tableSchema);
		});
	}

	private TableOperationCatalogViewTable(TableOperationCatalogView catalogView, RelProtoDataType rowType) {
		this.catalogView = catalogView;
		this.rowType = rowType;
	}

	@Override
	public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
		FlinkRelBuilder relBuilder = FlinkRelBuilder.of(context.getCluster(), relOptTable);

		RelNode relNode = relBuilder.tableOperation(catalogView.getTableOperation()).build();
		return RelOptUtil.createCastRel(relNode, rowType.apply(relBuilder.getTypeFactory()), false);
	}

	@Override
	public RelDataType getRowType(RelDataTypeFactory typeFactory) {
		return rowType.apply(typeFactory);
	}
}
