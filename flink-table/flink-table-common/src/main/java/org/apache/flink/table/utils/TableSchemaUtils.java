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

package org.apache.flink.table.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.WatermarkSpec;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Utilities to {@link TableSchema}.
 */
@Internal
public class TableSchemaUtils {

	/**
	 * Return {@link TableSchema} which consists of all physical columns. That means, the computed
	 * columns are filtered out.
	 *
	 * <p>Readers(or writers) such as {@link TableSource} and {@link TableSink} should use this physical
	 * schema to generate {@link TableSource#getProducedDataType()} and {@link TableSource#getTableSchema()}
	 * rather than using the raw TableSchema which may contains computed columns.
	 */
	public static TableSchema getPhysicalSchema(TableSchema tableSchema) {
		Preconditions.checkNotNull(tableSchema);
		TableSchema.Builder builder = new TableSchema.Builder();
		tableSchema.getTableColumns().forEach(
			tableColumn -> {
				if (!tableColumn.isGenerated()) {
					builder.field(tableColumn.getName(), tableColumn.getType());
				}
			});
		tableSchema.getPrimaryKey().ifPresent(
			uniqueConstraint -> builder.primaryKey(uniqueConstraint.getColumns().toArray(new String[0]))
		);
		return builder.build();
	}

	/**
	 * Creates a new {@link TableSchema} with the projected fields from another {@link TableSchema}.
	 * The new {@link TableSchema} doesn't contain any primary key or watermark information.
	 *
	 * @see org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown
	 */
	public static TableSchema projectSchema(TableSchema tableSchema, int[][] projectedFields) {
		checkArgument(!containsGeneratedColumns(tableSchema), "It's illegal to project on a schema contains computed columns.");
		TableSchema.Builder schemaBuilder = TableSchema.builder();
		List<TableColumn> tableColumns = tableSchema.getTableColumns();
		for (int[] fieldPath : projectedFields) {
			checkArgument(fieldPath.length == 1, "Nested projection push down is not supported yet.");
			TableColumn column = tableColumns.get(fieldPath[0]);
			schemaBuilder.field(column.getName(), column.getType());
		}
		return schemaBuilder.build();
	}

	/**
	 * Returns true if there are any generated columns in the given {@link TableColumn}.
	 */
	public static boolean containsGeneratedColumns(TableSchema schema) {
		Preconditions.checkNotNull(schema);
		return schema.getTableColumns().stream().anyMatch(TableColumn::isGenerated);
	}

	/**
	 * Throws exception if the given {@link TableSchema} contains any generated columns.
	 */
	public static TableSchema checkNoGeneratedColumns(TableSchema schema) {
		Preconditions.checkNotNull(schema);
		if (containsGeneratedColumns(schema)) {
			throw new ValidationException(
				"The given schema contains generated columns, schema: " + schema.toString());
		}
		return schema;
	}

	/**
	 * Returns the field indices of primary key in the physical columns of
	 * this schema (not include computed columns).
	 */
	public static int[] getPrimaryKeyIndices(TableSchema schema) {
		if (schema.getPrimaryKey().isPresent()) {
			RowType physicalRowType = (RowType) schema.toPhysicalRowDataType().getLogicalType();
			List<String> fieldNames = physicalRowType.getFieldNames();
			return schema.getPrimaryKey().get().getColumns().stream()
				.mapToInt(fieldNames::indexOf)
				.toArray();
		} else {
			return new int[0];
		}
	}

	/**
	 * Creates a builder with given table schema.
	 *
	 * @param oriSchema Original schema
	 * @return the builder with all the information from the given schema
	 */
	public static TableSchema.Builder builderWithGivenSchema(TableSchema oriSchema) {
		TableSchema.Builder builder = builderWithGivenColumns(oriSchema.getTableColumns());
		// Copy watermark specification.
		for (WatermarkSpec wms : oriSchema.getWatermarkSpecs()) {
			builder.watermark(
					wms.getRowtimeAttribute(),
					wms.getWatermarkExpr(),
					wms.getWatermarkExprOutputType());
		}
		// Copy primary key constraint.
		oriSchema.getPrimaryKey()
				.map(pk -> builder.primaryKey(pk.getName(),
						pk.getColumns().toArray(new String[0])));
		return builder;
	}

	/**
	 * Creates a new schema but drop the constraint with given name.
	 */
	public static TableSchema dropConstraint(TableSchema oriSchema, String constraintName) {
		// Validate the constraint name is valid.
		Optional<UniqueConstraint> uniqueConstraintOpt = oriSchema.getPrimaryKey();
		if (!uniqueConstraintOpt.isPresent()
				|| !uniqueConstraintOpt.get().getName().equals(constraintName)) {
			throw new ValidationException(
					String.format("Constraint %s to drop does not exist", constraintName));
		}
		TableSchema.Builder builder = builderWithGivenColumns(oriSchema.getTableColumns());
		// Copy watermark specification.
		for (WatermarkSpec wms : oriSchema.getWatermarkSpecs()) {
			builder.watermark(
					wms.getRowtimeAttribute(),
					wms.getWatermarkExpr(),
					wms.getWatermarkExprOutputType());
		}
		return builder.build();
	}

	/** Returns the builder with copied columns info from the given table schema. */
	private static TableSchema.Builder builderWithGivenColumns(List<TableColumn> oriColumns) {
		TableSchema.Builder builder = TableSchema.builder();
		for (TableColumn column : oriColumns) {
			if (column.isGenerated()) {
				builder.field(column.getName(), column.getType(), column.getExpr().get());
			} else {
				builder.field(column.getName(), column.getType());
			}
		}
		return builder;
	}
}
