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
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.util.Preconditions;

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
		return builder.build();
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
}
