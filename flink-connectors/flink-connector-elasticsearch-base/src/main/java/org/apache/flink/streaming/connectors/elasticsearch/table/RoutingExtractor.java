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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.function.Function;

/**
 * An extractor for a Elasticsearch routing from a {@link RowData}.
 */
@Internal
public class RoutingExtractor {
	private RoutingExtractor() {
	}

	public static Function<RowData, String> createRoutingExtractor(
		TableSchema schema,
		@Nullable String filedName) {
		if (filedName == null) {
			return null;
		}
		List<TableColumn> tableColumns = schema.getTableColumns();
		for (int i = 0; i < schema.getFieldCount(); i++) {
			TableColumn column = tableColumns.get(i);
			if (column.getName().equals(filedName)) {
				RowData.FieldGetter fieldGetter = RowData.createFieldGetter(
					column.getType().getLogicalType(),
					i);
				return (Function<RowData, String> & Serializable) (row) -> {
					Object fieldOrNull = fieldGetter.getFieldOrNull(row);
					if (fieldOrNull != null) {
						return fieldOrNull.toString();
					} else {
						return null;
					}
				};
			}
		}
		throw new IllegalArgumentException("Filed " + filedName + " not exist in table schema.");
	}
}
