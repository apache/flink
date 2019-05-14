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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.calcite.FlinkTypeFactory;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Thin wrapper around Calcite specific {@link Table}, this is a temporary solution
 * that allows to register those tables in the {@link CatalogManager}.
 * TODO remove once we decouple TableEnvironment from Calcite.
 */
@Internal
public class CalciteCatalogTable implements CatalogBaseTable {
	private final Table table;
	private final FlinkTypeFactory typeFactory;

	public CalciteCatalogTable(Table table, FlinkTypeFactory typeFactory) {
		this.table = table;
		this.typeFactory = typeFactory;
	}

	public Table getTable() {
		return table;
	}

	@Override
	public Map<String, String> getProperties() {
		return Collections.emptyMap();
	}

	@Override
	public TableSchema getSchema() {
		RelDataType relDataType = table.getRowType(typeFactory);

		String[] fieldNames = relDataType.getFieldNames().toArray(new String[0]);
		TypeInformation[] fieldTypes = relDataType.getFieldList()
			.stream()
			.map(field -> FlinkTypeFactory.toTypeInfo(field.getType())).toArray(TypeInformation[]::new);

		return new TableSchema(fieldNames, fieldTypes);
	}

	@Override
	public String getComment() {
		return null;
	}

	@Override
	public CatalogBaseTable copy() {
		return this;
	}

	@Override
	public Optional<String> getDescription() {
		return Optional.empty();
	}

	@Override
	public Optional<String> getDetailedDescription() {
		return Optional.empty();
	}
}
