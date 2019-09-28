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

package org.apache.flink.table.planner.plan;

import org.apache.flink.table.planner.plan.schema.FlinkRelOptTable;
import org.apache.flink.table.planner.plan.schema.FlinkTable;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.validate.SqlNameMatchers;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Flink specific {@link CalciteCatalogReader} that changes the RelOptTable which wrapped a
 * FlinkTable to a {@link FlinkRelOptTable}.
 */
public class FlinkCalciteCatalogReader extends CalciteCatalogReader {

	public FlinkCalciteCatalogReader(
		CalciteSchema rootSchema,
		List<List<String>> defaultSchemas,
		RelDataTypeFactory typeFactory,
		CalciteConnectionConfig config) {

		super(
			rootSchema,
			SqlNameMatchers.withCaseSensitive(config != null && config.caseSensitive()),
				Stream.concat(
					defaultSchemas.stream(),
					Stream.of(Collections.<String>emptyList())
				).collect(Collectors.toList()),
			typeFactory,
			config);
	}

	@Override
	public Prepare.PreparingTable getTable(List<String> names) {
		Prepare.PreparingTable originRelOptTable = super.getTable(names);
		if (originRelOptTable == null) {
			return null;
		} else {
			// Wrap FlinkTable as FlinkRelOptTable to use in query optimization.
			FlinkTable table = originRelOptTable.unwrap(FlinkTable.class);
			if (table != null) {
				return FlinkRelOptTable.create(
					originRelOptTable.getRelOptSchema(),
					originRelOptTable.getRowType(),
					originRelOptTable.getQualifiedName(),
					table);
			} else {
				return originRelOptTable;
			}
		}
	}
}
