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

package org.apache.flink.table.planner.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlValidatorImpl;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A workaround for adjusting types of ROW nested fields.
 *
 * <p>The {@link SqlNameMatcher} is used in {@link SqlValidatorImpl} when deriving a type of
 * a nested field of a Table. E.g {@code SELECT `row`.`nested` FROM table}. See {@link #field(RelDataType, String)}
 * for more information.
 */
public class FlinkSqlNameMatcher implements SqlNameMatcher {

	private final SqlNameMatcher baseMatcher;
	private final RelDataTypeFactory typeFactory;

	public FlinkSqlNameMatcher(SqlNameMatcher baseMatcher, RelDataTypeFactory typeFactory) {
		this.baseMatcher = baseMatcher;
		this.typeFactory = typeFactory;
	}

	@Override
	public boolean isCaseSensitive() {
		return baseMatcher.isCaseSensitive();
	}

	@Override
	public boolean matches(String string, String name) {
		return baseMatcher.matches(string, name);
	}

	@Override
	public <K extends List<String>, V> V get(
			Map<K, V> map,
			List<String> prefixNames,
			List<String> names) {
		return baseMatcher.get(map, prefixNames, names);
	}

	@Override
	public String bestString() {
		return baseMatcher.bestString();
	}

	/**
	 * Compared to the original method we adjust the nullability of the nested column based on
	 * the nullability of the enclosing type.
	 *
	 * <p>If the fields type is NOT NULL, but the enclosing ROW is nullable we still can produce nulls.
	 */
	@Override
	public RelDataTypeField field(
			RelDataType rowType,
			String fieldName) {
		RelDataTypeField field = baseMatcher.field(rowType, fieldName);
		if (field != null && rowType.isNullable() && !field.getType().isNullable()) {
			RelDataType typeWithNullability = typeFactory.createTypeWithNullability(field.getType(), true);
			return new RelDataTypeFieldImpl(field.getName(), field.getIndex(), typeWithNullability);
		}

		return field;
	}

	@Override
	public int frequency(Iterable<String> names, String name) {
		return baseMatcher.frequency(names, name);
	}

	@Override
	public Set<String> createSet() {
		return baseMatcher.createSet();
	}
}
