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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
  * A table schema that represents a table's structure with field names and types.
  */
@PublicEvolving
public class TableSchema {

	private static final String ATOMIC_TYPE_FIELD_NAME = "f0";

	private final String[] fieldNames;

	private final TypeInformation<?>[] fieldTypes;

	private final Map<String, Integer> fieldNameToIndex;

	public TableSchema(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		this.fieldNames = Preconditions.checkNotNull(fieldNames);
		this.fieldTypes = Preconditions.checkNotNull(fieldTypes);

		if (fieldNames.length != fieldTypes.length) {
			throw new TableException(
				"Number of field names and field types must be equal.\n" +
				"Number of names is " + fieldNames.length + ", number of types is " + fieldTypes.length + ".\n" +
				"List of field names: " + Arrays.toString(fieldNames) + "\n" +
				"List of field types: " + Arrays.toString(fieldTypes));
		}

		// validate and create name to index mapping
		fieldNameToIndex = new HashMap<>();
		final Set<String> duplicateNames = new HashSet<>();
		final Set<String> uniqueNames = new HashSet<>();
		for (int i = 0; i < fieldNames.length; i++) {
			// check for null
			Preconditions.checkNotNull(fieldTypes[i]);
			final String fieldName = Preconditions.checkNotNull(fieldNames[i]);

			// collect indices
			fieldNameToIndex.put(fieldName, i);

			// check uniqueness of field names
			if (uniqueNames.contains(fieldName)) {
				duplicateNames.add(fieldName);
			} else {
				uniqueNames.add(fieldName);
			}
		}
		if (!duplicateNames.isEmpty()) {
			throw new TableException(
				"Field names must be unique.\n" +
				"List of duplicate fields: " + duplicateNames.toString() + "\n" +
				"List of all fields: " + Arrays.toString(fieldNames));
		}
	}

	/**
	 * Returns a deep copy of the table schema.
	 */
	public TableSchema copy() {
		return new TableSchema(fieldNames.clone(), fieldTypes.clone());
	}

	/**
	 * Returns all field type information as an array.
	 */
	public TypeInformation<?>[] getFieldTypes() {
		return fieldTypes;
	}

	/**
	 * Returns the specified type information for the given field index.
	 *
	 * @param fieldIndex the index of the field
	 */
	public Optional<TypeInformation<?>> getFieldType(int fieldIndex) {
		if (fieldIndex < 0 || fieldIndex >= fieldTypes.length) {
			return Optional.empty();
		}
		return Optional.of(fieldTypes[fieldIndex]);
	}

	/**
	 * Returns the specified type information for the given field name.
	 *
	 * @param fieldName the name of the field
	 */
	public Optional<TypeInformation<?>> getFieldType(String fieldName) {
		if (fieldNameToIndex.containsKey(fieldName)) {
			return Optional.of(fieldTypes[fieldNameToIndex.get(fieldName)]);
		}
		return Optional.empty();
	}

	/**
	 * Returns the number of fields.
	 */
	public int getFieldCount() {
		return fieldNames.length;
	}

	/**
	 * Returns all field names as an array.
	 */
	public String[] getFieldNames() {
		return fieldNames;
	}

	/**
	 * Returns the specified name for the given field index.
	 *
	 * @param fieldIndex the index of the field
	 */
	public Optional<String> getFieldName(int fieldIndex) {
		if (fieldIndex < 0 || fieldIndex >= fieldNames.length) {
			return Optional.empty();
		}
		return Optional.of(fieldNames[fieldIndex]);
	}

	/**
	 * @deprecated Use {@link TableSchema#getFieldTypes()} instead. Can be dropped after 1.7.
	 */
	@Deprecated
	public TypeInformation<?>[] getTypes() {
		return getFieldTypes();
	}

	/**
	 * @deprecated Use {@link TableSchema#getFieldNames()} instead. Can be dropped after 1.7.
	 */
	@Deprecated
	public String[] getColumnNames() {
		return getFieldNames();
	}

	/**
	 * Converts a table schema into a (nested) type information describing a {@link Row}.
	 */
	public TypeInformation<Row> toRowType() {
		return Types.ROW_NAMED(fieldNames, fieldTypes);
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("root\n");
		for (int i = 0; i < fieldNames.length; i++) {
			sb.append(" |-- ").append(fieldNames[i]).append(": ").append(fieldTypes[i]).append('\n');
		}
		return sb.toString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		TableSchema schema = (TableSchema) o;
		return Arrays.equals(fieldNames, schema.fieldNames) &&
			Arrays.equals(fieldTypes, schema.fieldTypes);
	}

	@Override
	public int hashCode() {
		int result = Arrays.hashCode(fieldNames);
		result = 31 * result + Arrays.hashCode(fieldTypes);
		return result;
	}

	/**
	 * Creates a table schema from a {@link TypeInformation} instance. If the type information is
	 * a {@link CompositeType}, the field names and types for the composite type are used to
	 * construct the {@link TableSchema} instance. Otherwise, a table schema with a single field
	 * is created. The field name is "f0" and the field type the provided type.
	 *
	 * @param typeInfo The {@link TypeInformation} from which the table schema is generated.
	 * @return The table schema that was generated from the given {@link TypeInformation}.
	 */
	public static TableSchema fromTypeInfo(TypeInformation<?> typeInfo) {
		if (typeInfo instanceof CompositeType<?>) {
			final CompositeType<?> compositeType = (CompositeType<?>) typeInfo;
			// get field names and types from composite type
			final String[] fieldNames = compositeType.getFieldNames();
			final TypeInformation<?>[] fieldTypes = new TypeInformation[fieldNames.length];
			for (int i = 0; i < fieldTypes.length; i++) {
				fieldTypes[i] = compositeType.getTypeAt(i);
			}
			return new TableSchema(fieldNames, fieldTypes);
		} else {
			// create table schema with a single field named "f0" of the given type.
			return new TableSchema(
				new String[]{ATOMIC_TYPE_FIELD_NAME},
				new TypeInformation<?>[]{typeInfo});
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Builder for creating a {@link TableSchema}.
	 */
	public static class Builder {

		private List<String> fieldNames;

		private List<TypeInformation<?>> fieldTypes;

		public Builder() {
			fieldNames = new ArrayList<>();
			fieldTypes = new ArrayList<>();
		}

		/**
		 * Add a field with name and type. The call order of this method determines the order
		 * of fields in the schema.
		 */
		public Builder field(String name, TypeInformation<?> type) {
			Preconditions.checkNotNull(name);
			Preconditions.checkNotNull(type);
			fieldNames.add(name);
			fieldTypes.add(type);
			return this;
		}

		/**
		 * Returns a {@link TableSchema} instance.
		 */
		public TableSchema build() {
			return new TableSchema(
				fieldNames.toArray(new String[0]),
				fieldTypes.toArray(new TypeInformation<?>[0]));
		}
	}
}
