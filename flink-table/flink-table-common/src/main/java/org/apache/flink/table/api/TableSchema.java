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
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
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
import java.util.stream.IntStream;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.Field;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;

/**
 * A table schema that represents a table's structure with field names and data types.
 */
@PublicEvolving
public class TableSchema {

	private static final String ATOMIC_TYPE_FIELD_NAME = "f0";

	private final String[] fieldNames;

	private final DataType[] fieldDataTypes;

	private final Map<String, Integer> fieldNameToIndex;

	private TableSchema(String[] fieldNames, DataType[] fieldDataTypes) {
		this.fieldNames = Preconditions.checkNotNull(fieldNames);
		this.fieldDataTypes = Preconditions.checkNotNull(fieldDataTypes);

		if (fieldNames.length != fieldDataTypes.length) {
			throw new TableException(
				"Number of field names and field data types must be equal.\n" +
					"Number of names is " + fieldNames.length + ", number of data types is " + fieldDataTypes.length + ".\n" +
					"List of field names: " + Arrays.toString(fieldNames) + "\n" +
					"List of field data types: " + Arrays.toString(fieldDataTypes));
		}

		// validate and create name to index mapping
		fieldNameToIndex = new HashMap<>();
		final Set<String> duplicateNames = new HashSet<>();
		final Set<String> uniqueNames = new HashSet<>();
		for (int i = 0; i < fieldNames.length; i++) {
			// check for null
			Preconditions.checkNotNull(fieldDataTypes[i]);
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
	 * @deprecated Use the {@link Builder} instead.
	 */
	@Deprecated
	public TableSchema(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		this(fieldNames, fromLegacyInfoToDataType(fieldTypes));
	}

	/**
	 * Returns a deep copy of the table schema.
	 */
	public TableSchema copy() {
		return new TableSchema(fieldNames.clone(), fieldDataTypes.clone());
	}

	/**
	 * Returns all field data types as an array.
	 */
	public DataType[] getFieldDataTypes() {
		return fieldDataTypes;
	}

	/**
	 * @deprecated This method will be removed in future versions as it uses the old type system. It
	 *             is recommended to use {@link #getFieldDataTypes()} instead which uses the new type
	 *             system based on {@link DataTypes}. Please make sure to use either the old or the new
	 *             type system consistently to avoid unintended behavior. See the website documentation
	 *             for more information.
	 */
	@Deprecated
	public TypeInformation<?>[] getFieldTypes() {
		return fromDataTypeToLegacyInfo(fieldDataTypes);
	}

	/**
	 * Returns the specified data type for the given field index.
	 *
	 * @param fieldIndex the index of the field
	 */
	public Optional<DataType> getFieldDataType(int fieldIndex) {
		if (fieldIndex < 0 || fieldIndex >= fieldDataTypes.length) {
			return Optional.empty();
		}
		return Optional.of(fieldDataTypes[fieldIndex]);
	}

	/**
	 * @deprecated This method will be removed in future versions as it uses the old type system. It
	 *             is recommended to use {@link #getFieldDataType(int)} instead which uses the new type
	 *             system based on {@link DataTypes}. Please make sure to use either the old or the new
	 *             type system consistently to avoid unintended behavior. See the website documentation
	 *             for more information.
	 */
	@Deprecated
	public Optional<TypeInformation<?>> getFieldType(int fieldIndex) {
		return getFieldDataType(fieldIndex)
			.map(TypeConversions::fromDataTypeToLegacyInfo);
	}

	/**
	 * Returns the specified data type for the given field name.
	 *
	 * @param fieldName the name of the field
	 */
	public Optional<DataType> getFieldDataType(String fieldName) {
		if (fieldNameToIndex.containsKey(fieldName)) {
			return Optional.of(fieldDataTypes[fieldNameToIndex.get(fieldName)]);
		}
		return Optional.empty();
	}

	/**
	 * @deprecated This method will be removed in future versions as it uses the old type system. It
	 *             is recommended to use {@link #getFieldDataType(String)} instead which uses the new type
	 *             system based on {@link DataTypes}. Please make sure to use either the old or the new
	 *             type system consistently to avoid unintended behavior. See the website documentation
	 *             for more information.
	 */
	@Deprecated
	public Optional<TypeInformation<?>> getFieldType(String fieldName) {
		return getFieldDataType(fieldName)
			.map(TypeConversions::fromDataTypeToLegacyInfo);
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
	 * Converts a table schema into a (nested) data type describing a {@link DataTypes#ROW(Field...)}.
	 */
	public DataType toRowDataType() {
		final Field[] fields = IntStream.range(0, fieldDataTypes.length)
			.mapToObj(i -> FIELD(fieldNames[i], fieldDataTypes[i]))
			.toArray(Field[]::new);
		return ROW(fields);
	}

	/**
	 * @deprecated Use {@link #toRowDataType()} instead.
	 */
	@Deprecated
	@SuppressWarnings("unchecked")
	public TypeInformation<Row> toRowType() {
		return (TypeInformation<Row>) fromDataTypeToLegacyInfo(toRowDataType());
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("root\n");
		for (int i = 0; i < fieldNames.length; i++) {
			sb.append(" |-- ").append(fieldNames[i]).append(": ").append(fieldDataTypes[i]).append('\n');
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
			Arrays.equals(fieldDataTypes, schema.fieldDataTypes);
	}

	@Override
	public int hashCode() {
		int result = Arrays.hashCode(fieldNames);
		result = 31 * result + Arrays.hashCode(fieldDataTypes);
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
	 *
	 * @deprecated This method will be removed soon. Use {@link DataTypes} to declare types.
	 */
	@Deprecated
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

		private List<DataType> fieldDataTypes;

		public Builder() {
			fieldNames = new ArrayList<>();
			fieldDataTypes = new ArrayList<>();
		}

		/**
		 * Add a field with name and data type.
		 *
		 * <p>The call order of this method determines the order of fields in the schema.
		 */
		public Builder field(String name, DataType dataType) {
			Preconditions.checkNotNull(name);
			Preconditions.checkNotNull(dataType);
			fieldNames.add(name);
			fieldDataTypes.add(dataType);
			return this;
		}

		/**
		 * Add an array of fields with names and data types.
		 *
		 * <p>The call order of this method determines the order of fields in the schema.
		 */
		public Builder fields(String[] names, DataType[] dataTypes) {
			Preconditions.checkNotNull(names);
			Preconditions.checkNotNull(dataTypes);

			fieldNames.addAll(Arrays.asList(names));
			fieldDataTypes.addAll(Arrays.asList(dataTypes));
			return this;
		}

		/**
		 * @deprecated This method will be removed in future versions as it uses the old type system. It
		 *             is recommended to use {@link #field(String, DataType)} instead which uses the new type
		 *             system based on {@link DataTypes}. Please make sure to use either the old or the new
		 *             type system consistently to avoid unintended behavior. See the website documentation
		 *             for more information.
		 */
		@Deprecated
		public Builder field(String name, TypeInformation<?> typeInfo) {
			return field(name, fromLegacyInfoToDataType(typeInfo));
		}

		/**
		 * Returns a {@link TableSchema} instance.
		 */
		public TableSchema build() {
			return new TableSchema(
				fieldNames.toArray(new String[0]),
				fieldDataTypes.toArray(new DataType[0]));
		}
	}
}
