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
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.Field;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
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

	/** Mapping from qualified field name to (nested) field type. */
	private final Map<String, DataType> fieldNameToType;

	private final List<WatermarkSpec> watermarkSpecs;

	private TableSchema(String[] fieldNames, DataType[] fieldDataTypes, List<WatermarkSpec> watermarkSpecs) {
		this.fieldNames = Preconditions.checkNotNull(fieldNames);
		this.fieldDataTypes = Preconditions.checkNotNull(fieldDataTypes);
		this.watermarkSpecs = Preconditions.checkNotNull(watermarkSpecs);

		if (fieldNames.length != fieldDataTypes.length) {
			throw new ValidationException(
				"Number of field names and field data types must be equal.\n" +
					"Number of names is " + fieldNames.length + ", number of data types is " + fieldDataTypes.length + ".\n" +
					"List of field names: " + Arrays.toString(fieldNames) + "\n" +
					"List of field data types: " + Arrays.toString(fieldDataTypes));
		}

		// validate and create name to type mapping
		fieldNameToType = new HashMap<>();
		for (int i = 0; i < fieldNames.length; i++) {
			// check for null
			DataType fieldType = Preconditions.checkNotNull(fieldDataTypes[i]);
			String fieldName = Preconditions.checkNotNull(fieldNames[i]);
			validateAndCreateNameToTypeMapping(fieldName, fieldType, "");
		}

		// validate watermark and rowtime attribute
		for (WatermarkSpec watermark : watermarkSpecs) {
			String rowtimeAttribute = watermark.getRowtimeAttribute();
			DataType rowtimeType = getFieldDataType(rowtimeAttribute)
				.orElseThrow(() -> new ValidationException(String.format(
					"Rowtime attribute '%s' is not defined in schema.", rowtimeAttribute)));
			if (rowtimeType.getLogicalType().getTypeRoot() != TIMESTAMP_WITHOUT_TIME_ZONE) {
				throw new ValidationException(String.format(
					"Rowtime attribute '%s' must be of type TIMESTAMP but is of type '%s'.",
					rowtimeAttribute, rowtimeType));
			}
			LogicalType watermarkOutputType = watermark.getWatermarkExprOutputType().getLogicalType();
			if (watermarkOutputType.getTypeRoot() != TIMESTAMP_WITHOUT_TIME_ZONE) {
				throw new ValidationException(String.format(
					"Watermark strategy '%s' must be of type TIMESTAMP but is of type '%s'.",
					watermark.getWatermarkExpressionString(),
					watermarkOutputType.asSerializableString()));
			}
		}
	}

	/**
	 * @deprecated Use the {@link Builder} instead.
	 */
	@Deprecated
	public TableSchema(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		this(fieldNames, fromLegacyInfoToDataType(fieldTypes), Collections.emptyList());
	}

	/**
	 * Returns a deep copy of the table schema.
	 */
	public TableSchema copy() {
		return new TableSchema(fieldNames.clone(), fieldDataTypes.clone(), new ArrayList<>(watermarkSpecs));
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
	 * @param fieldName the name of the field. the field name can be a nested field using a dot separator,
	 *                    e.g. "field1.innerField2"
	 */
	public Optional<DataType> getFieldDataType(String fieldName) {
		if (fieldNameToType.containsKey(fieldName)) {
			return Optional.of(fieldNameToType.get(fieldName));
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

	/**
	 * Returns a list of the watermark specification which contains rowtime attribute
	 * and watermark strategy expression.
	 *
	 * <p>NOTE: Currently, there is at most one {@link WatermarkSpec} in the list, because we don't
	 * support multiple watermarks definition yet. But in the future, we may support multiple watermarks.
	 */
	public List<WatermarkSpec> getWatermarkSpecs() {
		return watermarkSpecs;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("root\n");
		for (int i = 0; i < fieldNames.length; i++) {
			sb.append(" |-- ").append(fieldNames[i]).append(": ").append(fieldDataTypes[i]).append('\n');
		}
		if (!watermarkSpecs.isEmpty()) {
			for (WatermarkSpec watermark : watermarkSpecs) {
				sb.append(" |-- ").append("WATERMARK FOR ")
					.append(watermark.getRowtimeAttribute()).append(" AS ")
					.append(watermark.getWatermarkExpressionString());
			}
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
			Arrays.equals(fieldDataTypes, schema.fieldDataTypes) &&
			watermarkSpecs.equals(schema.getWatermarkSpecs());
	}

	@Override
	public int hashCode() {
		int result = Arrays.hashCode(fieldNames);
		result = 31 * result + Arrays.hashCode(fieldDataTypes);
		result = 31 * result + watermarkSpecs.hashCode();
		return result;
	}

	/**
	 * Creates a mapping from field name to data type, the field name can be a nested field.
	 * This is mainly used for validating whether the rowtime attribute (might be nested) exists
	 * in the schema. During creating, it also validates whether there is duplicate field names.
	 *
	 * <p>For example, a "f0" field of ROW type has two nested fields "q1" and "q2". Then the
	 * mapping will be ["f0" -> ROW, "f0.q1" -> INT, "f0.q2" -> STRING].
	 * <pre>
	 * {@code
	 *     f0 ROW<q1 INT, q2 STRING>
	 * }
	 * </pre>
	 * @param fieldName name of this field, e.g. "q1" or "q2" in the above example.
	 * @param fieldType data type of this field
	 * @param parentFieldName the field name of parent type, e.g. "f0" in the above example.
	 */
	private void validateAndCreateNameToTypeMapping(String fieldName, DataType fieldType, String parentFieldName) {
		String fullFieldName = parentFieldName.isEmpty() ? fieldName : parentFieldName + "." + fieldName;
		DataType oldType = fieldNameToType.put(fullFieldName, fieldType);
		if (oldType != null) {
			throw new ValidationException("Field names must be unique. Duplicate field: '" + fullFieldName + "'");
		}
		if (fieldType instanceof FieldsDataType) {
			Map<String, DataType> fieldDataTypes = ((FieldsDataType) fieldType).getFieldDataTypes();
			fieldDataTypes.forEach((key, value) -> validateAndCreateNameToTypeMapping(key, value, fullFieldName));
		}
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

		private final List<String> fieldNames;

		private final List<DataType> fieldDataTypes;

		private final List<WatermarkSpec> watermarkSpecs;

		public Builder() {
			fieldNames = new ArrayList<>();
			fieldDataTypes = new ArrayList<>();
			watermarkSpecs = new ArrayList<>();
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
		 * Specifies the previously defined field as an event-time attribute and specifies the watermark strategy.
		 *
		 * @param rowtimeAttribute the field name as a rowtime attribute, can be a nested field using dot separator.
		 * @param watermarkExpressionString the string representation of watermark generation expression,
		 *                                  e.g. "ts - INTERVAL '5' SECOND". The string is a qualified SQL expression
		 *                                  string (UDFs are expanded) but will not be validated by {@link TableSchema}.
		 * @param watermarkExprOutputType the data type of the computation result of watermark generation expression.
		 *                                Whether the data type equals to the output type of expression will also
		 *                                not be validated by {@link TableSchema}.
		 */
		public Builder watermark(String rowtimeAttribute, String watermarkExpressionString, DataType watermarkExprOutputType) {
			Preconditions.checkNotNull(rowtimeAttribute);
			Preconditions.checkNotNull(watermarkExpressionString);
			Preconditions.checkNotNull(watermarkExprOutputType);
			if (!this.watermarkSpecs.isEmpty()) {
				throw new IllegalStateException("Multiple watermark definition is not supported yet.");
			}
			this.watermarkSpecs.add(new WatermarkSpec(rowtimeAttribute, watermarkExpressionString, watermarkExprOutputType));
			return this;
		}

		/**
		 * Returns a {@link TableSchema} instance.
		 */
		public TableSchema build() {
			return new TableSchema(
				fieldNames.toArray(new String[0]),
				fieldDataTypes.toArray(new DataType[0]),
				watermarkSpecs);
		}
	}
}
