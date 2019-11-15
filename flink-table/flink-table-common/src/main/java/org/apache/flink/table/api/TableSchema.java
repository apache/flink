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
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.Field;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo;
import static org.apache.flink.table.types.utils.TypeConversions.fromLegacyInfoToDataType;
import static org.apache.flink.table.utils.TableSchemaValidation.validateNameTypeNumberEqual;
import static org.apache.flink.table.utils.TableSchemaValidation.validateSchema;

/**
 * A table schema that represents a table's structure with field names, data types, watermark
 * information and constraint information (e.g. primary key).
 *
 * <p>Concepts about primary key:</p>
 * <ul>
 *     <li>
 *         Primary key can be consist of single or multiple columns (fields).
 *     </li>
 *     <li>
 *         A primary key on source will be simply trusted, we won't validate the constraint.
 *         The primary key information will then be used for query optimization. If a bounded
 *         or unbounded table source defines any primary key, it must contain a unique value
 *         for each row of data. You cannot have two records having the same value of that field(s).
 *         Otherwise, the result of query might be wrong.
 *     </li>
 *     <li>
 *         A primary key on sink is a weak constraint. Currently, we won't validate the constraint,
 *         but we may add some check in the future to validate whether the primary key of the query
 *         matches the primary key of the sink during compile time.
 *     </li>
 * </ul>
 */
@PublicEvolving
public class TableSchema {

	private static final String ATOMIC_TYPE_FIELD_NAME = "f0";

	private final List<TableColumn> columns;

	private final List<WatermarkSpec> watermarkSpecs;

	private final List<String> primaryKey;

	private TableSchema(
			List<TableColumn> columns,
			List<WatermarkSpec> watermarkSpecs,
			List<String> primaryKey) {
		this.columns = Preconditions.checkNotNull(columns);
		this.watermarkSpecs = Preconditions.checkNotNull(watermarkSpecs);
		this.primaryKey = Preconditions.checkNotNull(primaryKey);
	}

	/**
	 * @deprecated Use the {@link Builder} instead.
	 */
	@Deprecated
	public TableSchema(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		DataType[] fieldDataTypes = fromLegacyInfoToDataType(fieldTypes);
		validateNameTypeNumberEqual(fieldNames, fieldDataTypes);
		List<TableColumn> columns = new ArrayList<>();
		for (int i = 0; i < fieldNames.length; i++) {
			columns.add(TableColumn.of(fieldNames[i], fieldDataTypes[i]));
		}
		validateSchema(columns, emptyList(), emptyList());
		this.columns = columns;
		this.watermarkSpecs = emptyList();
		this.primaryKey = emptyList();
	}

	/**
	 * Returns a deep copy of the table schema.
	 */
	public TableSchema copy() {
		return new TableSchema(
			new ArrayList<>(columns),
			new ArrayList<>(watermarkSpecs),
			new ArrayList<>(primaryKey));
	}

	/**
	 * Returns all field data types as an array.
	 */
	public DataType[] getFieldDataTypes() {
		return columns.stream()
			.map(TableColumn::getType)
			.toArray(DataType[]::new);
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
		return fromDataTypeToLegacyInfo(getFieldDataTypes());
	}

	/**
	 * Returns the specified data type for the given field index.
	 *
	 * @param fieldIndex the index of the field
	 */
	public Optional<DataType> getFieldDataType(int fieldIndex) {
		if (fieldIndex < 0 || fieldIndex >= columns.size()) {
			return Optional.empty();
		}
		return Optional.of(columns.get(fieldIndex).getType());
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
		return this.columns.stream()
			.filter(column -> column.getName().equals(fieldName))
			.findFirst().map(TableColumn::getType);
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
		return columns.size();
	}

	/**
	 * Returns all field names as an array.
	 */
	public String[] getFieldNames() {
		return this.columns.stream()
			.map(TableColumn::getName)
			.toArray(String[]::new);
	}

	/**
	 * Returns the specified name for the given field index.
	 *
	 * @param fieldIndex the index of the field
	 */
	public Optional<String> getFieldName(int fieldIndex) {
		if (fieldIndex < 0 || fieldIndex >= columns.size()) {
			return Optional.empty();
		}
		return Optional.of(this.columns.get(fieldIndex).getName());
	}

	/**
	 * Returns the {@link TableColumn} instance for the given field index.
	 *
	 * @param fieldIndex the index of the field
	 */
	public Optional<TableColumn> getTableColumn(int fieldIndex) {
		if (fieldIndex < 0 || fieldIndex >= columns.size()) {
			return Optional.empty();
		}
		return Optional.of(this.columns.get(fieldIndex));
	}

	/**
	 * Returns the {@link TableColumn} instance for the given field name.
	 *
	 * @param fieldName the name of the field
	 */
	public Optional<TableColumn> getTableColumn(String fieldName) {
		return this.columns.stream()
			.filter(column -> column.getName().equals(fieldName))
			.findFirst();
	}

	/**
	 * Returns all the {@link TableColumn}s for this table schema.
	 */
	public List<TableColumn> getTableColumns() {
		return new ArrayList<>(this.columns);
	}

	/**
	 * Converts a table schema into a (nested) data type describing a
	 * {@link DataTypes#ROW(Field...)}.
	 *
	 * <p>Note that the returned row type contains field types for all the columns, including
	 * normal columns and computed columns. Be caution with the computed column data types, because
	 * they are not expected to be included in the row type of TableSource or TableSink.
	 */
	public DataType toRowDataType() {
		final Field[] fields = columns.stream()
			.map(column -> FIELD(column.getName(), column.getType()))
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
		return new ArrayList<>(watermarkSpecs);
	}

	/**
	 * Returns primary key defined on the table. A primary key is consist of single or
	 * multiple field names. The returned list will be empty if no primary key is defined.
	 *
	 * <p>See the {@link TableSchema} class javadoc for more definition about primary key.
	 */
	public List<String> getPrimaryKey() {
		return new ArrayList<>(primaryKey);
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		sb.append("root\n");
		for (TableColumn column : columns) {
			sb.append(" |-- ")
				.append(column.getName())
				.append(": ");
			sb.append(column.getType());
			if (column.getExpr().isPresent()) {
				sb.append(" AS ").append(column.getExpr().get());
			}
			sb.append('\n');
		}
		if (!watermarkSpecs.isEmpty()) {
			for (WatermarkSpec watermark : watermarkSpecs) {
				sb.append(" |-- ").append("WATERMARK FOR ")
					.append(watermark.getRowtimeAttribute()).append(" AS ")
					.append(watermark.getWatermarkExpressionString())
					.append("\n");
			}
		}
		if (!primaryKey.isEmpty()) {
			sb.append(" |-- ").append("PRIMARY KEY (").append(String.join(", ", primaryKey)).append(")\n");
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
		return Objects.equals(columns, schema.columns)
			&& Objects.equals(watermarkSpecs, schema.watermarkSpecs)
			&& Objects.equals(primaryKey, schema.primaryKey);
	}

	@Override
	public int hashCode() {
		return Objects.hash(columns, watermarkSpecs, primaryKey);
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

		private final List<TableColumn> columns;

		private final List<WatermarkSpec> watermarkSpecs;

		private final List<String> primaryKey;

		public Builder() {
			columns = new ArrayList<>();
			watermarkSpecs = new ArrayList<>();
			primaryKey = new ArrayList<>();
		}

		/**
		 * Add a field with name and data type.
		 *
		 * <p>The call order of this method determines the order of fields in the schema.
		 */
		public Builder field(String name, DataType dataType) {
			Preconditions.checkNotNull(name);
			Preconditions.checkNotNull(dataType);
			columns.add(TableColumn.of(name, dataType));
			return this;
		}

		/**
		 * Add a computed field which is generated by the given expression.
		 * This also defines the field name and the data type.
		 *
		 * <p>The call order of this method determines the order of fields in the schema.
		 *
		 * @param name       Field name
		 * @param dataType   Field data type
		 * @param expression Computed column expression, it should be a SQL-style expression whose
		 *                   identifiers should be all quoted and expanded.
		 *
		 *                   It should be expanded because this expression may be persisted
		 *                   then deserialized from the catalog, an expanded identifier would
		 *                   avoid the ambiguity if there are same name UDF referenced from
		 *                   different paths. For example, if there is a UDF named "my_udf" from
		 *                   path "my_catalog.my_database", you could pass in an expression like
		 *                   "`my_catalog`.`my_database`.`my_udf`(`f0`) + 1";
		 *
		 *                   It should be quoted because user could use a reserved keyword as the
		 *                   identifier, and we have no idea if it is quoted when deserialize from
		 *                   the catalog, so we force to use quoted identifier here. But framework
		 *                   will not check whether it is qualified and quoted or not.
		 *
		 */
		public Builder field(String name, DataType dataType, String expression) {
			Preconditions.checkNotNull(name);
			Preconditions.checkNotNull(dataType);
			Preconditions.checkNotNull(expression);
			columns.add(TableColumn.of(name, dataType, expression));
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
			validateNameTypeNumberEqual(names, dataTypes);
			List<TableColumn> columns = IntStream.range(0, names.length)
				.mapToObj(idx -> TableColumn.of(names[idx], dataTypes[idx]))
				.collect(Collectors.toList());
			this.columns.addAll(columns);
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
		 * Add a primary key with the given field names.
		 * There can only be one PRIMARY KEY for a given table.
		 * See the {@link TableSchema} class javadoc for more definition about primary key.
		 */
		public Builder primaryKey(String... fields) {
			Preconditions.checkArgument(
				fields != null && fields.length > 0,
				"The primary key fields shouldn't be null or empty.");
			Preconditions.checkArgument(
				primaryKey.isEmpty(),
				"A primary key " + primaryKey +
					" have been defined, can not define another primary key " +
					Arrays.toString(fields));
			primaryKey.addAll(Arrays.asList(fields));
			return this;
		}

		/**
		 * Returns a {@link TableSchema} instance.
		 */
		public TableSchema build() {
			validateSchema(this.columns, this.watermarkSpecs, this.primaryKey);
			return new TableSchema(columns, watermarkSpecs, this.primaryKey);
		}
	}
}
