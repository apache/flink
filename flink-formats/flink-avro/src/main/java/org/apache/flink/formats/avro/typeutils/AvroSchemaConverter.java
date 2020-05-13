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

package org.apache.flink.formats.avro.typeutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.AvroRowDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowSerializationSchema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaParseException;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

import java.util.List;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasFamily;

/**
 * Converts an Avro schema into Flink's type information. It uses {@link RowTypeInfo} for representing
 * objects and converts Avro types into types that are compatible with Flink's Table & SQL API.
 *
 * <p>Note: Changes in this class need to be kept in sync with the corresponding runtime
 * classes {@link AvroRowDeserializationSchema} and {@link AvroRowSerializationSchema}.
 */
public class AvroSchemaConverter {

	private AvroSchemaConverter() {
		// private
	}

	/**
	 * Converts an Avro class into a nested row structure with deterministic field order and data
	 * types that are compatible with Flink's Table & SQL API.
	 *
	 * @param avroClass Avro specific record that contains schema information
	 * @return type information matching the schema
	 */
	@SuppressWarnings("unchecked")
	public static <T extends SpecificRecord> TypeInformation<Row> convertToTypeInfo(Class<T> avroClass) {
		Preconditions.checkNotNull(avroClass, "Avro specific record class must not be null.");
		// determine schema to retrieve deterministic field order
		final Schema schema = SpecificData.get().getSchema(avroClass);
		return (TypeInformation<Row>) convertToTypeInfo(schema);
	}

	/**
	 * Converts an Avro schema string into a nested row structure with deterministic field order and data
	 * types that are compatible with Flink's Table & SQL API.
	 *
	 * @param avroSchemaString Avro schema definition string
	 * @return type information matching the schema
	 */
	@SuppressWarnings("unchecked")
	public static <T> TypeInformation<T> convertToTypeInfo(String avroSchemaString) {
		Preconditions.checkNotNull(avroSchemaString, "Avro schema must not be null.");
		final Schema schema;
		try {
			schema = new Schema.Parser().parse(avroSchemaString);
		} catch (SchemaParseException e) {
			throw new IllegalArgumentException("Could not parse Avro schema string.", e);
		}
		return (TypeInformation<T>) convertToTypeInfo(schema);
	}

	private static TypeInformation<?> convertToTypeInfo(Schema schema) {
		switch (schema.getType()) {
			case RECORD:
				final List<Schema.Field> fields = schema.getFields();

				final TypeInformation<?>[] types = new TypeInformation<?>[fields.size()];
				final String[] names = new String[fields.size()];
				for (int i = 0; i < fields.size(); i++) {
					final Schema.Field field = fields.get(i);
					types[i] = convertToTypeInfo(field.schema());
					names[i] = field.name();
				}
				return Types.ROW_NAMED(names, types);
			case ENUM:
				return Types.STRING;
			case ARRAY:
				// result type might either be ObjectArrayTypeInfo or BasicArrayTypeInfo for Strings
				return Types.OBJECT_ARRAY(convertToTypeInfo(schema.getElementType()));
			case MAP:
				return Types.MAP(Types.STRING, convertToTypeInfo(schema.getValueType()));
			case UNION:
				final Schema actualSchema;
				if (schema.getTypes().size() == 2 && schema.getTypes().get(0).getType() == Schema.Type.NULL) {
					actualSchema = schema.getTypes().get(1);
				} else if (schema.getTypes().size() == 2 && schema.getTypes().get(1).getType() == Schema.Type.NULL) {
					actualSchema = schema.getTypes().get(0);
				} else if (schema.getTypes().size() == 1) {
					actualSchema = schema.getTypes().get(0);
				} else {
					// use Kryo for serialization
					return Types.GENERIC(Object.class);
				}
				return convertToTypeInfo(actualSchema);
			case FIXED:
				// logical decimal type
				if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
					return Types.BIG_DEC;
				}
				// convert fixed size binary data to primitive byte arrays
				return Types.PRIMITIVE_ARRAY(Types.BYTE);
			case STRING:
				// convert Avro's Utf8/CharSequence to String
				return Types.STRING;
			case BYTES:
				// logical decimal type
				if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
					return Types.BIG_DEC;
				}
				return Types.PRIMITIVE_ARRAY(Types.BYTE);
			case INT:
				// logical date and time type
				final org.apache.avro.LogicalType logicalType = schema.getLogicalType();
				if (logicalType == LogicalTypes.date()) {
					return Types.SQL_DATE;
				} else if (logicalType == LogicalTypes.timeMillis()) {
					return Types.SQL_TIME;
				}
				return Types.INT;
			case LONG:
				// logical timestamp type
				if (schema.getLogicalType() == LogicalTypes.timestampMillis()) {
					return Types.SQL_TIMESTAMP;
				}
				return Types.LONG;
			case FLOAT:
				return Types.FLOAT;
			case DOUBLE:
				return Types.DOUBLE;
			case BOOLEAN:
				return Types.BOOLEAN;
			case NULL:
				return Types.VOID;
		}
		throw new IllegalArgumentException("Unsupported Avro type '" + schema.getType() + "'.");
	}

	/**
	 * Converts an Avro class into a nested row structure with deterministic field order and data
	 * types that are compatible with Flink's Table & SQL API.
	 *
	 * @param avroClass Avro specific record that contains schema information
	 * @return logical type matching the schema
	 */
	public static <T extends SpecificRecord> LogicalType convertToLogicalType(Class<T> avroClass) {
		Preconditions.checkNotNull(avroClass, "Avro specific record class must not be null.");
		// determine schema to retrieve deterministic field order
		final Schema schema = SpecificData.get().getSchema(avroClass);
		return convertToLogicalType(schema);
	}

	/**
	 * Converts an Avro schema string into a nested row structure with deterministic
	 * field order and data types that are compatible with Flink's Table & SQL API.
	 *
	 * @param avroSchemaString Avro schema definition string
	 * @return logical type matching the schema
	 */
	public static LogicalType convertToLogicalType(String avroSchemaString) {
		Preconditions.checkNotNull(avroSchemaString, "Avro schema must not be null.");
		final Schema schema;
		try {
			schema = new Schema.Parser().parse(avroSchemaString);
		} catch (SchemaParseException e) {
			throw new IllegalArgumentException("Could not parse Avro schema string.", e);
		}
		return convertToLogicalType(schema);
	}

	/**
	 * Converts Flink SQL {@link LogicalType} (can be nested) into an Avro schema.
	 *
	 * @param logicalType logical type
	 * @return Avro's {@link Schema} matching this logical type.
	 */
	public static Schema convertToSchema(LogicalType logicalType) {
		return convertToSchema(logicalType, 0);
	}

	private static LogicalType convertToLogicalType(Schema schema) {
		return convertToDataType(schema).getLogicalType();
	}

	private static DataType convertToDataType(Schema schema) {
		switch (schema.getType()) {
			case RECORD:
				final List<Schema.Field> fields = schema.getFields();
				final DataTypes.Field[] dataTypeFields = new DataTypes.Field[fields.size()];
				for (int i = 0; i < fields.size(); i++) {
					final Schema.Field field = fields.get(i);
					dataTypeFields[i] = DataTypes.FIELD(
						field.name(),
						convertToDataType(field.schema()));
				}
				return DataTypes.ROW(dataTypeFields);
			case ENUM:
			case STRING:
				// convert Avro's Utf8/CharSequence to String
				return DataTypes.STRING();
			case ARRAY:
				// result type might either be ObjectArrayTypeInfo or BasicArrayTypeInfo for Strings
				return DataTypes.ARRAY(convertToDataType(schema.getElementType()));
			case MAP:
				return DataTypes.MAP(DataTypes.STRING(), convertToDataType(schema.getValueType()));
			case UNION:
				final Schema actualSchema;
				if (schema.getTypes().size() == 2 && schema.getTypes().get(0).getType() == Schema.Type.NULL) {
					actualSchema = schema.getTypes().get(1);
				} else if (schema.getTypes().size() == 2 && schema.getTypes().get(1).getType() == Schema.Type.NULL) {
					actualSchema = schema.getTypes().get(0);
				} else if (schema.getTypes().size() == 1) {
					actualSchema = schema.getTypes().get(0);
				} else {
					// use Kryo for serialization
					return DataTypes.RAW(Types.GENERIC(Object.class));
				}
				return convertToDataType(actualSchema);
			case FIXED:
			case BYTES:
				// logical decimal type
				if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
					LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) schema.getLogicalType();
					return DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale());
				}
				// convert fixed size binary data to primitive byte arrays
				return DataTypes.BYTES();
			case INT:
				// logical date and time type
				final org.apache.avro.LogicalType logicalType = schema.getLogicalType();
				if (logicalType == LogicalTypes.date()) {
					return DataTypes.DATE();
				} else if (logicalType == LogicalTypes.timeMillis()) {
					return DataTypes.TIME(3);
				}
				return DataTypes.INT();
			case LONG:
				// logical timestamp type
				if (schema.getLogicalType() == LogicalTypes.timestampMillis()) {
					return DataTypes.TIMESTAMP(3);
				}
				return DataTypes.BIGINT();
			case FLOAT:
				return DataTypes.FLOAT();
			case DOUBLE:
				return DataTypes.DOUBLE();
			case BOOLEAN:
				return DataTypes.BOOLEAN();
			case NULL:
				return DataTypes.NULL();
		}
		throw new IllegalArgumentException("Unsupported Avro type '" + schema.getType() + "'.");
	}

	private static Schema convertToSchema(LogicalType logicalType, int rowTypeCounter) {
		switch (logicalType.getTypeRoot()) {
			case NULL:
				return SchemaBuilder.builder().nullType();
			case BOOLEAN:
				return SchemaBuilder.builder().nullable().booleanType();
			case INTEGER:
				return SchemaBuilder.builder().nullable().intType();
			case BIGINT:
				return SchemaBuilder.builder().nullable().longType();
			case FLOAT:
				return SchemaBuilder.builder().nullable().floatType();
			case DOUBLE:
				return SchemaBuilder.builder().nullable().doubleType();
			case CHAR:
			case VARCHAR:
				return SchemaBuilder.builder().nullable().stringType();
			case BINARY:
			case VARBINARY:
				return SchemaBuilder.builder().nullable().bytesType();
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				// use long to represents Timestamp
				return LogicalTypes.timestampMillis().addToSchema(SchemaBuilder.builder().longType());
			case DATE:
				// use int to represents Date
				return LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
			case TIME_WITHOUT_TIME_ZONE:
				// use int to represents Time, we only support millisecond when deserialization
				return LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType());
			case DECIMAL:
				DecimalType decimalType = (DecimalType) logicalType;
				// store BigDecimal as byte[]
				return LogicalTypes
					.decimal(decimalType.getPrecision(), decimalType.getScale())
					.addToSchema(SchemaBuilder.builder().bytesType());
			case ROW:
				RowType rowType = (RowType) logicalType;
				List<String> fieldNames = rowType.getFieldNames();
				// we have to make sure the record name is different in a Schema
				SchemaBuilder.FieldAssembler<Schema> builder = SchemaBuilder
					.builder()
					.record("row_" + rowTypeCounter)
					.fields();
				rowTypeCounter++;
				for (int i = 0; i < rowType.getFieldCount(); i++) {
					builder = builder
						.name(fieldNames.get(i))
						.type(convertToSchema(rowType.getTypeAt(i), rowTypeCounter))
						.noDefault();
				}
				return builder.endRecord();
			case MAP:
				MapType mapType = (MapType) logicalType;
				if (!hasFamily(mapType.getKeyType(), LogicalTypeFamily.CHARACTER_STRING)) {
					throw new IllegalArgumentException(
						"Avro assumes map keys are strings, " + mapType.getKeyType() +
							" type as key type is not supported.");
				}
				return SchemaBuilder
					.builder()
					.nullable() // will be UNION of Array and null
					.map()
					.values(convertToSchema(mapType.getValueType(), rowTypeCounter));
			case ARRAY:
				ArrayType arrayType = (ArrayType) logicalType;
				return SchemaBuilder
					.builder()
					.nullable() // wil be union of array and null
					.array()
					.items(convertToSchema(arrayType.getElementType(), rowTypeCounter));
			case RAW:
				// if the union type has more than 2 types, it will be recognized a generic type
				// see AvroRowDeserializationSchema#convertAvroType and AvroRowSerializationSchema#convertFlinkType
				return SchemaBuilder.builder().unionOf()
					.nullType().and()
					.booleanType().and()
					.longType().and()
					.doubleType()
					.endUnion();
			default:
				throw new UnsupportedOperationException("Unsupport to derive Schema for type: " + logicalType);
		}
	}
}
