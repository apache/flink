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
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

import java.util.List;

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
				final LogicalType logicalType = schema.getLogicalType();
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
}
