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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;

import java.util.List;

/**
 * Utilities for Avro record class conversion.
 */
public class AvroRecordClassConverter {

	private AvroRecordClassConverter() {
		// private
	}

	/**
	 * Converts the extracted AvroTypeInfo into a RowTypeInfo nested structure with deterministic field order.
	 * Replaces generic Utf8 with basic String type information.
	 */
	@SuppressWarnings("unchecked")
	public static <T extends SpecificRecordBase> TypeInformation<Row> convert(Class<T> avroClass) {
		final AvroTypeInfo<T> avroTypeInfo = new AvroTypeInfo<>(avroClass);
		// determine schema to retrieve deterministic field order
		final Schema schema = SpecificData.get().getSchema(avroClass);
		return (TypeInformation<Row>) convertType(avroTypeInfo, schema);
	}

	/**
	 * The conversions performed by this function are as follows:
	 * 1. Recursively converts extracted AvroTypeInfo into a RowTypeInfo nested structure with deterministic field order.
	 * 2. Replaces generic Utf8 with basic String type information.
	 * 3. Replaces Avro primitive type with corresponding Flink primitive type.
	 * 4. Replaces Avro map type with a MapTypeInfo and recursively converts its value type.
	 * 5. Replaces Avro array type with a ArrayTypeInfo and recursively converts its element type.
	 */
	private static TypeInformation<?> convertType(TypeInformation<?> extracted, Schema schema) {
		if (schema.getType() == Schema.Type.RECORD) {
			final List<Schema.Field> fields = schema.getFields();
			final AvroTypeInfo<?> avroTypeInfo = (AvroTypeInfo<?>) extracted;

			final TypeInformation<?>[] types = new TypeInformation<?>[fields.size()];
			final String[] names = new String[fields.size()];
			for (int i = 0; i < fields.size(); i++) {
				final Schema.Field field = fields.get(i);
				types[i] = convertType(avroTypeInfo.getTypeAt(field.name()), field.schema());
				names[i] = field.name();
			}
			return new RowTypeInfo(types, names);
		} else if (schema.getType() == Schema.Type.MAP) {
			TypeInformation valueType = Types.VOID;
			if (schema.getValueType().getType() == Schema.Type.RECORD) {
				valueType = createAvroTypeInfo(schema.getValueType().getFullName());
			}
			return Types.MAP(Types.STRING, convertType(valueType, schema.getValueType()));
		} else if (schema.getType() == Schema.Type.ARRAY) {
			TypeInformation elementType = Types.VOID;
			if (schema.getElementType().getType() == Schema.Type.RECORD) {
				elementType = createAvroTypeInfo(schema.getElementType().getFullName());
			}
			return Types.OBJECT_ARRAY(convertType(elementType, schema.getElementType()));
		} else if (schema.getType() == Schema.Type.STRING) {
			return Types.STRING;
		} else if (schema.getType() == Schema.Type.INT) {
			return Types.INT;
		} else if (schema.getType() == Schema.Type.LONG) {
			return Types.LONG;
		} else if (schema.getType() == Schema.Type.FLOAT) {
			return Types.FLOAT;
		} else if (schema.getType() == Schema.Type.DOUBLE) {
			return Types.DOUBLE;
		} else if (schema.getType() == Schema.Type.BOOLEAN) {
			return Types.BOOLEAN;
		} else if (extracted instanceof GenericTypeInfo<?>) {
			final GenericTypeInfo<?> genericTypeInfo = (GenericTypeInfo<?>) extracted;
			if (genericTypeInfo.getTypeClass() == Utf8.class) {
				return BasicTypeInfo.STRING_TYPE_INFO;
			}
		}

		return extracted;
	}

	/**
	 * Creates an AvroTypeInfo from the AvroClass which is derived using Java reflection.
	 */
	@SuppressWarnings("unchecked")
	private static AvroTypeInfo createAvroTypeInfo(String avroClassName) {
		Class avroClass;
		try {
			avroClass = Class.forName(avroClassName);
		} catch (ClassNotFoundException e) {
			throw new FlinkRuntimeException(e);
		}

		return new AvroTypeInfo<>(avroClass);
	}
}
