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

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.util.Utf8;

import java.util.List;
import java.util.Map;

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
	 * Recursively converts extracted AvroTypeInfo into a RowTypeInfo nested structure with deterministic field order.
	 * Replaces generic Utf8 with basic String type information.
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
		} else if (extracted instanceof GenericTypeInfo<?>) {
			final GenericTypeInfo<?> genericTypeInfo = (GenericTypeInfo<?>) extracted;
			if (genericTypeInfo.getTypeClass() == Utf8.class) {
				return BasicTypeInfo.STRING_TYPE_INFO;
			} else if (genericTypeInfo.getTypeClass() == Map.class) {
				// avro map key is always string
				return Types.MAP(Types.STRING,
					convertPrimitiveType(schema.getValueType().getType()));
			} else if (genericTypeInfo.getTypeClass() == List.class &&
				schema.getType() == Schema.Type.ARRAY) {
				return Types.LIST(convertPrimitiveType(schema.getElementType().getType()));
			}
		}
		return extracted;
	}

	/**
	 * Converts Avro primitive type into corresponding TypeInformation.
	 */
	private static TypeInformation convertPrimitiveType(Schema.Type type){
		TypeInformation ctype = Types.VOID;
		if (type == Schema.Type.STRING) {
			ctype = Types.STRING;
		} else if (type == Schema.Type.INT) {
			ctype = Types.INT;
		} else if (type == Schema.Type.LONG) {
			ctype = Types.LONG;
		} else if (type == Schema.Type.FLOAT) {
			ctype = Types.FLOAT;
		} else if (type == Schema.Type.DOUBLE) {
			ctype = Types.DOUBLE;
		} else if (type == Schema.Type.BOOLEAN) {
			ctype = Types.BOOLEAN;
		}

		return ctype;
	}
}
