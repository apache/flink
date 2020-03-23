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

package org.apache.flink.formats.csv;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema.Column;

import java.util.Arrays;
import java.util.HashSet;

/**
 * Converter functions that covert Flink's type information to Jackson's {@link CsvSchema}.
 *
 * <p>In {@link CsvSchema}, there are four types (string, number, boolean, and array). In order
 * to satisfy various Flink types, this class sorts out instances of {@link TypeInformation} that
 * are not supported. It converts supported types to one of CsvSchema's types.
 *
 * <p>Note: Changes in this class need to be kept in sync with the corresponding runtime
 * classes {@link CsvRowDeserializationSchema} and {@link CsvRowSerializationSchema}.
 */
public final class CsvRowSchemaConverter {

	private CsvRowSchemaConverter() {
		// private
	}

	/**
	 * Types that can be converted to ColumnType.NUMBER.
	 *
	 * <p>From Jackson: Value should be a number, but literals "null", "true" and "false" are also
	 * understood, and an empty String is considered null. Values are also trimmed (leading/trailing
	 * white space). Other non-numeric Strings may cause parsing exception.
	 */
	private static final HashSet<TypeInformation<?>> NUMBER_TYPES =
		new HashSet<>(Arrays.asList(Types.BYTE, Types.SHORT, Types.INT, Types.LONG, Types.DOUBLE,
			Types.FLOAT, Types.BIG_DEC, Types.BIG_INT));

	/**
	 * Types that can be converted to ColumnType.STRING.
	 *
	 * <p>From Jackson: Default type if not explicitly defined; no type-inference is performed,
	 * and value is not trimmed.
	 */
	private static final HashSet<TypeInformation<?>> STRING_TYPES =
		new HashSet<>(Arrays.asList(Types.STRING,
			Types.SQL_DATE, Types.SQL_TIME, Types.SQL_TIMESTAMP,
			Types.LOCAL_DATE, Types.LOCAL_TIME, Types.LOCAL_DATE_TIME));

	/**
	 * Types that can be converted to ColumnType.BOOLEAN.
	 *
	 * <p>From Jackson: Value is expected to be a boolean ("true", "false") String, or "null", or
	 * empty String (equivalent to null). Values are trimmed (leading/trailing white space).
	 * Values other than indicated above may result in an exception.
	 */
	private static final HashSet<TypeInformation<?>> BOOLEAN_TYPES =
		new HashSet<>(Arrays.asList(Types.BOOLEAN, Types.VOID));

	/**
	 * Convert {@link RowTypeInfo} to {@link CsvSchema}.
	 */
	public static CsvSchema convert(RowTypeInfo rowType) {
		final Builder builder = new CsvSchema.Builder();
		final String[] fields = rowType.getFieldNames();
		final TypeInformation<?>[] types = rowType.getFieldTypes();
		for (int i = 0; i < rowType.getArity(); i++) {
			builder.addColumn(new Column(i, fields[i], convertType(fields[i], types[i])));
		}
		return builder.build();
	}

	/**
	 * Convert {@link TypeInformation} to {@link CsvSchema.ColumnType} based on Jackson's categories.
	 */
	private static CsvSchema.ColumnType convertType(String fieldName, TypeInformation<?> info) {
		if (STRING_TYPES.contains(info)) {
			return CsvSchema.ColumnType.STRING;
		} else if (NUMBER_TYPES.contains(info)) {
			return CsvSchema.ColumnType.NUMBER;
		} else if (BOOLEAN_TYPES.contains(info)) {
			return CsvSchema.ColumnType.BOOLEAN;
		} else if (info instanceof ObjectArrayTypeInfo) {
			validateNestedField(fieldName, ((ObjectArrayTypeInfo) info).getComponentInfo());
			return CsvSchema.ColumnType.ARRAY;
		} else if (info instanceof BasicArrayTypeInfo) {
			validateNestedField(fieldName, ((BasicArrayTypeInfo) info).getComponentInfo());
			return CsvSchema.ColumnType.ARRAY;
		} else if (info instanceof RowTypeInfo) {
			final TypeInformation<?>[] types = ((RowTypeInfo) info).getFieldTypes();
			for (TypeInformation<?> type : types) {
				validateNestedField(fieldName, type);
			}
			return CsvSchema.ColumnType.ARRAY;
		} else if (info instanceof PrimitiveArrayTypeInfo &&
				((PrimitiveArrayTypeInfo) info).getComponentType() == Types.BYTE) {
			return CsvSchema.ColumnType.STRING;
		} else {
			throw new IllegalArgumentException(
				"Unsupported type information '" + info.toString() + "' for field '" + fieldName + "'.");
		}
	}

	private static void validateNestedField(String fieldName, TypeInformation<?> info) {
		if (!NUMBER_TYPES.contains(info) && !STRING_TYPES.contains(info) && !BOOLEAN_TYPES.contains(info)) {
			throw new IllegalArgumentException(
				"Only simple types are supported in the second level nesting of fields '" +
					fieldName + "' but was: " + info);
		}
	}
}
