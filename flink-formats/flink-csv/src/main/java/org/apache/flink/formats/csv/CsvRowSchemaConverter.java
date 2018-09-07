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

import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Column;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

/**
 * Converting functions that related to {@link CsvSchema}.
 * In {@link CsvSchema}, there are four types(string,number,boolean
 * and array), in order to satisfy various flink types, this class
 * sorts out instances of {@link TypeInformation} and convert them to
 * one of CsvSchema's types.
 */
public final class CsvRowSchemaConverter {

	/**
	 * Types that can be converted to ColumnType.NUMBER.
	 */
	private static final HashSet<TypeInformation<?>> NUMBER_TYPES =
		new HashSet<>(Arrays.asList(Types.LONG, Types.INT, Types.DOUBLE, Types.FLOAT,
			Types.BIG_DEC, Types.BIG_INT));

	/**
	 * Types that can be converted to ColumnType.STRING.
	 */
	private static final HashSet<TypeInformation<?>> STRING_TYPES =
		new HashSet<>(Arrays.asList(Types.STRING, Types.SQL_DATE,
			Types.SQL_TIME, Types.SQL_TIMESTAMP));

	/**
	 * Types that can be converted to ColumnType.BOOLEAN.
	 */
	private static final HashSet<TypeInformation<?>> BOOLEAN_TYPES =
		new HashSet<>(Collections.singletonList(Types.BOOLEAN));

	/**
	 * Convert {@link RowTypeInfo} to {@link CsvSchema}.
	 */
	public static CsvSchema rowTypeToCsvSchema(RowTypeInfo rowType) {
		Builder builder = new CsvSchema.Builder();
		String[] fields = rowType.getFieldNames();
		TypeInformation<?>[] infos = rowType.getFieldTypes();
		for (int i = 0; i < rowType.getArity(); i++) {
			builder.addColumn(new Column(i, fields[i], convertType(infos[i])));
		}
		return builder.build();
	}

	/**
	 * Convert {@link TypeInformation} to {@link CsvSchema.ColumnType}
	 * based on their catogories.
	 */
	private static CsvSchema.ColumnType convertType(TypeInformation<?> info) {
		if (STRING_TYPES.contains(info)) {
			return CsvSchema.ColumnType.STRING;
		} else if (NUMBER_TYPES.contains(info)) {
			return CsvSchema.ColumnType.NUMBER;
		} else if (BOOLEAN_TYPES.contains(info)) {
			return CsvSchema.ColumnType.BOOLEAN;
		} else if (info instanceof ObjectArrayTypeInfo
			|| info instanceof BasicArrayTypeInfo
			|| info instanceof RowTypeInfo) {
			return CsvSchema.ColumnType.ARRAY;
		} else if (info instanceof PrimitiveArrayTypeInfo &&
			((PrimitiveArrayTypeInfo) info).getComponentType() == Types.BYTE) {
			return CsvSchema.ColumnType.STRING;
		} else {
			throw new RuntimeException("Unable to support " + info.toString()
					+ " yet");
		}
	}
}
