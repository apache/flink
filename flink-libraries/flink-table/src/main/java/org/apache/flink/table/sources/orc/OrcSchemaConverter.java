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

package org.apache.flink.table.sources.orc;

import org.apache.flink.table.api.types.BooleanType;
import org.apache.flink.table.api.types.ByteArrayType;
import org.apache.flink.table.api.types.ByteType;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DateType;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.DoubleType;
import org.apache.flink.table.api.types.FloatType;
import org.apache.flink.table.api.types.IntType;
import org.apache.flink.table.api.types.LongType;
import org.apache.flink.table.api.types.ShortType;
import org.apache.flink.table.api.types.StringType;
import org.apache.flink.table.api.types.TimestampType;

import org.apache.orc.TypeDescription;

/**
 * A {@link OrcSchemaConverter} is used to convert
 * Flink field name and {@link DataType} pairs
 * to Orc {@link org.apache.orc.TypeDescription}, and vice versa.
 */
public class OrcSchemaConverter {
	public static TypeDescription convert(final DataType[] fieldTypes, final String[] fieldNames) {
		final TypeDescription td = TypeDescription.createStruct();
		for (int i = 0; i < fieldTypes.length; i++) {
			td.addField(fieldNames[i], convertType(fieldTypes[i]));
		}
		return td;
	}

	private static TypeDescription convertType(final DataType fieldType) {
		if (fieldType instanceof BooleanType) {
			return TypeDescription.createBoolean();
		} else if (fieldType instanceof ByteType) {
			return TypeDescription.createByte();
		} else if (fieldType instanceof ShortType) {
			return TypeDescription.createShort();
		} else if (fieldType instanceof IntType) {
			return TypeDescription.createInt();
		} else if (fieldType instanceof LongType) {
			return TypeDescription.createLong();
		} else if (fieldType instanceof FloatType) {
			return TypeDescription.createFloat();
		} else if (fieldType instanceof DoubleType) {
			return TypeDescription.createDouble();
		} else if (fieldType instanceof StringType || fieldType instanceof ByteArrayType) {
			return TypeDescription.createString();
		} else if (fieldType instanceof DateType) {
			return TypeDescription.createDate();
		} else if (fieldType instanceof TimestampType) {
			return TypeDescription.createTimestamp();
		} else if (fieldType instanceof DecimalType) {
			int precision = ((DecimalType) fieldType).precision();
			int scale = ((DecimalType) fieldType).scale();
			return TypeDescription.createDecimal().withPrecision(precision).withScale(scale);
		} else {
			throw new UnsupportedOperationException("Unsupported category: " + fieldType);
		}
	}
}
