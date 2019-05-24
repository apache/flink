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

package org.apache.flink.batch.connectors.hive;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.io.IOException;

/**
 * Util class for accessing Hive tables.
 */
public class HiveTableUtil {

	private HiveTableUtil() {
	}

	/**
	 * Get Hive {@link ObjectInspector} for a Flink {@link TypeInformation}.
	 */
	public static ObjectInspector getObjectInspector(TypeInformation flinkType) throws IOException {
		return getObjectInspector(toHiveTypeInfo(flinkType));
	}

	// TODO: reuse Hive's TypeInfoUtils?
	private static ObjectInspector getObjectInspector(TypeInfo type) throws IOException {
		switch (type.getCategory()) {

			case PRIMITIVE:
				PrimitiveTypeInfo primitiveType = (PrimitiveTypeInfo) type;
				return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(primitiveType);

			// TODO: support complex types
			default:
				throw new IOException("Unsupported Hive type category " + type.getCategory());
		}
	}

	/**
	 * Converts a Flink {@link TypeInformation} to corresponding Hive {@link TypeInfo}.
	 */
	public static TypeInfo toHiveTypeInfo(TypeInformation flinkType) {
		if (flinkType.equals(BasicTypeInfo.STRING_TYPE_INFO)) {
			return TypeInfoFactory.stringTypeInfo;
		}
		if (flinkType.equals(BasicTypeInfo.SHORT_TYPE_INFO)) {
			return TypeInfoFactory.shortTypeInfo;
		}
		if (flinkType.equals(BasicTypeInfo.INT_TYPE_INFO)) {
			return TypeInfoFactory.intTypeInfo;
		}
		if (flinkType.equals(BasicTypeInfo.LONG_TYPE_INFO)) {
			return TypeInfoFactory.longTypeInfo;
		}
		if (flinkType.equals(BasicTypeInfo.FLOAT_TYPE_INFO)) {
			return TypeInfoFactory.floatTypeInfo;
		}
		if (flinkType.equals(BasicTypeInfo.DOUBLE_TYPE_INFO)) {
			return TypeInfoFactory.doubleTypeInfo;
		}
		if (flinkType.equals(BasicTypeInfo.BOOLEAN_TYPE_INFO)) {
			return TypeInfoFactory.booleanTypeInfo;
		}
		if (flinkType.equals(SqlTimeTypeInfo.TIMESTAMP)) {
			return TypeInfoFactory.timestampTypeInfo;
		}
		if (flinkType.equals(SqlTimeTypeInfo.DATE)) {
			return TypeInfoFactory.dateTypeInfo;
		}
		if (flinkType.equals(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)) {
			return TypeInfoFactory.binaryTypeInfo;
		}
		if (flinkType.equals(BasicTypeInfo.BIG_DEC_TYPE_INFO)) {
			return TypeInfoFactory.decimalTypeInfo;
		}
		// TODO: support complex data types
		throw new IllegalArgumentException("Unsupported type " + flinkType.toString());
	}
}
