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

package org.apache.flink.orc;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import org.apache.orc.TypeDescription;

import java.util.List;

/**
 * A class that provides utility methods for orc file reading.
 */
class OrcUtils {

	/**
	 * Converts an ORC schema to a Flink TypeInformation.
	 *
	 * @param schema The ORC schema.
	 * @return The TypeInformation that corresponds to the ORC schema.
	 */
	static TypeInformation schemaToTypeInfo(TypeDescription schema) {
		switch (schema.getCategory()) {
			case BOOLEAN:
				return BasicTypeInfo.BOOLEAN_TYPE_INFO;
			case BYTE:
				return BasicTypeInfo.BYTE_TYPE_INFO;
			case SHORT:
				return BasicTypeInfo.SHORT_TYPE_INFO;
			case INT:
				return BasicTypeInfo.INT_TYPE_INFO;
			case LONG:
				return BasicTypeInfo.LONG_TYPE_INFO;
			case FLOAT:
				return BasicTypeInfo.FLOAT_TYPE_INFO;
			case DOUBLE:
				return BasicTypeInfo.DOUBLE_TYPE_INFO;
			case DECIMAL:
				return BasicTypeInfo.BIG_DEC_TYPE_INFO;
			case STRING:
			case CHAR:
			case VARCHAR:
				return BasicTypeInfo.STRING_TYPE_INFO;
			case DATE:
				return SqlTimeTypeInfo.DATE;
			case TIMESTAMP:
				return SqlTimeTypeInfo.TIMESTAMP;
			case BINARY:
				return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
			case STRUCT:
				List<TypeDescription> fieldSchemas = schema.getChildren();
				TypeInformation[] fieldTypes = new TypeInformation[fieldSchemas.size()];
				for (int i = 0; i < fieldSchemas.size(); i++) {
					fieldTypes[i] = schemaToTypeInfo(fieldSchemas.get(i));
				}
				String[] fieldNames = schema.getFieldNames().toArray(new String[]{});
				return new RowTypeInfo(fieldTypes, fieldNames);
			case LIST:
				TypeDescription elementSchema = schema.getChildren().get(0);
				TypeInformation<?> elementType = schemaToTypeInfo(elementSchema);
				// arrays of primitive types are handled as object arrays to support null values
				return ObjectArrayTypeInfo.getInfoFor(elementType);
			case MAP:
				TypeDescription keySchema = schema.getChildren().get(0);
				TypeDescription valSchema = schema.getChildren().get(1);
				TypeInformation<?> keyType = schemaToTypeInfo(keySchema);
				TypeInformation<?> valType = schemaToTypeInfo(valSchema);
				return new MapTypeInfo<>(keyType, valType);
			case UNION:
				throw new UnsupportedOperationException("UNION type is not supported yet.");
			default:
				throw new IllegalArgumentException("Unknown type " + schema);
		}
	}
}
