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

package org.apache.flink.formats.protobuf;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

import java.util.HashMap;
import java.util.Map;

/**
 * Mappings between proto and flink data types.
 */
public class ProtoFlinkMapping {

	public static final Map<JavaType, TypeInformation<?>> PROTO_TO_FLINK_TYPES = new HashMap<>();
	public static final Map<TypeInformation<?>, JavaType> FLINK_TO_PROTO_TYPES = new HashMap<>();

	static {
		// TODO: handle ENUM
		// TODO: the maps should have beeen immutable, as these are public static variables
		PROTO_TO_FLINK_TYPES.put(JavaType.INT, BasicTypeInfo.INT_TYPE_INFO);
		PROTO_TO_FLINK_TYPES.put(JavaType.LONG, BasicTypeInfo.LONG_TYPE_INFO);
		PROTO_TO_FLINK_TYPES.put(JavaType.DOUBLE, BasicTypeInfo.DOUBLE_TYPE_INFO);
		PROTO_TO_FLINK_TYPES.put(JavaType.FLOAT, BasicTypeInfo.FLOAT_TYPE_INFO);
		PROTO_TO_FLINK_TYPES.put(JavaType.BOOLEAN, BasicTypeInfo.BOOLEAN_TYPE_INFO);
		PROTO_TO_FLINK_TYPES.put(JavaType.STRING, BasicTypeInfo.STRING_TYPE_INFO);
		PROTO_TO_FLINK_TYPES.put(JavaType.BYTE_STRING, BasicTypeInfo.BYTE_TYPE_INFO);

		for (JavaType javaType : PROTO_TO_FLINK_TYPES.keySet()) {
			FLINK_TO_PROTO_TYPES.put(PROTO_TO_FLINK_TYPES.get(javaType), javaType);
		}
	}
}
