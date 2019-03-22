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

package org.apache.flink.table.util;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

/**
 * Utility for TypeInformation.
 */
public class TypeUtils {

	public static Boolean isInternalArrayType(TypeInformation typeInfo) {
		return typeInfo instanceof PrimitiveArrayTypeInfo ||
			typeInfo instanceof BasicArrayTypeInfo ||
			(typeInfo instanceof ObjectArrayTypeInfo && !typeInfo.equals(BYTE_PRIMITIVE_ARRAY_TYPE_INFO));
	}

	public static boolean isInternalCompositeType(TypeInformation typeInfo) {
		return typeInfo instanceof BaseRowTypeInfo ||
			typeInfo instanceof RowTypeInfo ||
			typeInfo instanceof PojoTypeInfo ||
			typeInfo instanceof TupleTypeInfo;
	}

}
