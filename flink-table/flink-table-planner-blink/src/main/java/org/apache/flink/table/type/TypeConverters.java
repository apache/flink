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

package org.apache.flink.table.type;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Converters of {@link InternalType} and {@link TypeInformation}.
 */
public class TypeConverters {

	public static final Map<TypeInformation, InternalType> TYPE_INFO_TO_INTERNAL_TYPE;
	static {
		Map<TypeInformation, InternalType> tiToType = new HashMap<>();
		tiToType.put(BasicTypeInfo.STRING_TYPE_INFO, InternalTypes.STRING);
		tiToType.put(BasicTypeInfo.BOOLEAN_TYPE_INFO, InternalTypes.BOOLEAN);
		tiToType.put(BasicTypeInfo.DOUBLE_TYPE_INFO, InternalTypes.DOUBLE);
		tiToType.put(BasicTypeInfo.FLOAT_TYPE_INFO, InternalTypes.FLOAT);
		tiToType.put(BasicTypeInfo.BYTE_TYPE_INFO, InternalTypes.BYTE);
		tiToType.put(BasicTypeInfo.INT_TYPE_INFO, InternalTypes.INT);
		tiToType.put(BasicTypeInfo.LONG_TYPE_INFO, InternalTypes.LONG);
		tiToType.put(BasicTypeInfo.SHORT_TYPE_INFO, InternalTypes.SHORT);
		tiToType.put(BasicTypeInfo.CHAR_TYPE_INFO, InternalTypes.CHAR);
		tiToType.put(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, InternalTypes.BINARY);
		tiToType.put(SqlTimeTypeInfo.DATE, InternalTypes.DATE);
		tiToType.put(SqlTimeTypeInfo.TIMESTAMP, InternalTypes.TIMESTAMP);
		tiToType.put(SqlTimeTypeInfo.TIME, InternalTypes.TIME);

		// BigDecimal have infinity precision and scale, but we converted it into a limited
		// Decimal(38, 18). If the user's BigDecimal is more precision than this, we will
		// throw Exception to remind user to use GenericType in real data conversion.
		tiToType.put(BasicTypeInfo.BIG_DEC_TYPE_INFO, InternalTypes.SYSTEM_DEFAULT_DECIMAL);
		TYPE_INFO_TO_INTERNAL_TYPE = Collections.unmodifiableMap(tiToType);
	}

	/**
	 * Create a {@link InternalType} from a {@link TypeInformation}.
	 *
	 * <p>Note: Information may be lost. For example, after Pojo is converted to InternalType,
	 * we no longer know that it is a Pojo and only think it is a Row.
	 *
	 * <p>Eg:
	 * {@link BasicTypeInfo#STRING_TYPE_INFO} => {@link InternalTypes#STRING}.
	 * {@link BasicTypeInfo#BIG_DEC_TYPE_INFO} => {@link DecimalType}.
	 * {@link RowTypeInfo} => {@link RowType}.
	 * {@link PojoTypeInfo} (CompositeType) => {@link RowType}.
	 * {@link TupleTypeInfo} (CompositeType) => {@link RowType}.
	 */
	public static InternalType createInternalTypeFromTypeInfo(TypeInformation typeInfo) {
		InternalType type = TYPE_INFO_TO_INTERNAL_TYPE.get(typeInfo);
		if (type != null) {
			return type;
		}

		if (typeInfo instanceof CompositeType) {
			CompositeType compositeType = (CompositeType) typeInfo;
			return InternalTypes.createRowType(
					Stream.iterate(0, x -> x + 1).limit(compositeType.getArity())
							.map((Function<Integer, TypeInformation>) compositeType::getTypeAt)
							.map(TypeConverters::createInternalTypeFromTypeInfo)
							.toArray(InternalType[]::new),
					compositeType.getFieldNames()
			);
		} else if (typeInfo instanceof PrimitiveArrayTypeInfo) {
			PrimitiveArrayTypeInfo arrayType = (PrimitiveArrayTypeInfo) typeInfo;
			return InternalTypes.createArrayType(
					createInternalTypeFromTypeInfo(arrayType.getComponentType()));
		} else if (typeInfo instanceof BasicArrayTypeInfo) {
			BasicArrayTypeInfo arrayType = (BasicArrayTypeInfo) typeInfo;
			return InternalTypes.createArrayType(
					createInternalTypeFromTypeInfo(arrayType.getComponentInfo()));
		} else if (typeInfo instanceof ObjectArrayTypeInfo) {
			ObjectArrayTypeInfo arrayType = (ObjectArrayTypeInfo) typeInfo;
			return InternalTypes.createArrayType(
					createInternalTypeFromTypeInfo(arrayType.getComponentInfo()));
		} else if (typeInfo instanceof MapTypeInfo) {
			MapTypeInfo mapType = (MapTypeInfo) typeInfo;
			return InternalTypes.createMapType(
					createInternalTypeFromTypeInfo(mapType.getKeyTypeInfo()),
					createInternalTypeFromTypeInfo(mapType.getValueTypeInfo()));
		} else {
			return InternalTypes.createGenericType(typeInfo);
		}
	}
}
