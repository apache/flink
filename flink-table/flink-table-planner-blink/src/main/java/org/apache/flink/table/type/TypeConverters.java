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

import com.google.common.collect.ImmutableBiMap;

import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Converters of {@link InternalType} and {@link TypeInformation}.
 */
public class TypeConverters {

	public static final ImmutableBiMap<InternalType, TypeInformation> INTERNAL_TYPE_TO_EXTERNAL_TYPE_INFO =
			new ImmutableBiMap.Builder<InternalType, TypeInformation>()
					.put(InternalTypes.STRING, BasicTypeInfo.STRING_TYPE_INFO)
					.put(InternalTypes.BOOLEAN, BasicTypeInfo.BOOLEAN_TYPE_INFO)
					.put(InternalTypes.DOUBLE, BasicTypeInfo.DOUBLE_TYPE_INFO)
					.put(InternalTypes.FLOAT, BasicTypeInfo.FLOAT_TYPE_INFO)
					.put(InternalTypes.BYTE, BasicTypeInfo.BYTE_TYPE_INFO)
					.put(InternalTypes.INT, BasicTypeInfo.INT_TYPE_INFO)
					.put(InternalTypes.LONG, BasicTypeInfo.LONG_TYPE_INFO)
					.put(InternalTypes.SHORT, BasicTypeInfo.SHORT_TYPE_INFO)
					.put(InternalTypes.CHAR, BasicTypeInfo.CHAR_TYPE_INFO)
					.put(InternalTypes.BYTE_ARRAY, PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
					.put(InternalTypes.DATE, SqlTimeTypeInfo.DATE)
					.put(InternalTypes.TIMESTAMP, SqlTimeTypeInfo.TIMESTAMP)
					.put(InternalTypes.TIME, SqlTimeTypeInfo.TIME)
					.put(InternalTypes.SYSTEM_DEFAULT_DECIMAL, BasicTypeInfo.BIG_DEC_TYPE_INFO)
					.build();

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

		InternalType type = INTERNAL_TYPE_TO_EXTERNAL_TYPE_INFO.inverse().get(typeInfo);
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
			return InternalTypes.createArrayType(createInternalTypeFromTypeInfo(arrayType.getComponentType()));
		} else if (typeInfo instanceof BasicArrayTypeInfo) {
			BasicArrayTypeInfo arrayType = (BasicArrayTypeInfo) typeInfo;
			return InternalTypes.createArrayType(createInternalTypeFromTypeInfo(arrayType.getComponentInfo()));
		} else if (typeInfo instanceof ObjectArrayTypeInfo) {
			ObjectArrayTypeInfo arrayType = (ObjectArrayTypeInfo) typeInfo;
			return InternalTypes.createArrayType(createInternalTypeFromTypeInfo(arrayType.getComponentInfo()));
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
