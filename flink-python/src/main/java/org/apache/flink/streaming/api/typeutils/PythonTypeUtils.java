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

package org.apache.flink.streaming.api.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BigIntSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.CharSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.GenericArraySerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.ShortSerializer;
import org.apache.flink.api.common.typeutils.base.array.BooleanPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.CharPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.DoublePrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.FloatPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.IntPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.LongPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.ShortPrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.streaming.api.typeinfo.python.PickledByteArrayTypeInfo;
import org.apache.flink.table.runtime.typeutils.serializers.python.BigDecSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.StringSerializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * A util class for converting the given TypeInformation to other objects.
 */
@Internal
public class PythonTypeUtils {

	/**
	 * Get coder proto according to the given type information.
	 */
	public static class TypeInfoToProtoConverter {

		public static FlinkFnApi.TypeInfo.FieldType getFieldType(TypeInformation typeInformation) {

			if (typeInformation instanceof BasicTypeInfo) {
				return buildBasicTypeProto((BasicTypeInfo) typeInformation);
			}

			if (typeInformation instanceof PrimitiveArrayTypeInfo) {
				return buildPrimitiveArrayTypeProto((PrimitiveArrayTypeInfo) typeInformation);
			}

			if (typeInformation instanceof RowTypeInfo) {
				return buildRowTypeProto((RowTypeInfo) typeInformation);
			}

			if (typeInformation instanceof PickledByteArrayTypeInfo) {
				return buildPickledBytesTypeProto((PickledByteArrayTypeInfo) typeInformation);
			}

			if (typeInformation instanceof TupleTypeInfo) {
				return buildTupleTypeProto((TupleTypeInfo) typeInformation);
			}

			if (typeInformation instanceof BasicArrayTypeInfo) {
				return buildBasicArrayTypeProto((BasicArrayTypeInfo) typeInformation);
			}

			throw new UnsupportedOperationException(
				String.format("The type information: %s is not supported in PyFlink currently.",
					typeInformation.toString()));
		}

		public static FlinkFnApi.TypeInfo toTypeInfoProto(FlinkFnApi.TypeInfo.FieldType fieldType) {
			return FlinkFnApi.TypeInfo.newBuilder().addField(FlinkFnApi.TypeInfo.Field.newBuilder().setType(fieldType).build()).build();
		}

		private static FlinkFnApi.TypeInfo.FieldType buildBasicTypeProto(BasicTypeInfo basicTypeInfo) {

			FlinkFnApi.TypeInfo.TypeName typeName = null;

			if (basicTypeInfo.equals(BasicTypeInfo.BOOLEAN_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.BOOLEAN;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.BYTE_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.BYTE;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.STRING_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.STRING;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.SHORT_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.SHORT;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.INT_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.INT;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.LONG_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.LONG;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.FLOAT_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.FLOAT;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.DOUBLE_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.DOUBLE;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.CHAR_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.CHAR;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.BIG_INT_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.BIG_INT;
			}

			if (basicTypeInfo.equals(BasicTypeInfo.BIG_DEC_TYPE_INFO)) {
				typeName = FlinkFnApi.TypeInfo.TypeName.BIG_DEC;
			}

			if (typeName == null) {
				throw new UnsupportedOperationException(
					String.format("The BasicTypeInfo: %s is not supported in PyFlink currently.",
						basicTypeInfo.toString()));
			}

			return FlinkFnApi.TypeInfo.FieldType.newBuilder()
				.setTypeName(typeName).build();
		}

		private static FlinkFnApi.TypeInfo.FieldType buildPrimitiveArrayTypeProto(
			PrimitiveArrayTypeInfo primitiveArrayTypeInfo) {
			FlinkFnApi.TypeInfo.FieldType elementFieldType = null;
			if (primitiveArrayTypeInfo.equals(PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.BOOLEAN_TYPE_INFO);
			}

			if (primitiveArrayTypeInfo.equals(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.BYTE_TYPE_INFO);
			}

			if (primitiveArrayTypeInfo.equals(PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.SHORT_TYPE_INFO);
			}

			if (primitiveArrayTypeInfo.equals(PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.INT_TYPE_INFO);
			}

			if (primitiveArrayTypeInfo.equals(PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.LONG_TYPE_INFO);
			}

			if (primitiveArrayTypeInfo.equals(PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.FLOAT_TYPE_INFO);
			}

			if (primitiveArrayTypeInfo.equals(PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.DOUBLE_TYPE_INFO);
			}

			if (primitiveArrayTypeInfo.equals(PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.CHAR_TYPE_INFO);
			}

			if (elementFieldType == null) {
				throw new UnsupportedOperationException(
					String.format("The element type of PrimitiveArrayTypeInfo: %s is not supported in PyFlink currently."
						, primitiveArrayTypeInfo.toString()));
			}

			FlinkFnApi.TypeInfo.FieldType.Builder builder = FlinkFnApi.TypeInfo.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.TypeInfo.TypeName.PRIMITIVE_ARRAY);
			builder.setCollectionElementType(elementFieldType);
			return builder.build();
		}

		private static FlinkFnApi.TypeInfo.FieldType buildBasicArrayTypeProto(
			BasicArrayTypeInfo basicArrayTypeInfo) {
			FlinkFnApi.TypeInfo.FieldType elementFieldType = null;
			if (basicArrayTypeInfo.equals(BasicArrayTypeInfo.BOOLEAN_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.BOOLEAN_TYPE_INFO);
			}

			if (basicArrayTypeInfo.equals(BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.BYTE_TYPE_INFO);
			}

			if (basicArrayTypeInfo.equals(BasicArrayTypeInfo.SHORT_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.SHORT_TYPE_INFO);
			}

			if (basicArrayTypeInfo.equals(BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.INT_TYPE_INFO);
			}

			if (basicArrayTypeInfo.equals(BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.LONG_TYPE_INFO);
			}

			if (basicArrayTypeInfo.equals(BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.FLOAT_TYPE_INFO);
			}

			if (basicArrayTypeInfo.equals(BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.DOUBLE_TYPE_INFO);
			}

			if (basicArrayTypeInfo.equals(BasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.CHAR_TYPE_INFO);
			}

			if (basicArrayTypeInfo.equals(BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO)) {
				elementFieldType = buildBasicTypeProto(BasicTypeInfo.STRING_TYPE_INFO);
			}

			if (elementFieldType == null) {
				throw new UnsupportedOperationException(
					String.format("The element type of BasicArrayTypeInfo: %s is not supported in PyFlink currently."
						, basicArrayTypeInfo.toString()));
			}

			FlinkFnApi.TypeInfo.FieldType.Builder builder = FlinkFnApi.TypeInfo.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.TypeInfo.TypeName.BASIC_ARRAY);
			builder.setCollectionElementType(elementFieldType);
			return builder.build();
		}

		private static FlinkFnApi.TypeInfo.FieldType buildRowTypeProto(RowTypeInfo rowTypeInfo) {
			FlinkFnApi.TypeInfo.FieldType.Builder builder =
				FlinkFnApi.TypeInfo.FieldType.newBuilder()
					.setTypeName(FlinkFnApi.TypeInfo.TypeName.ROW);

			FlinkFnApi.TypeInfo.Builder rowTypeInfoBuilder = FlinkFnApi.TypeInfo.newBuilder();

			int arity = rowTypeInfo.getArity();
			for (int index = 0; index < arity; index++) {
				rowTypeInfoBuilder.addField(
					FlinkFnApi.TypeInfo.Field.newBuilder()
						.setName(rowTypeInfo.getFieldNames()[index])
						.setType(TypeInfoToProtoConverter.getFieldType(rowTypeInfo.getTypeAt(index)))
						.build());
			}
			builder.setRowTypeInfo(rowTypeInfoBuilder.build());
			return builder.build();
		}

		private static FlinkFnApi.TypeInfo.FieldType buildPickledBytesTypeProto(PickledByteArrayTypeInfo pickledByteArrayTypeInfo) {
			return FlinkFnApi.TypeInfo.FieldType.newBuilder()
				.setTypeName(FlinkFnApi.TypeInfo.TypeName.PICKLED_BYTES).build();
		}

		private static FlinkFnApi.TypeInfo.FieldType buildTupleTypeProto(TupleTypeInfo tupleTypeInfo) {
			FlinkFnApi.TypeInfo.FieldType.Builder builder =
				FlinkFnApi.TypeInfo.FieldType.newBuilder()
					.setTypeName(FlinkFnApi.TypeInfo.TypeName.TUPLE);

			FlinkFnApi.TypeInfo.Builder tupleTypeInfoBuilder = FlinkFnApi.TypeInfo.newBuilder();

			int arity = tupleTypeInfo.getArity();
			for (int index = 0; index < arity; index++) {
				tupleTypeInfoBuilder.addField(
					FlinkFnApi.TypeInfo.Field.newBuilder()
						.setName(tupleTypeInfo.getFieldNames()[index])
						.setType(TypeInfoToProtoConverter.getFieldType(tupleTypeInfo.getTypeAt(index)))
						.build());
			}
			builder.setTupleTypeInfo(tupleTypeInfoBuilder.build());
			return builder.build();
		}
	}

	/**
	 * Get serializers according to the given typeInformation.
	 */
	public static class TypeInfoToSerializerConverter {
		private static final Map<Class, TypeSerializer> typeInfoToSerialzerMap = new HashMap<>();

		static {
			typeInfoToSerialzerMap.put(BasicTypeInfo.BOOLEAN_TYPE_INFO.getTypeClass(), BooleanSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.INT_TYPE_INFO.getTypeClass(), IntSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.STRING_TYPE_INFO.getTypeClass(), StringSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.SHORT_TYPE_INFO.getTypeClass(), ShortSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.LONG_TYPE_INFO.getTypeClass(), LongSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.FLOAT_TYPE_INFO.getTypeClass(), FloatSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.DOUBLE_TYPE_INFO.getTypeClass(), DoubleSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.CHAR_TYPE_INFO.getTypeClass(), CharSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.BIG_INT_TYPE_INFO.getTypeClass(), BigIntSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.BIG_DEC_TYPE_INFO.getTypeClass(), BigDecSerializer.INSTANCE);
			typeInfoToSerialzerMap.put(BasicTypeInfo.BYTE_TYPE_INFO.getTypeClass(), ByteSerializer.INSTANCE);

			typeInfoToSerialzerMap.put(PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass(),
				BooleanPrimitiveArraySerializer.INSTANCE);
			typeInfoToSerialzerMap.put(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass(),
				BytePrimitiveArraySerializer.INSTANCE);
			typeInfoToSerialzerMap.put(PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass(),
				CharPrimitiveArraySerializer.INSTANCE);
			typeInfoToSerialzerMap.put(PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass(),
				DoublePrimitiveArraySerializer.INSTANCE);
			typeInfoToSerialzerMap.put(PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass(),
				FloatPrimitiveArraySerializer.INSTANCE);
			typeInfoToSerialzerMap.put(PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass(),
				LongPrimitiveArraySerializer.INSTANCE);
			typeInfoToSerialzerMap.put(PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass(),
				ShortPrimitiveArraySerializer.INSTANCE);
			typeInfoToSerialzerMap.put(PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass(),
				IntPrimitiveArraySerializer.INSTANCE);
		}

		@SuppressWarnings("unchecked")
		public static TypeSerializer typeInfoSerializerConverter(TypeInformation typeInformation) {
			TypeSerializer typeSerializer = typeInfoToSerialzerMap.get(typeInformation.getTypeClass());
			if (typeSerializer != null) {
				return typeSerializer;
			} else {

				if (typeInformation instanceof PickledByteArrayTypeInfo) {
					return BytePrimitiveArraySerializer.INSTANCE;
				}

				if (typeInformation instanceof RowTypeInfo) {
					RowTypeInfo rowTypeInfo = (RowTypeInfo) typeInformation;
					TypeSerializer[] fieldTypeSerializers = Arrays.stream(rowTypeInfo.getFieldTypes())
						.map(f -> typeInfoSerializerConverter(f)).toArray(TypeSerializer[]::new);
					return new RowSerializer(fieldTypeSerializers);
				}

				if (typeInformation instanceof TupleTypeInfo) {
					TupleTypeInfo tupleTypeInfo = (TupleTypeInfo) typeInformation;
					TypeInformation[] typeInformations = new TypeInformation[tupleTypeInfo.getArity()];
					for (int idx = 0; idx < tupleTypeInfo.getArity(); idx++) {
						typeInformations[idx] = tupleTypeInfo.getTypeAt(idx);
					}

					TypeSerializer[] fieldTypeSerialzers = Arrays.stream(typeInformations)
						.map(f -> typeInfoSerializerConverter(f)).toArray(TypeSerializer[]::new);
					return new TupleSerializer(Tuple.getTupleClass(tupleTypeInfo.getArity()), fieldTypeSerialzers);
				}

				if (typeInformation instanceof BasicArrayTypeInfo){
					BasicArrayTypeInfo basicArrayTypeInfo = (BasicArrayTypeInfo) typeInformation;
					Class<?> elementClass = basicArrayTypeInfo.getComponentTypeClass();
					TypeSerializer<?> elementTypeSerializer = typeInfoToSerialzerMap.get(elementClass);
					return new GenericArraySerializer(elementClass, elementTypeSerializer);
				}

			}

			throw new UnsupportedOperationException(
				String.format("Could not find type serializer for current type [%s].", typeInformation.toString()));
		}
	}

}
