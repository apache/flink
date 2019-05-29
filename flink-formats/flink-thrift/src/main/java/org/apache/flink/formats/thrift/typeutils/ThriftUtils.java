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

package org.apache.flink.formats.thrift.typeutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.MultisetTypeInfo;
import org.apache.flink.formats.thrift.ThriftCodeGenerator;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.EnumMetaData;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


/**
 * Thrift related utility functions.
 */
public class ThriftUtils {

	private static final Logger LOG = LoggerFactory.getLogger(ThriftUtils.class);

	public static final String FIELD_METADATA_MAP = "metaDataMap";
	public static final String FIELD_BINARY_FIELD_VALUE_METADATAS = "binaryFieldValueMetaDatas";
	public static final String METHOD_FIND_BY_VALUE = "findByValue";

	public static Class<?> getFieldType(@Nonnull Class<?> thriftClass, String fieldName) {
		try {
			Field field = thriftClass.getField(fieldName);
			return field.getType();
		} catch (NoSuchFieldException e) {
			return null;
		}
	}

	public static Map<TFieldIdEnum, FieldMetaData> getMetaDataMapField(@Nonnull Class<?> thriftClass)
		throws IOException {
		try {
			Field metaDataMapField;
			metaDataMapField = thriftClass.getField(FIELD_METADATA_MAP);
			Map<TFieldIdEnum, FieldMetaData> fieldMetaDataMap =
				(Map<TFieldIdEnum, FieldMetaData>) metaDataMapField.get(thriftClass);
			return fieldMetaDataMap;
		} catch (NoSuchFieldException | IllegalAccessException e) {
			LOG.debug("Failed to get metaDataMap field for {}", thriftClass, e);
			throw new IOException(e);
		}
	}

	public static TFieldIdEnum getFieldIdEnum(@Nonnull Class<? extends TBase> thriftClass, String fieldName)
		throws IOException {
		try {
			Class<?>[] classes = thriftClass.getDeclaredClasses();
			Class<?> fieldsClazz = null;

			for (Class<?> clazz : classes) {
				if (clazz.getSimpleName().equals("_Fields")) {
					fieldsClazz = clazz;
					break;
				}
			}
			Method findByNameMethod = fieldsClazz.getMethod("findByName", String.class);
			TFieldIdEnum result = (TFieldIdEnum) findByNameMethod.invoke(fieldsClazz, fieldName);
			return result;
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	public static TypeInformation getParameterizedTypeInfo(Field field, ThriftCodeGenerator codeGenerator)
		throws ClassNotFoundException {
		ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
		Type elementType = parameterizedType.getActualTypeArguments()[0];
		Class<?> elementClass = Class.forName(elementType.getTypeName());
		TypeInformation typeInfo = TBase.class.isAssignableFrom(elementClass) ?
			new ThriftTypeInfo(elementClass, codeGenerator) : TypeInformation.of(elementClass);
		return typeInfo;
	}

	public static boolean isBinaryField(Class<? extends TBase> typeClass,
		FieldMetaData fieldMetaData, ThriftCodeGenerator codeGenerator) {
		boolean isBinaryField = false;
		if (codeGenerator.equals(ThriftCodeGenerator.THRIFT)) {
			isBinaryField = fieldMetaData.valueMetaData.isBinary();
		} else if (codeGenerator.equals(ThriftCodeGenerator.SCROOGE)) {
			Class<?> fieldClass = ThriftUtils.getFieldType(typeClass, fieldMetaData.fieldName);
			isBinaryField = fieldClass.equals(ByteBuffer.class);
		}
		return isBinaryField;
	}

	public static TypeInformation<?> getFieldTypeInformation(@Nonnull Class<? extends TBase> thriftClass,
		String fieldName, ThriftCodeGenerator codeGenerator)
		throws IOException, NoSuchFieldException, ClassNotFoundException {
		Map<TFieldIdEnum, FieldMetaData> metaDataMap = getMetaDataMapField(thriftClass);
		TFieldIdEnum fieldIdEnum = getFieldIdEnum(thriftClass, fieldName);
		FieldMetaData fieldMetaData = metaDataMap.get(fieldIdEnum);
		Class<?> fieldClass = ThriftUtils.ttypeToClass(fieldMetaData.valueMetaData.type);
		boolean isBinary = isBinaryField(thriftClass, fieldMetaData, codeGenerator);
		TypeInformation<?> typeinfo = null;
		Field field = thriftClass.getField(fieldName);

		switch (fieldMetaData.valueMetaData.type) {
			case TType.BOOL:
			case TType.BYTE:
			case TType.DOUBLE:
			case TType.I16:
			case TType.I32:
			case TType.I64: {
				typeinfo = TypeInformation.of(fieldClass);
				break;
			}
			case TType.STRING: {
				typeinfo = isBinary ? TypeInformation.of(ByteBuffer.class) : TypeInformation.of(fieldClass);
				break;
			}

			case TType.ENUM: {
				typeinfo = TypeInformation.of(TEnum.class);
				break;
			}

			case TType.LIST: {
				TypeInformation typeInfo = ThriftUtils.getParameterizedTypeInfo(field, codeGenerator);
				typeinfo = new ListTypeInfo<>(typeInfo);
				break;
			}

			case TType.MAP: {
				ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
				Type keyType = parameterizedType.getActualTypeArguments()[0];
				Type valueType = parameterizedType.getActualTypeArguments()[1];
				Class<?> keyClass = Class.forName(keyType.getTypeName());
				Class<?> valueClass = Class.forName(valueType.getTypeName());
				TypeInformation keyTypeInfo = TBase.class.isAssignableFrom(keyClass) ?
					new ThriftTypeInfo(keyClass, codeGenerator) : TypeInformation.of(keyClass);
				TypeInformation valueTypeInfo = TBase.class.isAssignableFrom(valueClass) ?
					new ThriftTypeInfo(valueClass, codeGenerator) : TypeInformation.of(valueClass);
				typeinfo = new MapTypeInfo(keyTypeInfo, valueTypeInfo);
				break;
			}

			case TType.SET: {
				TypeInformation fieldTypeInfo = ThriftUtils.getParameterizedTypeInfo(field, codeGenerator);
				typeinfo = new MultisetTypeInfo<>(fieldTypeInfo);
				break;
			}

			case TType.STRUCT: {
				StructMetaData structMetaData = (StructMetaData) fieldMetaData.valueMetaData;
				typeinfo = new ThriftTypeInfo(structMetaData.structClass, codeGenerator);
				break;
			}

			case TType.STOP:
			case TType.VOID:
			default: {
				break;
			}
		}
		return typeinfo;
	}

	public static Set<FieldValueMetaData> getBinaryFieldValueMetaDatas(@Nonnull Class<?> thriftClass)
		throws IOException {
		try {
			Field binaryFieldValueMetaDatasField;
			binaryFieldValueMetaDatasField = thriftClass.getField(FIELD_BINARY_FIELD_VALUE_METADATAS);
			Set<FieldValueMetaData> binaryFieldValueMetaDatas =
				(Set<FieldValueMetaData>) binaryFieldValueMetaDatasField.get(thriftClass);
			return binaryFieldValueMetaDatas;
		} catch (NoSuchFieldException e) {
			return null;
		} catch (IllegalAccessException e) {
			throw new IOException(e);
		}
	}

	public static TEnum getTEnumValue(Class<? extends TEnum> tEnumType, int fieldValue) throws IOException {
		try {
			Method method = tEnumType.getMethod(METHOD_FIND_BY_VALUE, int.class);
			TEnum tEnum = (TEnum) method.invoke(tEnumType, fieldValue);
			return tEnum;
		} catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
			throw new IOException(e);
		}
	}

	public static TEnum getTEnumValue(Class<? extends TEnum> tEnumType, String fieldName) throws IOException {
		try {
			Field field = tEnumType.getField(fieldName);
			TEnum tEnum = (TEnum) field.get(tEnumType);
			return tEnum;
		} catch (NoSuchFieldException | IllegalAccessException e) {
			throw new IOException(e);
		}
	}

	public static Iterable<Field> getFieldsUpTo(@Nonnull Class<?> startClass, @Nullable Class<?> exclusiveParent) {

		List<Field> currentClassFields = Lists.newArrayList(startClass.getDeclaredFields());
		Class<?> parentClass = startClass.getSuperclass();

		if (parentClass != null &&
			(exclusiveParent == null || !(parentClass.equals(exclusiveParent)))) {
			List<Field> parentClassFields =
				(List<Field>) getFieldsUpTo(parentClass, exclusiveParent);
			currentClassFields.addAll(parentClassFields);
		}

		return currentClassFields;
	}

	public static Field getFieldByName(Class<?> clazz, String fieldName) throws IOException {
		Field field;
		try {
			field = clazz.getField(fieldName);
		} catch (NoSuchFieldException e) {
			throw new IOException(e);
		}
		return field;
	}

	public static Class<?> ttypeToClass(byte ttype) {
		switch (ttype) {
			case TType.STOP:
				return null;
			case TType.VOID:
				return void.class;
			case TType.BOOL:
				return boolean.class;
			case TType.BYTE:
				return byte.class;
			case TType.DOUBLE:
				return double.class;
			case TType.I16:
				return short.class;
			case TType.I32:
				return int.class;
			case TType.I64:
				return long.class;
			case TType.STRING:
				return String.class;
			case TType.STRUCT:
				return Object.class;
			case TType.MAP:
				return Map.class;
			case TType.SET:
				return Set.class;
			case TType.LIST:
				return List.class;
			case TType.ENUM:
				return Enum.class;
		}
		return null;
	}

	public static byte classToTType(Class clazz) throws IOException {
		if (clazz == null) {
			return TType.STOP;
		} else if (Utils.isAssignableFrom(Void.class, clazz)) {
			return TType.VOID;
		} else if (Utils.isAssignableFrom(Boolean.class, clazz)) {
			return TType.BOOL;
		} else if (Utils.isAssignableFrom(Byte.class, clazz)) {
			return TType.BYTE;
		} else if (Utils.isAssignableFrom(Double.class, clazz)) {
			return TType.DOUBLE;
		} else if (Utils.isAssignableFrom(Short.class, clazz)) {
			return TType.I16;
		} else if (Utils.isAssignableFrom(Integer.class, clazz)) {
			return TType.I32;
		} else if (Utils.isAssignableFrom(Long.class, clazz)) {
			return TType.I64;
		} else if (Utils.isAssignableFrom(String.class, clazz)) {
			return TType.STRING;
		} else if (Utils.isAssignableFrom(TBase.class, clazz)) {
			return TType.STRUCT;
		} else if (Utils.isAssignableFrom(Map.class, clazz)) {
			return TType.MAP;
		} else if (Utils.isAssignableFrom(Set.class, clazz)) {
			return TType.SET;
		} else if (Utils.isAssignableFrom(List.class, clazz)) {
			return TType.LIST;
		} else if (Utils.isAssignableFrom(Enum.class, clazz)) {
			return TType.ENUM;
		}
		return TType.STOP;
	}

	private static Set<Byte> primitiveThriftTypes = new ImmutableSet.Builder<Byte>()
		.add(TType.BOOL)
		.add(TType.BYTE)
		.add(TType.DOUBLE)
		.add(TType.I16).add(TType.I32).add(TType.I64)
		.build();

	public static TypeInformation<Row> getRowTypeInformation(Class<? extends TBase> thriftClass) {
		return getRowTypeInformation(thriftClass, Optional.empty());
	}

	public static TypeInformation<Row> getRowTypeInformation(
		Class<? extends TBase> thriftClass, Optional<String> rowTimeAttributeName) {
		Map<? extends TFieldIdEnum, FieldMetaData> metaDataMap = FieldMetaData.getStructMetaDataMap(thriftClass);

		String[] fieldNames = new String[metaDataMap.size()];
		TypeInformation<?>[] typeInformations = new TypeInformation<?>[metaDataMap.size()];

		int i = 0;
		for (FieldMetaData fieldMetaData : metaDataMap.values()) {
			boolean isRowTimeAttributeName = false;
			fieldNames[i] = fieldMetaData.fieldName;
			if (rowTimeAttributeName.isPresent() && rowTimeAttributeName.get().equals(fieldMetaData.fieldName)) {
				isRowTimeAttributeName = true;
			}
			typeInformations[i] = getTypeInformation(fieldMetaData.valueMetaData, isRowTimeAttributeName);
			i++;
		}
		return Types.ROW_NAMED(fieldNames, typeInformations);
	}

	private static TypeInformation<?> getTypeInformation(FieldValueMetaData fieldValueMetaData) {
		return getTypeInformation(fieldValueMetaData, false);
	}

	public static TypeInformation<?> getTypeInformation(
		FieldValueMetaData fieldValueMetaData, boolean isRowTimeAttributeName) {
		switch (fieldValueMetaData.type) {
			case TType.STOP:
			case TType.VOID:
				return null;

			case TType.BOOL:
				return Types.BOOLEAN;
			case TType.BYTE:
				return Types.BYTE;
			case TType.DOUBLE:
				return Types.DOUBLE;
			case TType.I16:
				return Types.SHORT;
			case TType.I32:
				return Types.INT;
			case TType.I64:
				return isRowTimeAttributeName ? Types.SQL_TIMESTAMP : Types.LONG;
			case TType.STRING:
				return Types.STRING;

			case TType.STRUCT:
				StructMetaData structMetaData = (StructMetaData) fieldValueMetaData;
				return getRowTypeInformation(structMetaData.structClass);

			case TType.MAP:
				MapMetaData mapMetaData = (MapMetaData) fieldValueMetaData;
				return Types.MAP(
					getTypeInformation(mapMetaData.keyMetaData),
					getTypeInformation(mapMetaData.valueMetaData)
				);

			case TType.SET:
				// Mapping SET to a LIST since Flink SQL type system does not have a SET type.
				// Semantics of reading from a SET is equivalent to reading from a LIST.
				SetMetaData setMetaData = (SetMetaData) fieldValueMetaData;
				return getArrayTypeInformation(setMetaData.elemMetaData);

			case TType.LIST:
				ListMetaData listMetaData = (ListMetaData) fieldValueMetaData;
				return getArrayTypeInformation(listMetaData.elemMetaData);

			case TType.ENUM:
				// Mapping ENUM to a STRING. There is a ENUM type in Flink SQL.
				// To be revisited.
				EnumMetaData enumMetaData = (EnumMetaData) fieldValueMetaData;
				return Types.STRING;

			default:
				return null;
		}
	}

	private static TypeInformation<?> getArrayTypeInformation(FieldValueMetaData fieldValueMetaData) {
		if (isPrimitiveThriftType(fieldValueMetaData.type)) {
			return Types.PRIMITIVE_ARRAY(getTypeInformation(fieldValueMetaData));
		} else {
			return Types.OBJECT_ARRAY(getTypeInformation(fieldValueMetaData));
		}
	}

	private static boolean isPrimitiveThriftType(byte tType) {
		return primitiveThriftTypes.contains(tType);
	}
}
