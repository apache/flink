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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Thrift related utility functions.
 */
public class ThriftUtils {

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
			throw new IOException(e);
		}
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
		if (isPrimitieThriftType(fieldValueMetaData.type)) {
			return Types.PRIMITIVE_ARRAY(getTypeInformation(fieldValueMetaData));
		} else {
			return Types.OBJECT_ARRAY(getTypeInformation(fieldValueMetaData));
		}
	}

	private static boolean isPrimitieThriftType(byte tType) {
		return primitiveThriftTypes.contains(tType);
	}
}
