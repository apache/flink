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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.MultisetTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.formats.thrift.ThriftCodeGenerator;

import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TType;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ThriftInfoInfo is for creating TypeInformation for Thrift classes.
 *
 * @param <T> The Thrift class
 */
public class ThriftTypeInfo<T extends TBase> extends PojoTypeInfo<T> {

	public Class<T> thriftClass;
	private ThriftCodeGenerator codeGenerator;

	public ThriftTypeInfo(Class<T> typeClass, ThriftCodeGenerator codeGenerator) {
		super(typeClass, generateThriftFields(typeClass, codeGenerator));
		this.thriftClass = typeClass;
		this.codeGenerator = codeGenerator;
	}

	@Override
	public boolean isBasicType() {
		return false;
	}

	@PublicEvolving
	public TypeSerializer<T> createSerializer(ExecutionConfig var1) {
		return new ThriftSerializer<>(thriftClass, codeGenerator);
	}

	private static <T extends TBase> List<PojoField> generateThriftFields(
		Class<T> typeClass, ThriftCodeGenerator codeGenerator) {
		List<PojoField> result = new ArrayList<>();
		Map<TFieldIdEnum, FieldMetaData> fieldMetaDataMap;
		Set<FieldValueMetaData> fieldValueMetaDataSet = new HashSet<>();
		try {
			fieldMetaDataMap = ThriftUtils.getMetaDataMapField(typeClass);
			if (codeGenerator.equals(ThriftCodeGenerator.SCROOGE)) {
				fieldValueMetaDataSet = ThriftUtils.getBinaryFieldValueMetaDatas(typeClass);
			}
			for (Map.Entry<TFieldIdEnum, FieldMetaData> entry : fieldMetaDataMap.entrySet()) {
				FieldMetaData fieldMetaData = entry.getValue();
				boolean isBinaryField = false;
				if (codeGenerator.equals(ThriftCodeGenerator.THRIFT)) {
					isBinaryField = fieldMetaData.valueMetaData.isBinary();
				} else if (codeGenerator.equals(ThriftCodeGenerator.SCROOGE)) {
					isBinaryField = fieldValueMetaDataSet.contains(fieldMetaData.valueMetaData);
				}
				PojoField pojoField = generatePojoField(typeClass, fieldMetaData, isBinaryField, codeGenerator);
				result.add(pojoField);
			}
		} catch (IOException | NoSuchFieldException | ClassNotFoundException e) {

		}
		return result;
	}

	private static TypeInformation getParameterizedTypeInfo(Field field, ThriftCodeGenerator codeGenerator)
		throws ClassNotFoundException {
		ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
		Type elementType = parameterizedType.getActualTypeArguments()[0];
		Class<?> elementClass = Class.forName(elementType.getTypeName());
		TypeInformation typeInfo = TBase.class.isAssignableFrom(elementClass) ?
			new ThriftTypeInfo(elementClass, codeGenerator) : TypeInformation.of(elementClass);
		return typeInfo;
	}

	private static PojoField generatePojoField(Class<?> typeClass, FieldMetaData fieldMetaData,
		boolean isBinaryField, ThriftCodeGenerator codeGenerator) throws NoSuchFieldException, ClassNotFoundException {
		PojoField result = null;
		String fieldName = fieldMetaData.fieldName;
		Field field = typeClass.getField(fieldName);
		Class<?> fieldClass = ThriftUtils.ttypeToClass(fieldMetaData.valueMetaData.type);

		switch (fieldMetaData.valueMetaData.type) {
			case TType.BOOL:
			case TType.BYTE:
			case TType.DOUBLE:
			case TType.I16:
			case TType.I32:
			case TType.I64: {
				result = new PojoField(field, TypeInformation.of(fieldClass));
				break;
			}

			case TType.STRING: {
				if (isBinaryField) {
					result = new PojoField(field, TypeInformation.of(ByteBuffer.class));
				} else {
					result = new PojoField(field, TypeInformation.of(fieldClass));
				}
				break;
			}
			case TType.ENUM: {
				result = new PojoField(field, TypeInformation.of(TEnum.class));
				break;
			}

			case TType.LIST: {
				TypeInformation typeInfo = getParameterizedTypeInfo(field, codeGenerator);
				result = new PojoField(field, new ListTypeInfo<>(typeInfo));
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
				MapTypeInfo mapTypeInfo = new MapTypeInfo(keyTypeInfo, valueTypeInfo);
				result = new PojoField(field, mapTypeInfo);
				break;
			}

			case TType.SET: {
				TypeInformation typeInfo = getParameterizedTypeInfo(field, codeGenerator);
				result = new PojoField(field, new MultisetTypeInfo<>(typeInfo));
				break;
			}

			case TType.STRUCT: {
				StructMetaData structMetaData = (StructMetaData) fieldMetaData.valueMetaData;
				ThriftTypeInfo thriftTypeInfo = new ThriftTypeInfo(structMetaData.structClass, codeGenerator);
				result = new PojoField(field, thriftTypeInfo);
				break;
			}

			case TType.STOP:
			case TType.VOID:
			default: {
				break;
			}
		}
		return result;
	}
}
