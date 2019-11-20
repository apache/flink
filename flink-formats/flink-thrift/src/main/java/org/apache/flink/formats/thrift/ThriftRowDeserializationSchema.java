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

package org.apache.flink.formats.thrift;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.thrift.typeutils.ThriftUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.meta_data.FieldValueMetaData;
import org.apache.thrift.meta_data.ListMetaData;
import org.apache.thrift.meta_data.MapMetaData;
import org.apache.thrift.meta_data.SetMetaData;
import org.apache.thrift.meta_data.StructMetaData;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TType;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 *  Deserialize thrift object into row type.
 */
public class ThriftRowDeserializationSchema implements DeserializationSchema<Row> {

	private Class<? extends TBase> thriftClass;
	private Optional<String> rowTimeAttributeName;

	private transient TDeserializer tDeserializer;

	/** Type information describing the result type. */
	private final TypeInformation<Row> typeInfo;

	public ThriftRowDeserializationSchema(Class<? extends TBase> thriftClass,
		Class<? extends TProtocolFactory> thriftProtocolFactory,
		Optional<String> rowTimeAttributeName) {
		Preconditions.checkNotNull(thriftClass, "Thrift struct class must not be null.");
		this.thriftClass = thriftClass;
		this.rowTimeAttributeName = rowTimeAttributeName;
		typeInfo = ThriftUtils.getRowTypeInformation(thriftClass, rowTimeAttributeName);

		TProtocolFactory protocolFactory = null;
		try {
			protocolFactory = thriftProtocolFactory.newInstance();
		} catch (Exception e) {

		}
		tDeserializer = new TDeserializer(protocolFactory);
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		TBase thriftObject = deserializeThriftClass(message);
		return convertThriftObjectToRow(thriftObject, thriftClass, typeInfo);
	}

	private TBase deserializeThriftClass(byte[] message) throws IOException {
		try {
			TBase thriftObject = thriftClass.newInstance();
			tDeserializer.deserialize(thriftObject, message);
			return thriftObject;
		} catch (InstantiationException | IllegalAccessException | TException e) {
			throw new IOException(e);
		}
	}

	private Row convertThriftObjectToRow(TBase thriftObject, Class<? extends TBase> thriftClass, TypeInformation<?> rowTypeInfo) {
		Map<? extends TFieldIdEnum, FieldMetaData> metaDataMap = FieldMetaData.getStructMetaDataMap(thriftClass);
		final Row row = new Row(metaDataMap.size());

		int i = 0;
		for (Map.Entry<? extends TFieldIdEnum, FieldMetaData> entry: metaDataMap.entrySet()) {
			row.setField(i, getRowField(thriftObject, entry.getKey(), entry.getValue(), ((RowTypeInfo) rowTypeInfo).getTypeAt(i)));
			i++;
		}

		return row;
	}

	private Object getRowField(
		TBase thriftObject, TFieldIdEnum tFieldIdEnum, FieldMetaData fieldMetaData, TypeInformation<?> typeInformation) {
		if (!thriftObject.isSet(tFieldIdEnum)) {
			return null;
		}

		Object thriftFieldValue = thriftObject.getFieldValue(tFieldIdEnum);
		if (thriftFieldValue == null) {
			return null;
		}

		return getRowField(thriftFieldValue, fieldMetaData.valueMetaData, typeInformation);
	}

	private Object getRowField(Object thriftFieldValue, FieldValueMetaData fieldValueMetaData, TypeInformation<?> typeInformation) {
		switch (fieldValueMetaData.type) {
			case TType.STRING:
				// thrift will assign TType.STRING to both binary and string.
				if (thriftFieldValue instanceof String) {
					return thriftFieldValue;
				} else {
					return new String((byte[]) thriftFieldValue);
				}
			case TType.STRUCT:
				StructMetaData structMetaData = (StructMetaData) fieldValueMetaData;

				return convertThriftObjectToRow((TBase) thriftFieldValue, structMetaData.structClass, typeInformation);
			case TType.MAP:
				Map<Object, Object> thriftMap = (Map<Object, Object>) thriftFieldValue;
				Map<Object, Object> flinkMap = new HashMap<Object, Object>();
				for (Map.Entry<Object, Object> entry: thriftMap.entrySet()) {
					MapMetaData mapMetaData = (MapMetaData) fieldValueMetaData;
					MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInformation;
					flinkMap.put(
						getRowField(
							entry.getKey(), mapMetaData.keyMetaData, mapTypeInfo.getKeyTypeInfo()),
						getRowField(
							entry.getValue(), mapMetaData.valueMetaData, mapTypeInfo.getValueTypeInfo())
					);
				}

				return flinkMap;
			case TType.SET:
				// Mapping SET to a LIST since Flink SQL type system does not have a SET type.
				// Semantics of reading from a SET is equivalent to reading from a LIST.
				SetMetaData setMetaData = (SetMetaData) fieldValueMetaData;

				return getArray(thriftFieldValue, typeInformation, setMetaData.elemMetaData);
			case TType.LIST:
				ListMetaData listMetaData = (ListMetaData) fieldValueMetaData;

				return getArray(thriftFieldValue, typeInformation, listMetaData.elemMetaData);
			case TType.ENUM:
				return thriftFieldValue.toString();
			case TType.I64:
				if (typeInformation == Types.SQL_TIMESTAMP) {
					long timestamp = (Long) thriftFieldValue;

					return new Timestamp(timestamp * 1000);
				} else {
					return (Long) thriftFieldValue;
				}
			default:
				return thriftFieldValue;
		}
	}

	private Object getArray(
		Object thriftFieldValue, TypeInformation typeInformation, FieldValueMetaData elemMetaData) {
		List thriftList = (List) thriftFieldValue;
		TypeInformation componentTypeInformation;
		Class componentClass = Object.class;
		if (typeInformation instanceof PrimitiveArrayTypeInfo) {
			PrimitiveArrayTypeInfo primitiveArrayTypeInfo = (PrimitiveArrayTypeInfo) typeInformation;
			componentTypeInformation = primitiveArrayTypeInfo.getComponentType();
			componentClass = primitiveArrayTypeInfo.getComponentClass();
		} else if (typeInformation instanceof BasicArrayTypeInfo) {
			BasicArrayTypeInfo basicArrayTypeInfo = (BasicArrayTypeInfo) typeInformation;
			componentTypeInformation = basicArrayTypeInfo.getComponentInfo();
			componentClass = basicArrayTypeInfo.getComponentTypeClass();
		} else { //ObjectArrayTypeInfo
			ObjectArrayTypeInfo objectArrayTypeInfo = (ObjectArrayTypeInfo) typeInformation;
			componentTypeInformation = objectArrayTypeInfo.getComponentInfo();
		}

		Object[] flinkArray = (Object[]) Array.newInstance(componentClass, thriftList.size());
		int i = 0;
		for (Object thriftListElement: thriftList) {
			flinkArray[i] = getRowField(
				thriftListElement, elemMetaData, componentTypeInformation);
			i++;
		}
		return flinkArray;
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return typeInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final ThriftRowDeserializationSchema that = (ThriftRowDeserializationSchema) o;
		return Objects.equals(thriftClass, that.thriftClass);
	}

	@Override
	public int hashCode() {
		return thriftClass.hashCode();
	}

	private void writeObject(ObjectOutputStream outputStream) throws IOException {
		outputStream.writeObject(thriftClass);
		outputStream.writeObject(rowTimeAttributeName.isPresent() ? rowTimeAttributeName.get() : null);
	}

	public boolean isEndOfStream(Row var1) {
		return false;
	}
}
