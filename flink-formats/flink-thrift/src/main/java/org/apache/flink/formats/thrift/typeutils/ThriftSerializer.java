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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.formats.thrift.ThriftCodeGenerator;

import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;
import org.apache.thrift.protocol.TType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A serializer that serializes types via Thrift.
 */
public class ThriftSerializer<T extends TBase> extends TypeSerializer<T> {
	private static final Logger LOG = LoggerFactory.getLogger(ThriftSerializer.class);

	public Class<T> thriftClass;

	private ThriftCodeGenerator codeGenerator;

	public ThriftSerializer(Class<T> clazz, ThriftCodeGenerator codeGenerator) {
		this.thriftClass = clazz;
		this.codeGenerator = codeGenerator;
	}

	public String getThriftClassName() {
		checkNotNull(thriftClass);
		return thriftClass.getName();
	}

	/**
	 * Always return false for Thrift types.
	 */
	public boolean isImmutableType() {
		return false;
	}

	public TypeSerializer<T> duplicate() {
		return null;
	}

	/**
	 * Creates a new instance of the data type.
	 *
	 * @return A new instance of the data type.
	 */
	public T createInstance() {
		T instance = null;
		try {
			instance = thriftClass.newInstance();
		} catch (Exception e) {

		}
		return instance;
	}


	/**
	 * Creates a deep copy of the given element in a new element.
	 *
	 * @param from The element reuse be copied.
	 * @return A deep copy of the thrift object.
	 */
	public T copy(T from) {
		T newCopy = (T) from.deepCopy();
		return newCopy;
	}

	/**
	 * Creates a copy from the given element. The method makes an attempt to store the copy in the given reuse element, if
	 * the type is mutable. This is, however, not guaranteed.
	 *
	 * @param from The element to be copied.
	 * @param reuse The element to be reused. May or may not be used.
	 * @return A deep copy of the element.
	 */
	public T copy(T from, T reuse) {
		if (from == null) {
			return null;
		}
		// TODO: reuse the object when possible
		return copy(from);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the length of the data type, if it is a fix length data type.
	 *
	 * @return The length of the data type, or <code>-1</code> for variable length data types.
	 */
	public int getLength() {
		return -1;
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Serializes the given record to the given target output view.
	 *
	 * @param record The record to serialize.
	 * @param target The output view to write the serialized data to.
	 * @throws IOException Thrown, if the serialization encountered an I/O related error. Typically raised by the output
	 * view, which may have an underlying I/O channel to which it delegates.
	 */
	public void serialize(T record, DataOutputView target) throws IOException {
		serializeThriftObject(record, record.getClass(), target);
	}

	private void serializeThriftObject(TBase record, Class<?> thriftClazz, DataOutputView outputView)
		throws IOException {
		Map<TFieldIdEnum, FieldMetaData> fieldMetaDataMap;
		fieldMetaDataMap = ThriftUtils.getMetaDataMapField(thriftClazz);
		int numFields = 0;
		for (TFieldIdEnum fieldIdEnum : fieldMetaDataMap.keySet()) {
			Object fieldValue = record.getFieldValue(fieldIdEnum);
			if (fieldValue != null && record.isSet(fieldIdEnum)) {
				numFields++;
			}
		}
		outputView.writeInt(numFields);

		for (Map.Entry<TFieldIdEnum, FieldMetaData> entry : fieldMetaDataMap.entrySet()) {
			TFieldIdEnum fieldIdEnum = entry.getKey();
			FieldMetaData fieldMetaData = entry.getValue();

			Object fieldValue = record.getFieldValue(fieldIdEnum);
			if (fieldValue != null && record.isSet(fieldIdEnum)) {
				outputView.writeShort(fieldIdEnum.getThriftFieldId());

				boolean isBinary = false;
				if (codeGenerator.equals(ThriftCodeGenerator.THRIFT)) {
					isBinary = fieldMetaData.valueMetaData.isBinary();
				} else if (codeGenerator.equals(ThriftCodeGenerator.SCROOGE)) {
					Class<?> fieldClass = ThriftUtils.getFieldType(thriftClazz, fieldIdEnum.getFieldName());
					isBinary = fieldClass.equals(ByteBuffer.class);
				}
				serialize(fieldMetaData.valueMetaData.type, isBinary, fieldValue, outputView);
			}
		}
	}

	private void serializeObject(Object object, DataOutputView outputView) throws IOException {
		Class<?> objectClass = object.getClass();

		if (TBase.class.isAssignableFrom(objectClass)) {
			serializeThriftObject((TBase) object, objectClass, outputView);
		} else {
			serialize(objectClass, object, outputView);
		}
	}

	private void serialize(Class<?> type, Object object, DataOutputView outputView) throws IOException {
		if (type.isPrimitive()) {
			serializePrimitveType(type, object, outputView);
		} else if (List.class.isAssignableFrom(type)) {
			serialize(TType.LIST, false, object, outputView);
		} else if (Map.class.isAssignableFrom(type)) {
			serialize(TType.MAP, false, object, outputView);
		} else if (Set.class.isAssignableFrom(type)) {
			serialize(TType.SET, false, object, outputView);
		} else if (type.equals(String.class)) {
			outputView.writeUTF((String) object);
		} else if (type.equals(Integer.class)) {
			outputView.writeInt((int) object);
		} else if (type.equals(Long.class)) {
			outputView.writeLong((long) object);
		} else if (type.equals(Boolean.class)) {
			outputView.writeBoolean((boolean) object);
		} else if (type.equals(Byte.class)) {
			outputView.writeByte((byte) object);
		} else if (type.equals(Character.class)) {
			outputView.writeChar((char) object);
		} else if (type.equals(Short.class)) {
			outputView.writeShort((short) object);
		} else if (type.equals(Float.class)) {
			outputView.writeFloat((float) object);
		} else if (type.equals(Double.class)) {
			outputView.writeDouble((double) object);
		} else if (type.equals(byte[].class)) {
			outputView.write((byte[]) object);
		} else {
			LOG.error("Unexpected serialize type : {}", type);
			throw new IOException("Unexpected type : " + type);
		}
	}

	private void serialize(byte fieldType, boolean isBinary, Object fieldValue, DataOutputView outputView)
		throws IOException {
		outputView.writeByte(fieldType);
		switch (fieldType) {
			case TType.BOOL: {
				outputView.writeBoolean((boolean) fieldValue);
				break;
			}

			case TType.BYTE: {
				outputView.writeByte((byte) fieldValue);
				break;
			}

			case TType.DOUBLE: {
				outputView.writeDouble((double) fieldValue);
				break;
			}

			case TType.ENUM: {
				TEnum tEnum = (TEnum) fieldValue;
				outputView.writeInt(tEnum.getValue());
				break;
			}

			case TType.I16: {
				outputView.writeShort((short) fieldValue);
				break;
			}

			case TType.I32: {
				outputView.writeInt((int) fieldValue);
				break;
			}

			case TType.I64: {
				outputView.writeLong((long) fieldValue);
				break;
			}

			case TType.LIST: {
				List<?> list = (List<?>) fieldValue;
				outputView.writeInt(list.size());

				for (Object element : list) {
					serializeObject(element, outputView);
				}
				break;
			}

			case TType.MAP: {
				Map<?, ?> map = (Map<?, ?>) fieldValue;
				outputView.writeInt(map.size());
				for (Map.Entry<?, ?> entry : map.entrySet()) {
					serializeObject(entry.getKey(), outputView);
					serializeObject(entry.getValue(), outputView);
				}
				break;
			}

			case TType.SET: {
				Set<?> set = (Set<?>) fieldValue;
				outputView.writeInt(set.size());
				for (Object object : set) {
					serializeObject(object, outputView);
				}
				break;
			}

			case TType.STOP: {
				break;
			}

			case TType.STRING: {
				if (isBinary) {
					outputView.writeBoolean(true); // binary
					outputView.writeInt(((byte[]) fieldValue).length);
					serializeObject(fieldValue, outputView);
				} else {
					outputView.writeBoolean(false); // not binary
					outputView.writeUTF((String) fieldValue);
				}
				break;
			}

			case TType.STRUCT: {
				serializeObject(fieldValue, outputView);
				break;
			}

			case TType.VOID: {
				break;
			}

			default:
		}
	}

	/**
	 * Eight primitive types in Java : boolean , byte , char , short , int , long , float and double .
	 */
	private void serializePrimitveType(Class<?> type, Object value, DataOutputView target) throws IOException {
		if (type.equals(int.class)) {
			target.writeInt((int) value);
		} else if (type.equals(long.class)) {
			target.writeLong((long) value);
		} else if (type.equals(float.class)) {
			target.writeFloat((float) value);
		} else if (type.equals(double.class)) {
			target.writeDouble((double) value);
		} else if (type.equals(boolean.class)) {
			target.writeBoolean((boolean) value);
		} else if (type.equals(byte.class)) {
			target.writeByte((byte) value);
		} else if (type.equals(char.class)) {
			target.writeChar((char) value);
		} else if (type.equals(short.class)) {
			target.writeShort((short) value);
		}
	}


	/**
	 * De-serializes a record from the given source input view.
	 *
	 * @param source The input view from which to read the data.
	 * @return The deserialized element.
	 * @throws IOException Thrown, if the de-serialization encountered an I/O related error. Typically raised by the input
	 * view, which may have an underlying I/O channel from which it reads.
	 */
	public T deserialize(DataInputView source) throws IOException {
		return deserialize(null, source);
	}

	private void deserialize(TBase thriftObject, TFieldIdEnum fieldIdEnum, byte fieldType, DataInputView inputView)
		throws IOException, NoSuchFieldException, ClassNotFoundException {

		switch (fieldType) {
			case TType.BOOL: {
				boolean value = inputView.readBoolean();
				thriftObject.setFieldValue(fieldIdEnum, value);
				break;
			}

			case TType.BYTE: {
				byte value = inputView.readByte();
				thriftObject.setFieldValue(fieldIdEnum, value);
				break;
			}

			case TType.DOUBLE: {
				double value = inputView.readDouble();
				thriftObject.setFieldValue(fieldIdEnum, value);
				break;
			}

			case TType.ENUM: {
				int enumInt = inputView.readInt();
				String fieldName = fieldIdEnum.getFieldName();
				Class<? extends TEnum> enumType =
					(Class<? extends TEnum>) thriftObject.getClass().getField(fieldName).getType();
				TEnum value = ThriftUtils.getTEnumValue(enumType, enumInt);
				thriftObject.setFieldValue(fieldIdEnum, value);
				break;
			}

			case TType.I16: {
				short value = inputView.readShort();
				thriftObject.setFieldValue(fieldIdEnum, value);
				break;
			}

			case TType.I32: {
				int value = inputView.readInt();
				thriftObject.setFieldValue(fieldIdEnum, value);
				break;
			}

			case TType.I64: {
				long value = inputView.readLong();
				thriftObject.setFieldValue(fieldIdEnum, value);
				break;
			}

			case TType.LIST: {
				int listSize = inputView.readInt();
				Field field = ThriftUtils.getFieldByName(thriftClass, fieldIdEnum.getFieldName());
				ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
				Type elementType = parameterizedType.getActualTypeArguments()[0];

				List list = new ArrayList<>();
				for (int i = 0; i < listSize; i++) {
					Object obj = deserializeInternalObject(elementType, inputView);
					list.add(obj);
				}
				Collections.checkedList(list, elementType.getClass());
				thriftObject.setFieldValue(fieldIdEnum, list);
				break;
			}

			case TType.MAP: {
				int mapSize = inputView.readInt();
				Field field = ThriftUtils.getFieldByName(thriftClass, fieldIdEnum.getFieldName());

				ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
				Type keyType = parameterizedType.getActualTypeArguments()[0];
				Type valueType = parameterizedType.getActualTypeArguments()[1];
				Map map = new HashMap<>();

				for (int i = 0; i < mapSize; i++) {
					Object key = deserializeObject(keyType, inputView);
					map.put(key, deserializeInternalObject(valueType, inputView));
				}
				Collections.checkedMap(map, keyType.getClass(), valueType.getClass());
				thriftObject.setFieldValue(fieldIdEnum, map);
				break;
			}

			case TType.SET: {
				int setSize = inputView.readInt();
				Field field = ThriftUtils.getFieldByName(thriftClass, fieldIdEnum.getFieldName());
				ParameterizedType parameterizedType = (ParameterizedType) field.getGenericType();
				Type elementType = parameterizedType.getActualTypeArguments()[0];
				Set set = new HashSet<>();

				for (int i = 0; i < setSize; i++) {
					Object element = deserializeInternalObject(elementType, inputView);
					set.add(element);
				}
				Collections.checkedSet(set, elementType.getClass());
				thriftObject.setFieldValue(fieldIdEnum, set);
				break;
			}

			case TType.STOP: {
				break;
			}

			case TType.STRING: {
				Boolean isBinary = inputView.readBoolean();
				if (isBinary) {
					int arraySize = inputView.readInt();
					byte[] buffer = new byte[arraySize];
					inputView.read(buffer, 0, arraySize);
					if (codeGenerator.equals(ThriftCodeGenerator.SCROOGE)) {
						thriftObject.setFieldValue(fieldIdEnum, ByteBuffer.wrap(buffer));
					} else {
						thriftObject.setFieldValue(fieldIdEnum, buffer);
					}
				} else {
					String value = inputView.readUTF();
					thriftObject.setFieldValue(fieldIdEnum, value);
				}
				break;
			}

			case TType.STRUCT: {
				Field field = ThriftUtils.getFieldByName(thriftClass, fieldIdEnum.getFieldName());
				Type structType = field.getType();
				Class<?> clazz = null;

				try {
					clazz = Class.forName(structType.getTypeName());
				} catch (ClassNotFoundException e) {
					throw new IOException(e);
				}

				TBase obj = deserializeThrift(null, (Class<? extends TBase>) clazz, inputView);
				thriftObject.setFieldValue(fieldIdEnum, obj);
				break;
			}

			case TType.VOID: {
				break;
			}

			default:
		}
	}

	private Object deserializeInternalObject(Type valueType, DataInputView inputView) throws IOException {
		String valueTypeName = valueType.getTypeName();

		List<String> classNames = Utils.getClassAndTypeArguments(valueTypeName);
		String mainClassName = classNames.remove(0);
		switch (classNames.size()) {
			case 0: // simple class definition
				return deserializeObject(valueType, inputView);
			case 1: // generics definition (list or set)
				if (Utils.isAssignableFrom(List.class, mainClassName)) {
					byte listFieldType = inputView.readByte();
					int listSize = inputView.readInt();
					Type elementType = ((ParameterizedType) valueType).getActualTypeArguments()[0];
					List list = new ArrayList<>();
					for (int j = 0; j < listSize; j++) {
						Object element = deserializeObject(elementType, inputView);
						list.add(element);
					}
					Collections.checkedList(list, elementType.getClass());
					return list;
				} else if (Utils.isAssignableFrom(Set.class, mainClassName)) {
					byte setFieldType = inputView.readByte();
					int setSize = inputView.readInt();
					Type elementType = ((ParameterizedType) valueType).getActualTypeArguments()[0];
					Set set = new HashSet();
					for (int j = 0; j < setSize; j++) {
						Object element = deserializeObject(elementType, inputView);
						set.add(element);
					}
					Collections.checkedSet(set, elementType.getClass());
					return set;
				} else {
					throw new IOException("Unexpected generics internal type: " + valueTypeName);
				}
			case 2: // generics definition (map)
				byte mapFieldType = inputView.readByte();
				int map2Size = inputView.readInt();
				Type elementKeyType = ((ParameterizedType) valueType).getActualTypeArguments()[0];
				Type elementValueType = ((ParameterizedType) valueType).getActualTypeArguments()[1];
				Map map2 = new HashMap<>();
				for (int j = 0; j < map2Size; j++) {
					Object key2 = deserializeObject(elementKeyType, inputView);
					Object value2 = deserializeObject(elementValueType, inputView);
					map2.put(key2, value2);
				}
				Collections.checkedMap(map2, elementKeyType.getClass(), elementValueType.getClass());
				return map2;
			default:
				throw new IOException("Unexpected internal type: " + valueTypeName);
		}
	}

	private Object deserializeObject(Type objectType, DataInputView inputView) throws IOException {

		String className = objectType.getTypeName();
		Class<?> clazz = null;

		try {
			clazz = Class.forName(className);
		} catch (ClassNotFoundException e) {
			LOG.error("Failed to find class {}", className, e);
			throw new IOException(e);
		}

		if (TBase.class.isAssignableFrom(clazz)) {
			return deserializeThrift(null, (Class<? extends TBase>) clazz, inputView);
		} else if (objectType.equals(String.class)) {
			return inputView.readUTF();
		} else if (clazz.equals(Integer.class) || clazz.equals(int.class)) {
			return inputView.readInt();
		} else if (clazz.equals(Long.class) || clazz.equals(long.class)) {
			return inputView.readLong();
		} else if (clazz.equals(Boolean.class) || clazz.equals(boolean.class)) {
			return inputView.readBoolean();
		} else if (clazz.equals(byte.class) || clazz.equals(Byte.class)) {
			return inputView.readByte();
		} else if (clazz.equals(char.class) || clazz.equals(Character.class)) {
			return inputView.readChar();
		} else if (clazz.equals(short.class) || clazz.equals(Short.class)) {
			return inputView.readShort();
		} else if (clazz.equals(float.class) || clazz.equals(Float.class)) {
			return inputView.readFloat();
		} else if (clazz.equals(double.class) || clazz.equals(Double.class)) {
			return inputView.readDouble();
		} else {
			LOG.error("Unexpected deserialization type : {}", objectType);
			throw new IOException("Unexpected type : " + objectType);
		}
	}


	/**
	 * De-serializes a record from the given source input view into the given reuse record instance if mutable.
	 *
	 * @param reuse The record instance into which to de-serialize the data.
	 * @param source The input view from which to read the data.
	 * @return The deserialized element.
	 * @throws IOException Thrown, if the de-serialization encountered an I/O related error. Typically raised by the input
	 * view, which may have an underlying I/O channel from which it reads.
	 */
	public T deserialize(T reuse, DataInputView source) throws IOException {
		return (T) deserializeThrift(reuse, thriftClass, source);
	}

	private TBase deserializeThrift(TBase thriftObject, Class<? extends TBase> thriftClazz, DataInputView inputView)
		throws IOException {
		TBase result;
		try {
			result = (thriftObject != null) ? thriftObject : thriftClazz.newInstance();
			int numFields = inputView.readInt();
			for (int i = 0; i < numFields; i++) {
				short fieldId = inputView.readShort();
				TFieldIdEnum fieldIdEnum = result.fieldForId(fieldId);
				byte fieldType = inputView.readByte();
				deserialize(result, fieldIdEnum, fieldType, inputView);
			}
		} catch (InstantiationException | IllegalAccessException | NoSuchFieldException | ClassNotFoundException e) {
			LOG.error("Failed to deserialize {}", thriftObject,  e);
			throw new IOException((e));
		}
		return result;
	}

	/**
	 * Copies exactly one record from the source input view to the target output view. Whether this operation works on
	 * binary data or partially de-serializes the record to determine its length (such as for records of variable length)
	 * is up to the implementer. Binary copies are typically faster. A copy of a record containing two integer numbers (8
	 * bytes total) is most efficiently implemented as {@code target.write(source, 8);}.
	 *
	 * @param source The input view from which to read the record.
	 * @param target The target output view to which to write the record.
	 * @throws IOException Thrown if any of the two views raises an exception.
	 */
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		T record = deserialize(source);
		serialize(record, target);
	}

	public boolean equals(Object obj) {
		return false;
	}

	public int hashCode() {
		return -1;
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshot for checkpoints/savepoints
	// --------------------------------------------------------------------------------------------

	/**
	 * Snapshots the configuration of this TypeSerializer. This method is only relevant if the serializer is used to state
	 * stored in checkpoints/savepoints.
	 *
	 * <p>The snapshot of the TypeSerializer is supposed to contain all information that affects the serialization
	 * format of the serializer. The snapshot serves two purposes: First, to reproduce the serializer when the
	 * checkpoint/savepoint is restored, and second, to check whether the serialization format is compatible with the
	 * serializer used in the restored program.
	 *
	 * <p><b>IMPORTANT:</b> TypeSerializerSnapshots changed after Flink 1.6. Serializers implemented against
	 * Flink versions up to 1.6 should still work, but adjust to new model to enable state evolution and be future-proof.
	 * See the class-level comments, section "Upgrading TypeSerializers to the new TypeSerializerSnapshot model" for
	 * details.
	 *
	 * @return snapshot of the serializer's current configuration (cannot be {@code null}).
	 * @see TypeSerializerSnapshot#resolveSchemaCompatibility(TypeSerializer)
	 */
	public TypeSerializerSnapshot<T> snapshotConfiguration() {
		return null;
	}
}
