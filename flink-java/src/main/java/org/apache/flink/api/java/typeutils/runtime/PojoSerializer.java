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

package org.apache.flink.api.java.typeutils.runtime;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;


public final class PojoSerializer<T> extends TypeSerializer<T> {

	// Flags for the header
	private static byte IS_NULL = 1;
	private static byte NO_SUBCLASS = 2;
	private static byte IS_SUBCLASS = 4;
	private static byte IS_TAGGED_SUBCLASS = 8;

	private static final long serialVersionUID = 1L;

	private final Class<T> clazz;

	private TypeSerializer<Object>[] fieldSerializers;

	// We need to handle these ourselves in writeObject()/readObject()
	private transient Field[] fields;

	private int numFields;

	private transient Map<Class<?>, TypeSerializer<?>> subclassSerializerCache;
	private transient ClassLoader cl;

	private Map<Class<?>, Integer> registeredClasses;

	private TypeSerializer<?>[] registeredSerializers;

	private final ExecutionConfig executionConfig;

	@SuppressWarnings("unchecked")
	public PojoSerializer(
			Class<T> clazz,
			TypeSerializer<?>[] fieldSerializers,
			Field[] fields,
			ExecutionConfig executionConfig) {
		this.clazz = clazz;
		this.fieldSerializers = (TypeSerializer<Object>[]) fieldSerializers;
		this.fields = fields;
		this.numFields = fieldSerializers.length;
		this.executionConfig = executionConfig;

		LinkedHashSet<Class<?>> registeredPojoTypes = executionConfig.getRegisteredPojoTypes();

		for (int i = 0; i < numFields; i++) {
			this.fields[i].setAccessible(true);
		}

		cl = Thread.currentThread().getContextClassLoader();

		subclassSerializerCache = new HashMap<Class<?>, TypeSerializer<?>>();

		// We only want those classes that are not our own class and are actually sub-classes.
		List<Class<?>> cleanedTaggedClasses = new ArrayList<Class<?>>(registeredPojoTypes.size());
		for (Class<?> registeredClass: registeredPojoTypes) {
			if (registeredClass.equals(clazz)) {
				continue;
			}
			if (!clazz.isAssignableFrom(registeredClass)) {
				continue;
			}
			cleanedTaggedClasses.add(registeredClass);

		}
		this.registeredClasses = new LinkedHashMap<Class<?>, Integer>(cleanedTaggedClasses.size());
		registeredSerializers = new TypeSerializer[cleanedTaggedClasses.size()];

		int id = 0;
		for (Class<?> registeredClass: cleanedTaggedClasses) {
			this.registeredClasses.put(registeredClass, id);
			TypeInformation<?> typeInfo = TypeExtractor.createTypeInfo(registeredClass);
			registeredSerializers[id] = typeInfo.createSerializer(executionConfig);

			id++;
		}
	}

	private void writeObject(ObjectOutputStream out)
			throws IOException, ClassNotFoundException {
		out.defaultWriteObject();
		out.writeInt(fields.length);
		for (Field field: fields) {
			out.writeObject(field.getDeclaringClass());
			out.writeUTF(field.getName());
		}
	}

	private void readObject(ObjectInputStream in)
			throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		int numFields = in.readInt();
		fields = new Field[numFields];
		for (int i = 0; i < numFields; i++) {
			Class<?> clazz = (Class<?>)in.readObject();
			String fieldName = in.readUTF();
			fields[i] = null;
			// try superclasses as well
			while (clazz != null) {
				try {
					fields[i] = clazz.getDeclaredField(fieldName);
					fields[i].setAccessible(true);
					break;
				} catch (NoSuchFieldException e) {
					clazz = clazz.getSuperclass();
				}
			}
			if (fields[i] == null) {
				throw new RuntimeException("Class resolved at TaskManager is not compatible with class read during Plan setup."
						+ " (" + fieldName + ")");
			}
		}

		cl = Thread.currentThread().getContextClassLoader();
		subclassSerializerCache = new HashMap<Class<?>, TypeSerializer<?>>();
	}

	private TypeSerializer<?> getSubclassSerializer(Class<?> subclass) {
		TypeSerializer<?> result = subclassSerializerCache.get(subclass);
		if (result == null) {

			TypeInformation<?> typeInfo = TypeExtractor.createTypeInfo(subclass);
			result = typeInfo.createSerializer(executionConfig);
			if (result instanceof PojoSerializer) {
				PojoSerializer<?> subclassSerializer = (PojoSerializer<?>) result;
				subclassSerializer.copyBaseFieldOrder(this);
			}
			subclassSerializerCache.put(subclass, result);

		}
		return result;
	}

	@SuppressWarnings("unused")
	private boolean hasField(Field f) {
		for (Field field: fields) {
			if (f.equals(field)) {
				return true;
			}
		}
		return false;
	}

	private void copyBaseFieldOrder(PojoSerializer<?> baseSerializer) {
		// do nothing for now, but in the future, adapt subclass serializer to have same
		// ordering as base class serializer so that binary comparison on base class fields
		// can work
	}
	
	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public PojoSerializer<T> duplicate() {
		boolean stateful = false;
		TypeSerializer<?>[] duplicateFieldSerializers = new TypeSerializer[fieldSerializers.length];

		for (int i = 0; i < fieldSerializers.length; i++) {
			duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
			if (duplicateFieldSerializers[i] != fieldSerializers[i]) {
				// at least one of them is stateful
				stateful = true;
			}
		}

		if (stateful) {
			return new PojoSerializer<T>(clazz, duplicateFieldSerializers, fields, executionConfig);
		} else {
			return this;
		}
	}

	
	@Override
	public T createInstance() {
		if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
			return null;
		}
		try {
			T t = clazz.newInstance();
			initializeFields(t);
			return t;
		}
		catch (Exception e) {
			throw new RuntimeException("Cannot instantiate class.", e);
		}
	}

	protected void initializeFields(T t) {
		for (int i = 0; i < numFields; i++) {
			try {
				fields[i].set(t, fieldSerializers[i].createInstance());
			} catch (IllegalAccessException e) {
				throw new RuntimeException("Cannot initialize fields.", e);
			}
		}
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public T copy(T from) {
		if (from == null) {
			return null;
		}

		Class<?> actualType = from.getClass();
		if (actualType == clazz) {
			T target;
			try {
				target = (T) from.getClass().newInstance();
			}
			catch (Throwable t) {
				throw new RuntimeException("Cannot instantiate class.", t);
			}
			// no subclass
			try {
				for (int i = 0; i < numFields; i++) {
					Object value = fields[i].get(from);
					if (value != null) {
						Object copy = fieldSerializers[i].copy(value);
						fields[i].set(target, copy);
					}
					else {
						fields[i].set(target, null);
					}
				}
			} catch (IllegalAccessException e) {
				throw new RuntimeException("Error during POJO copy, this should not happen since we check the fields before.");

			}
			return target;
		} else {
			// subclass
			TypeSerializer subclassSerializer = getSubclassSerializer(actualType);
			return (T) subclassSerializer.copy(from);
		}
	}
	
	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public T copy(T from, T reuse) {
		if (from == null) {
			return null;
		}

		Class<?> actualType = from.getClass();
		if (reuse == null || actualType != reuse.getClass()) {
			// cannot reuse, do a non-reuse copy
			return copy(from);
		}

		if (actualType == clazz) {
			try {
				for (int i = 0; i < numFields; i++) {
					Object value = fields[i].get(from);
					if (value != null) {
						Object reuseValue = fields[i].get(reuse);
						Object copy;
						if(reuseValue != null) {
							copy = fieldSerializers[i].copy(value, reuseValue);
						}
						else {
							copy = fieldSerializers[i].copy(value);
						}
						fields[i].set(reuse, copy);
					}
					else {
						fields[i].set(reuse, null);
					}
				}
			} catch (IllegalAccessException e) {
				throw new RuntimeException("Error during POJO copy, this should not happen since we check the fields" + "before.");
			}
		} else {
			TypeSerializer subclassSerializer = getSubclassSerializer(actualType);
			reuse = (T) subclassSerializer.copy(from, reuse);
		}

		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}


	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void serialize(T value, DataOutputView target) throws IOException {
		int flags = 0;
		// handle null values
		if (value == null) {
			flags |= IS_NULL;
			target.writeByte(flags);
			return;
		}

		Integer subclassTag = -1;
		Class<?> actualClass = value.getClass();
		TypeSerializer subclassSerializer = null;
		if (clazz != actualClass) {
			subclassTag = registeredClasses.get(actualClass);
			if (subclassTag != null) {
				flags |= IS_TAGGED_SUBCLASS;
				subclassSerializer = registeredSerializers[subclassTag];
			} else {
				flags |= IS_SUBCLASS;
				subclassSerializer = getSubclassSerializer(actualClass);
			}
		} else {
			flags |= NO_SUBCLASS;
		}

		target.writeByte(flags);

		if ((flags & IS_SUBCLASS) != 0) {
			target.writeUTF(actualClass.getName());
		} else if ((flags & IS_TAGGED_SUBCLASS) != 0) {
			target.writeByte(subclassTag);
		}


		if ((flags & NO_SUBCLASS) != 0) {
			try {
				for (int i = 0; i < numFields; i++) {
					Object o = fields[i].get(value);
					if (o == null) {
						target.writeBoolean(true); // null field handling
					} else {
						target.writeBoolean(false);
						fieldSerializers[i].serialize(o, target);
					}
				}
			} catch (IllegalAccessException e) {
				throw new RuntimeException("Error during POJO copy, this should not happen since we check the fields" + "before.");

			}
		} else {
			// subclass
			if (subclassSerializer != null) {
				subclassSerializer.serialize(value, target);
			}
		}
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public T deserialize(DataInputView source) throws IOException {
		int flags = source.readByte();
		if((flags & IS_NULL) != 0) {
			return null;
		}

		T target;

		Class<?> actualSubclass = null;
		TypeSerializer subclassSerializer = null;

		if ((flags & IS_SUBCLASS) != 0) {
			String subclassName = source.readUTF();
			try {
				actualSubclass = Class.forName(subclassName, true, cl);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("Cannot instantiate class.", e);
			}
			subclassSerializer = getSubclassSerializer(actualSubclass);
			target = (T) subclassSerializer.createInstance();
			// also initialize fields for which the subclass serializer is not responsible
			initializeFields(target);
		} else if ((flags & IS_TAGGED_SUBCLASS) != 0) {

			int subclassTag = source.readByte();
			subclassSerializer = registeredSerializers[subclassTag];
			target = (T) subclassSerializer.createInstance();
			// also initialize fields for which the subclass serializer is not responsible
			initializeFields(target);
		} else {
			target = createInstance();
		}

		if ((flags & NO_SUBCLASS) != 0) {
			try {
				for (int i = 0; i < numFields; i++) {
					boolean isNull = source.readBoolean();
					if (isNull) {
						fields[i].set(target, null);
					} else {
						Object field = fieldSerializers[i].deserialize(source);
						fields[i].set(target, field);
					}
				}
			} catch (IllegalAccessException e) {
				throw new RuntimeException("Error during POJO copy, this should not happen since we check the fields" + "before.");

			}
		} else {
			if (subclassSerializer != null) {
				target = (T) subclassSerializer.deserialize(target, source);
			}
		}
		return target;
	}
	
	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public T deserialize(T reuse, DataInputView source) throws IOException {

		// handle null values
		int flags = source.readByte();
		if((flags & IS_NULL) != 0) {
			return null;
		}

		Class<?> subclass = null;
		TypeSerializer subclassSerializer = null;
		if ((flags & IS_SUBCLASS) != 0) {
			String subclassName = source.readUTF();
			try {
				subclass = Class.forName(subclassName, true, cl);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("Cannot instantiate class.", e);
			}
			subclassSerializer = getSubclassSerializer(subclass);

			if (reuse == null || subclass != reuse.getClass()) {
				// cannot reuse
				reuse = (T) subclassSerializer.createInstance();
				// also initialize fields for which the subclass serializer is not responsible
				initializeFields(reuse);
			}
		} else if ((flags & IS_TAGGED_SUBCLASS) != 0) {
			int subclassTag = source.readByte();
			subclassSerializer = registeredSerializers[subclassTag];

			if (reuse == null || ((PojoSerializer)subclassSerializer).clazz != reuse.getClass()) {
				// cannot reuse
				reuse = (T) subclassSerializer.createInstance();
				// also initialize fields for which the subclass serializer is not responsible
				initializeFields(reuse);
			}
		} else {
			if (reuse == null || clazz != reuse.getClass()) {
				reuse = createInstance();
			}
		}

		if ((flags & NO_SUBCLASS) != 0) {
			try {
				for (int i = 0; i < numFields; i++) {
					boolean isNull = source.readBoolean();
					if (isNull) {
						fields[i].set(reuse, null);
					} else {
						Object field;

						Object reuseField = fields[i].get(reuse);
						if(reuseField != null) {
							field = fieldSerializers[i].deserialize(reuseField, source);
						}
						else {
							field = fieldSerializers[i].deserialize(source);
						}

						fields[i].set(reuse, field);
					}
				}
			} catch (IllegalAccessException e) {
				throw new RuntimeException(
						"Error during POJO copy, this should not happen since we check the fields before.");
			}
		} else {
			if (subclassSerializer != null) {
				reuse = (T) subclassSerializer.deserialize(reuse, source);
			}
		}

		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		// copy the flags
		int flags = source.readByte();
		target.writeByte(flags);

		if ((flags & IS_NULL) != 0) {
			// is a null value, nothing further to copy
			return;
		}

		TypeSerializer<?> subclassSerializer = null;
		if ((flags & IS_SUBCLASS) != 0) {
			String className = source.readUTF();
			target.writeUTF(className);
			try {
				Class<?> subclass = Class.forName(className, true, Thread.currentThread()
						.getContextClassLoader());
				subclassSerializer = getSubclassSerializer(subclass);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("Cannot instantiate class.", e);
			}
		} else if ((flags & IS_TAGGED_SUBCLASS) != 0) {
			int subclassTag = source.readByte();
			target.writeByte(subclassTag);
			subclassSerializer = registeredSerializers[subclassTag];
		}

		if ((flags & NO_SUBCLASS) != 0) {
			for (int i = 0; i < numFields; i++) {
				boolean isNull = source.readBoolean();
				target.writeBoolean(isNull);
				if (!isNull) {
					fieldSerializers[i].copy(source, target);
				}
			}
		} else {
			if (subclassSerializer != null) {
				subclassSerializer.copy(source, target);
			}
		}
	}
	
	@Override
	public int hashCode() {
		int hashCode = numFields * 47;
		for (TypeSerializer<?> ser : this.fieldSerializers) {
			hashCode = (hashCode << 7) | (hashCode >>> -7);
			hashCode += ser.hashCode();
		}
		return hashCode;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj != null && obj instanceof PojoSerializer) {
			PojoSerializer<?> otherTS = (PojoSerializer<?>) obj;
			return (otherTS.clazz == this.clazz) &&
					Arrays.deepEquals(this.fieldSerializers, otherTS.fieldSerializers);
		}
		else {
			return false;
		}
	}
}
