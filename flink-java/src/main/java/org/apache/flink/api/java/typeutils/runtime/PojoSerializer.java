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
import java.util.Arrays;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;


public final class PojoSerializer<T> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;

	private final Class<T> clazz;

	private final TypeSerializer<Object>[] fieldSerializers;

	// We need to handle these ourselves in writeObject()/readObject()
	private transient Field[] fields;

	private final int numFields;

	private final boolean stateful;


	@SuppressWarnings("unchecked")
	public PojoSerializer(Class<T> clazz, TypeSerializer<?>[] fieldSerializers, Field[] fields) {
		this.clazz = clazz;
		this.fieldSerializers = (TypeSerializer<Object>[]) fieldSerializers;
		this.fields = fields;
		this.numFields = fieldSerializers.length;

		for (int i = 0; i < numFields; i++) {
			this.fields[i].setAccessible(true);
		}

		boolean stateful = false;
		for (TypeSerializer<?> ser : fieldSerializers) {
			if (ser.isStateful()) {
				stateful = true;
				break;
			}
		}
		this.stateful = stateful;
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
		int numKeyFields = in.readInt();
		fields = new Field[numKeyFields];
		for (int i = 0; i < numKeyFields; i++) {
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
	}
	
	
	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public boolean isStateful() {
		return this.stateful;
	}
	
	
	@Override
	public T createInstance() {
		try {
			T t = clazz.newInstance();
		
			for (int i = 0; i < numFields; i++) {
				fields[i].set(t, fieldSerializers[i].createInstance());
			}
			
			return t;
		}
		catch (Exception e) {
			throw new RuntimeException("Cannot instantiate class.", e);
		}
	}

	@Override
	public T copy(T from) {
		T target;
		try {
			target = clazz.newInstance();
		}
		catch (Throwable t) {
			throw new RuntimeException("Cannot instantiate class.", t);
		}
		
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
		}
		catch (IllegalAccessException e) {
			throw new RuntimeException("Error during POJO copy, this should not happen since we check the fields before.");
		}
		return target;
	}
	
	@Override
	public T copy(T from, T reuse) {
		try {
			for (int i = 0; i < numFields; i++) {
				Object value = fields[i].get(from);
				if (value != null) {
					Object copy = fieldSerializers[i].copy(fields[i].get(from), fields[i].get(reuse));
					fields[i].set(reuse, copy);
				}
				else {
					fields[i].set(reuse, null);
				}
			}
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Error during POJO copy, this should not happen since we check the fields" +
					"before.");
		}
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}


	@Override
	public void serialize(T value, DataOutputView target) throws IOException {
		// handle null values
		if (value == null) {
			target.writeBoolean(true);
			return;
		} else {
			target.writeBoolean(false);
		}
		try {
			for (int i = 0; i < numFields; i++) {
				Object o = fields[i].get(value);
				if(o == null) {
					target.writeBoolean(true); // null field handling
				} else {
					target.writeBoolean(false);
					fieldSerializers[i].serialize(o, target);
				}
			}
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Error during POJO copy, this should not happen since we check the fields" +
					"before.");
		}
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		boolean isNull = source.readBoolean();
		if(isNull) {
			return null;
		}
		T target;
		try {
			target = clazz.newInstance();
		}
		catch (Throwable t) {
			throw new RuntimeException("Cannot instantiate class.", t);
		}
		
		try {
			for (int i = 0; i < numFields; i++) {
				isNull = source.readBoolean();
				if(isNull) {
					fields[i].set(target, null);
				} else {
					Object field = fieldSerializers[i].deserialize(source);
					fields[i].set(target, field);
				}
			}
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Error during POJO copy, this should not happen since we check the fields" +
					"before.");
		}
		return target;
	}
	
	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		// handle null values
		boolean isNull = source.readBoolean();
		if (isNull) {
			return null;
		}
		try {
			for (int i = 0; i < numFields; i++) {
				isNull = source.readBoolean();
				if(isNull) {
					fields[i].set(reuse, null);
				} else {
					Object field = fieldSerializers[i].deserialize(fields[i].get(reuse), source);
					fields[i].set(reuse, field);
				}
			}
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Error during POJO copy, this should not happen since we check the fields" +
					"before.");
		}
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		// copy the Non-Null/Null tag
		target.writeBoolean(source.readBoolean());
		for (int i = 0; i < numFields; i++) {
			boolean isNull = source.readBoolean();
			target.writeBoolean(isNull);
			if (!isNull) {
				fieldSerializers[i].copy(source, target);
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
