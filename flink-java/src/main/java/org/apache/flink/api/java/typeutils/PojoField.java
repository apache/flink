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

package org.apache.flink.api.java.typeutils;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Objects;

import com.google.common.base.Preconditions;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class PojoField implements Serializable {

	private static final long serialVersionUID = 1975295846436559363L;

	private transient Field field;
	private final TypeInformation<?> type;

	public PojoField(Field field, TypeInformation<?> type) {
		this.field = Preconditions.checkNotNull(field);
		this.type = Preconditions.checkNotNull(type);
	}

	public Field getField() {
		return field;
	}

	public TypeInformation<?> getTypeInformation() {
		return type;
	}

	private void writeObject(ObjectOutputStream out)
			throws IOException, ClassNotFoundException {
		out.defaultWriteObject();
		out.writeObject(field.getDeclaringClass());
		out.writeUTF(field.getName());
	}

	private void readObject(ObjectInputStream in)
			throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		Class<?> clazz = (Class<?>)in.readObject();
		String fieldName = in.readUTF();
		field = null;
		// try superclasses as well
		while (clazz != null) {
			try {
				field = clazz.getDeclaredField(fieldName);
				field.setAccessible(true);
				break;
			} catch (NoSuchFieldException e) {
				clazz = clazz.getSuperclass();
			}
		}
		if (field == null) {
			throw new RuntimeException("Class resolved at TaskManager is not compatible with class read during Plan setup."
					+ " (" + fieldName + ")");
		}
	}

	@Override
	public String toString() {
		return "PojoField " + field.getDeclaringClass() + "." + field.getName() + " (" + type + ")";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof PojoField) {
			PojoField other = (PojoField) obj;

			return other.canEqual(this) && type.equals(other.type) &&
				Objects.equals(field, other.field);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(field, type);
	}

	public boolean canEqual(Object obj) {
		return obj instanceof PojoField;
	}
}