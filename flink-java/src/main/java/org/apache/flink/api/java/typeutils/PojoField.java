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

import org.apache.flink.api.common.typeinfo.TypeInformation;

public class PojoField implements Serializable {
	public transient Field field;
	public TypeInformation<?> type;

	public PojoField(Field field, TypeInformation<?> type) {
		this.field = field;
		this.type = type;
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
}