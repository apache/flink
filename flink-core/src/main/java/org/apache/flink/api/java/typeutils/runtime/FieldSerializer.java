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

import org.apache.flink.annotation.Internal;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;

/**
 * This class is for the serialization of java.lang.reflect.Field, which doesn't implement Serializable, therefore
 * readObject/writeObject need to be implemented in classes where there is a field of type java.lang.reflect.Field.
 * The two static methods in this class are to be called from these readObject/writeObject methods.
 */
@Internal
public class FieldSerializer {

	public static void serializeField(Field field, ObjectOutputStream out) throws IOException {
		out.writeObject(field.getDeclaringClass());
		out.writeUTF(field.getName());
	}

	public static Field deserializeField(ObjectInputStream in) throws IOException, ClassNotFoundException  {
		Class<?> clazz = (Class<?>) in.readObject();
		String fieldName = in.readUTF();
		// try superclasses as well
		while (clazz != null) {
			try {
				Field field = clazz.getDeclaredField(fieldName);
				field.setAccessible(true);
				return field;
			} catch (NoSuchFieldException e) {
				clazz = clazz.getSuperclass();
			}
		}

		return null;
	}
}
