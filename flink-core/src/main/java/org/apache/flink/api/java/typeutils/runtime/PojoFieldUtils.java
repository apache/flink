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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Field;

/**
 * Utility class for reading, writing, and finding POJO fields.
 */
@Internal
final class PojoFieldUtils {

	/**
	 * Writes a field to the given {@link DataOutputView}.
	 *
	 * <p>This write method avoids Java serialization, by writing only the classname of the field's declaring class
	 * and the field name. The written field can be read using {@link #readField(DataInputView, ClassLoader)}.
	 *
	 * @param out the output view to write to.
	 * @param field the field to write.
	 */
	static void writeField(DataOutputView out, Field field) throws IOException {
		Class<?> declaringClass = field.getDeclaringClass();
		out.writeUTF(declaringClass.getName());
		out.writeUTF(field.getName());
	}

	/**
	 * Reads a field from the given {@link DataInputView}.
	 *
	 * <p>This read methods avoids Java serialization, by reading the classname of the field's declaring class
	 * and dynamically loading it. The field is also read by field name and obtained via reflection.
	 *
	 * @param in the input view to read from.
	 * @param userCodeClassLoader the user classloader.
	 *
	 * @return the read field.
	 */
	static Field readField(DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		Class<?> declaringClass = InstantiationUtil.resolveClassByName(in, userCodeClassLoader);
		String fieldName = in.readUTF();
		return getField(fieldName, declaringClass);
	}

	/**
	 * Finds a field by name from its declaring class. This also searches for the
	 * field in super classes.
	 *
	 * @param fieldName the name of the field to find.
	 * @param declaringClass the declaring class of the field.
	 *
	 * @return the field.
	 */
	@Nullable
	static Field getField(String fieldName, Class<?> declaringClass) {
		Class<?> clazz = declaringClass;

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
