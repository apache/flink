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

package org.apache.flink.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.core.fs.FileSystem;

/**
 * Utility class which provides various methods for dynamic class loading.
 */
public final class ClassUtils {

	/**
	 * Private constructor used to overwrite public one.
	 */
	private ClassUtils() {}

	/**
	 * Searches for a file system class by its name and attempts to load it.
	 * 
	 * @param className
	 *        the name of the file system class
	 * @return an instance of the file system class
	 * @throws ClassNotFoundException
	 *         thrown if no class with such a name can be found
	 */
	public static Class<? extends FileSystem> getFileSystemByName(final String className) throws ClassNotFoundException {
		return Class.forName(className, true, getClassLoader()).asSubclass(FileSystem.class);
	}


	private static ClassLoader getClassLoader() {
		return ClassUtils.class.getClassLoader();
	}
	
	public static Class<?> resolveClassPrimitiveAware(String className) throws ClassNotFoundException {
		if (className == null) {
			throw new NullPointerException();
		}
		
		Class<?> primClass = PRIMITIVE_TYPES.get(className);
		if (primClass != null) {
			return primClass;
		} else {
			return Class.forName(className);
		}
	}
	
	public static boolean isPrimitiveOrBoxedOrString(Class<?> clazz) {
		return clazz != null && (clazz.isPrimitive() || ClassUtils.isBoxedTypeOrString(clazz));
	}
	
	public static boolean isBoxedTypeOrString(Class<?> clazz) {
		return BOXED_TYPES.contains(clazz);
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final Map<String, Class<?>> PRIMITIVE_TYPES = new HashMap<String, Class<?>>(9);
	
	private static final Set<Class<?>> BOXED_TYPES = new HashSet<Class<?>>();
	
	static {
		PRIMITIVE_TYPES.put("byte", byte.class);
		PRIMITIVE_TYPES.put("short", short.class);
		PRIMITIVE_TYPES.put("int", int.class);
		PRIMITIVE_TYPES.put("long", long.class);
		PRIMITIVE_TYPES.put("float", float.class);
		PRIMITIVE_TYPES.put("double", double.class);
		PRIMITIVE_TYPES.put("boolean", boolean.class);
		PRIMITIVE_TYPES.put("char", char.class);
		PRIMITIVE_TYPES.put("void", void.class);
		
		BOXED_TYPES.add(Byte.class);
		BOXED_TYPES.add(Short.class);
		BOXED_TYPES.add(Integer.class);
		BOXED_TYPES.add(Long.class);
		BOXED_TYPES.add(Float.class);
		BOXED_TYPES.add(Double.class);
		BOXED_TYPES.add(Boolean.class);
		BOXED_TYPES.add(Character.class);
		BOXED_TYPES.add(Void.class);
		BOXED_TYPES.add(String.class);
	}
}
