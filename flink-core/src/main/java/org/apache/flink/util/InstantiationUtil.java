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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.InputViewDataInputStreamWrapper;
import org.apache.flink.core.memory.OutputViewDataOutputStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.HashMap;

/**
 * Utility class to create instances from class objects and checking failure reasons.
 */
public final class InstantiationUtil {
	
	/**
	 * A custom ObjectInputStream that can also load user-code using a
	 * user-code ClassLoader.
	 *
	 */
	public static class ClassLoaderObjectInputStream extends ObjectInputStream {
		private ClassLoader classLoader;

		private static final HashMap<String, Class<?>> primitiveClasses
				= new HashMap<String, Class<?>>(8, 1.0F);
		static {
			primitiveClasses.put("boolean", boolean.class);
			primitiveClasses.put("byte", byte.class);
			primitiveClasses.put("char", char.class);
			primitiveClasses.put("short", short.class);
			primitiveClasses.put("int", int.class);
			primitiveClasses.put("long", long.class);
			primitiveClasses.put("float", float.class);
			primitiveClasses.put("double", double.class);
			primitiveClasses.put("void", void.class);
		}

		@Override
		public Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
			if (classLoader != null) {
				String name = desc.getName();
				try {
					return Class.forName(name, false, classLoader);
				} catch (ClassNotFoundException ex) {
					// check if class is a primitive class
					Class<?> cl = primitiveClasses.get(name);
					if (cl != null) {
						// return primitive class
						return cl;
					} else {
						// throw ClassNotFoundException
						throw ex;
					}
				}
			}

			return super.resolveClass(desc);
		}

		public ClassLoaderObjectInputStream(InputStream in, ClassLoader classLoader) throws IOException {
			super(in);
			this.classLoader = classLoader;
		}
	}
	
	
	/**
	 * Creates a new instance of the given class.
	 * 
	 * @param <T> The generic type of the class.
	 * @param clazz The class to instantiate.
	 * @param castTo Optional parameter, specifying the class that the given class must be a subclass off. This
	 *               argument is added to prevent class cast exceptions occurring later. 
	 * @return An instance of the given class.
	 * 
	 * @throws RuntimeException Thrown, if the class could not be instantiated. The exception contains a detailed
	 *                          message about the reason why the instantiation failed.
	 */
	public static <T> T instantiate(Class<T> clazz, Class<? super T> castTo) {
		if (clazz == null) {
			throw new NullPointerException();
		}
		
		// check if the class is a subclass, if the check is required
		if (castTo != null && !castTo.isAssignableFrom(clazz)) {
			throw new RuntimeException("The class '" + clazz.getName() + "' is not a subclass of '" + 
				castTo.getName() + "' as is required.");
		}
		
		return instantiate(clazz);
	}

	/**
	 * Creates a new instance of the given class.
	 * 
	 * @param <T> The generic type of the class.
	 * @param clazz The class to instantiate.

	 * @return An instance of the given class.
	 * 
	 * @throws RuntimeException Thrown, if the class could not be instantiated. The exception contains a detailed
	 *                          message about the reason why the instantiation failed.
	 */
	public static <T> T instantiate(Class<T> clazz) {
		if (clazz == null) {
			throw new NullPointerException();
		}
		
		// try to instantiate the class
		try {
			return clazz.newInstance();
		}
		catch (InstantiationException | IllegalAccessException iex) {
			// check for the common problem causes
			checkForInstantiation(clazz);
			
			// here we are, if non of the common causes was the problem. then the error was
			// most likely an exception in the constructor or field initialization
			throw new RuntimeException("Could not instantiate type '" + clazz.getName() + 
					"' due to an unspecified exception: " + iex.getMessage(), iex);
		}
		catch (Throwable t) {
			String message = t.getMessage();
			throw new RuntimeException("Could not instantiate type '" + clazz.getName() + 
				"' Most likely the constructor (or a member variable initialization) threw an exception" + 
				(message == null ? "." : ": " + message), t);
		}
	}
	
	/**
	 * Checks, whether the given class has a public nullary constructor.
	 * 
	 * @param clazz The class to check.
	 * @return True, if the class has a public nullary constructor, false if not.
	 */
	public static boolean hasPublicNullaryConstructor(Class<?> clazz) {
		Constructor<?>[] constructors = clazz.getConstructors();
		for (Constructor<?> constructor : constructors) {
			if (constructor.getParameterTypes().length == 0 &&
					Modifier.isPublic(constructor.getModifiers())) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Checks, whether the given class is public.
	 * 
	 * @param clazz The class to check.
	 * @return True, if the class is public, false if not.
	 */
	public static boolean isPublic(Class<?> clazz) {
		return Modifier.isPublic(clazz.getModifiers());
	}
	
	/**
	 * Checks, whether the class is a proper class, i.e. not abstract or an interface, and not a primitive type.
	 * 
	 * @param clazz The class to check.
	 * @return True, if the class is a proper class, false otherwise.
	 */
	public static boolean isProperClass(Class<?> clazz) {
		int mods = clazz.getModifiers();
		return !(Modifier.isAbstract(mods) || Modifier.isInterface(mods) || Modifier.isNative(mods));
	}

	/**
	 * Checks, whether the class is an inner class that is not statically accessible. That is especially true for
	 * anonymous inner classes.
	 * 
	 * @param clazz The class to check.
	 * @return True, if the class is a non-statically accessible inner class.
	 */
	public static boolean isNonStaticInnerClass(Class<?> clazz) {
		if (clazz.getEnclosingClass() == null) {
			// no inner class
			return false;
		} else {
			// inner class
			if (clazz.getDeclaringClass() != null) {
				// named inner class
				return !Modifier.isStatic(clazz.getModifiers());
			} else {
				// anonymous inner class
				return true;
			}
		}
	}
	
	/**
	 * Performs a standard check whether the class can be instantiated by {@code Class#newInstance()}.
	 * 
	 * @param clazz The class to check.
	 * @throws RuntimeException Thrown, if the class cannot be instantiated by {@code Class#newInstance()}.
	 */
	public static void checkForInstantiation(Class<?> clazz) {
		final String errorMessage = checkForInstantiationError(clazz);
		
		if (errorMessage != null) {
			throw new RuntimeException("The class '" + clazz.getName() + "' is not instantiable: " + errorMessage);
		}
	}
	
	public static String checkForInstantiationError(Class<?> clazz) {
		if (!isPublic(clazz)) {
			return "The class is not public.";
		} else if (clazz.isArray()) {
			return "The class is an array. An array cannot be simply instantiated, as with a parameterless constructor.";
		} else if (!isProperClass(clazz)) {
			return "The class is no proper class, it is either abstract, an interface, or a primitive type.";
		} else if (isNonStaticInnerClass(clazz)) {
			return "The class is an inner class, but not statically accessible.";
		} else if (!hasPublicNullaryConstructor(clazz)) {
			return "The class has no (implicit) public nullary constructor, i.e. a constructor without arguments.";
		} else {
			return null; 
		}
	}
	
	public static Object readObjectFromConfig(Configuration config, String key, ClassLoader cl) throws IOException, ClassNotFoundException {
		byte[] bytes = config.getBytes(key, null);
		if (bytes == null) {
			return null;
		}
		
		return deserializeObject(bytes, cl);
	}
	
	public static void writeObjectToConfig(Object o, Configuration config, String key) throws IOException {
		byte[] bytes = serializeObject(o);
		config.setBytes(key, bytes);
	}

	public static <T> byte[] serializeToByteArray(TypeSerializer<T> serializer, T record) throws IOException {
		if (record == null) {
			throw new NullPointerException("Record to serialize to byte array must not be null.");
		}

		ByteArrayOutputStream bos = new ByteArrayOutputStream(64);
		OutputViewDataOutputStreamWrapper outputViewWrapper = new OutputViewDataOutputStreamWrapper(new DataOutputStream(bos));

		serializer.serialize(record, outputViewWrapper);

		return bos.toByteArray();
	}

	public static <T> T deserializeFromByteArray(TypeSerializer<T> serializer, byte[] buf) throws IOException {
		if (buf == null) {
			throw new NullPointerException("Byte array to deserialize from must not be null.");
		}

		InputViewDataInputStreamWrapper inputViewWrapper = new InputViewDataInputStreamWrapper(new DataInputStream(new ByteArrayInputStream(buf)));

		T record = serializer.createInstance();
		return serializer.deserialize(record, inputViewWrapper);
	}
	
	public static Object deserializeObject(byte[] bytes, ClassLoader cl) throws IOException, ClassNotFoundException {
		ObjectInputStream oois = null;
		final ClassLoader old = Thread.currentThread().getContextClassLoader();
		try {
			Thread.currentThread().setContextClassLoader(cl);
			oois = new ClassLoaderObjectInputStream(new ByteArrayInputStream(bytes), cl);
			return oois.readObject();
		} finally {
			Thread.currentThread().setContextClassLoader(old);
			if (oois != null) {
				oois.close();
			}
		}
	}
	
	public static byte[] serializeObject(Object o) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
			oos.writeObject(o);
		}

		return baos.toByteArray();
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Private constructor to prevent instantiation.
	 */
	private InstantiationUtil() {
		throw new RuntimeException();
	}
}
