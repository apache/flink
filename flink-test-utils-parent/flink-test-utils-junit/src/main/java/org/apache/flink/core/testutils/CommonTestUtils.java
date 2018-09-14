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

package org.apache.flink.core.testutils;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.security.CodeSource;
import java.security.Permissions;
import java.security.ProtectionDomain;
import java.security.cert.Certificate;
import java.util.Map;

/**
 * This class contains reusable utility methods for unit tests.
 */
public class CommonTestUtils {

	/**
	 * Creates a copy of an object via Java Serialization.
	 *
	 * @param original The original object.
	 * @return The copied object.
	 */
	public static <T extends java.io.Serializable> T createCopySerializable(T original) throws IOException {
		if (original == null) {
			throw new IllegalArgumentException();
		}

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(original);
		oos.close();
		baos.close();

		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());

		try (ObjectInputStream ois = new ObjectInputStream(bais)) {
			@SuppressWarnings("unchecked")
			T copy = (T) ois.readObject();
			return copy;
		}
		catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
	}

	/**
	 * Creates a temporary file that contains the given string.
	 * The file is written with the platform's default encoding.
	 *
	 * <p>The temp file is automatically deleted on JVM exit.
	 *
	 * @param contents The contents to be written to the file.
	 * @return The temp file URI.
	 */
	public static String createTempFile(String contents) throws IOException {
		File f = File.createTempFile("flink_test_", ".tmp");
		f.deleteOnExit();

		try (BufferedWriter out = new BufferedWriter(new FileWriter(f))) {
			out.write(contents);
		}
		return f.toURI().toString();
	}

	/**
	 * Permanently blocks the current thread. The thread cannot be woken
	 * up via {@link Thread#interrupt()}.
	 */
	public static void blockForeverNonInterruptibly() {
		final Object lock = new Object();
		//noinspection InfiniteLoopStatement
		while (true) {
			try {
				//noinspection SynchronizationOnLocalVariableOrMethodParameter
				synchronized (lock) {
					lock.wait();
				}
			} catch (InterruptedException ignored) {}
		}
	}

	// ------------------------------------------------------------------------
	//  Manipulation of environment
	// ------------------------------------------------------------------------

	public static void setEnv(Map<String, String> newenv) {
		setEnv(newenv, true);
	}

	// This code is taken slightly modified from: http://stackoverflow.com/a/7201825/568695
	// it changes the environment variables of this JVM. Use only for testing purposes!
	@SuppressWarnings("unchecked")
	public static void setEnv(Map<String, String> newenv, boolean clearExisting) {
		try {
			Map<String, String> env = System.getenv();
			Class<?> clazz = env.getClass();
			Field field = clazz.getDeclaredField("m");
			field.setAccessible(true);
			Map<String, String> map = (Map<String, String>) field.get(env);
			if (clearExisting) {
				map.clear();
			}
			map.putAll(newenv);

			// only for Windows
			Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
			try {
				Field theCaseInsensitiveEnvironmentField =
					processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
				theCaseInsensitiveEnvironmentField.setAccessible(true);
				Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
				if (clearExisting) {
					cienv.clear();
				}
				cienv.putAll(newenv);
			} catch (NoSuchFieldException ignored) {}

		} catch (Exception e1) {
			throw new RuntimeException(e1);
		}
	}

	// ------------------------------------------------------------------------
	//  Testing of objects not in the application class loader
	// ------------------------------------------------------------------------

	/**
	 * Creates a new class that is not part of the classpath that the current JVM uses, and
	 * instantiates it.
	 *
	 * <p>This method uses {@link #createClassNotInClassPath(ClassLoader)} to define the new class.
	 *
	 * @param targetClassLoader The class loader to attach the class to
	 * @return The object instantiated from the newly defined class.
	 */
	public static Serializable createObjectForClassNotInClassPath(ClassLoader targetClassLoader) {
		try {
			Class<? extends Serializable> clazz = createClassNotInClassPath(targetClassLoader);
			return clazz.newInstance();
		}
		catch (Exception e) {
			throw new AssertionError("test setup broken", e);
		}
	}

	/**
	 * Creates a new class that is not part of the classpath that the current JVM uses.
	 * The class is ad-hoc defined and attached to the given ClassLoader.
	 *
	 * @param targetClassLoader The class loader to attach the class to
	 * @return The newly defined class
	 */
	public static Class<? extends Serializable> createClassNotInClassPath(ClassLoader targetClassLoader) {
		final byte[] classData = {-54, -2, -70, -66, 0, 0, 0, 51, 0, 65, 10, 0, 15, 0, 43, 7, 0, 44,
				10, 0, 2, 0, 43, 10, 0, 2, 0, 45, 9, 0, 7, 0, 46, 10, 0, 15, 0, 47, 7, 0, 48, 7, 0,
				49, 10, 0, 8, 0, 43, 8, 0, 50, 10, 0, 8, 0, 51, 10, 0, 8, 0, 52, 10, 0, 8, 0, 53, 10,
				0, 8, 0, 54, 7, 0, 55, 7, 0, 56, 1, 0, 16, 115, 101, 114, 105, 97, 108, 86, 101, 114,
				115, 105, 111, 110, 85, 73, 68, 1, 0, 1, 74, 1, 0, 13, 67, 111, 110, 115, 116, 97, 110,
				116, 86, 97, 108, 117, 101, 5, -1, -1, -1, -1, -1, -1, -1, -3, 1, 0, 6, 114, 97, 110,
				100, 111, 109, 1, 0, 6, 60, 105, 110, 105, 116, 62, 1, 0, 3, 40, 41, 86, 1, 0, 4, 67,
				111, 100, 101, 1, 0, 15, 76, 105, 110, 101, 78, 117, 109, 98, 101, 114, 84, 97, 98, 108,
				101, 1, 0, 18, 76, 111, 99, 97, 108, 86, 97, 114, 105, 97, 98, 108, 101, 84, 97, 98,
				108, 101, 1, 0, 4, 116, 104, 105, 115, 1, 0, 35, 76, 111, 114, 103, 47, 97, 112, 97, 99,
				104, 101, 47, 102, 108, 105, 110, 107, 47, 84, 101, 115, 116, 83, 101, 114, 105, 97, 108,
				105, 122, 97, 98, 108, 101, 59, 1, 0, 6, 101, 113, 117, 97, 108, 115, 1, 0, 21, 40, 76,
				106, 97, 118, 97, 47, 108, 97, 110, 103, 47, 79, 98, 106, 101, 99, 116, 59, 41, 90, 1, 0,
				1, 111, 1, 0, 18, 76, 106, 97, 118, 97, 47, 108, 97, 110, 103, 47, 79, 98, 106, 101, 99,
				116, 59, 1, 0, 4, 116, 104, 97, 116, 1, 0, 13, 83, 116, 97, 99, 107, 77, 97, 112, 84, 97,
				98, 108, 101, 7, 0, 48, 1, 0, 8, 104, 97, 115, 104, 67, 111, 100, 101, 1, 0, 3, 40, 41,
				73, 1, 0, 8, 116, 111, 83, 116, 114, 105, 110, 103, 1, 0, 20, 40, 41, 76, 106, 97, 118, 97,
				47, 108, 97, 110, 103, 47, 83, 116, 114, 105, 110, 103, 59, 1, 0, 10, 83, 111, 117, 114,
				99, 101, 70, 105, 108, 101, 1, 0, 21, 84, 101, 115, 116, 83, 101, 114, 105, 97, 108, 105,
				122, 97, 98, 108, 101, 46, 106, 97, 118, 97, 12, 0, 23, 0, 24, 1, 0, 16, 106, 97, 118, 97,
				47, 117, 116, 105, 108, 47, 82, 97, 110, 100, 111, 109, 12, 0, 57, 0, 58, 12, 0, 22, 0, 18,
				12, 0, 59, 0, 60, 1, 0, 33, 111, 114, 103, 47, 97, 112, 97, 99, 104, 101, 47, 102, 108, 105,
				110, 107, 47, 84, 101, 115, 116, 83, 101, 114, 105, 97, 108, 105, 122, 97, 98, 108, 101, 1,
				0, 23, 106, 97, 118, 97, 47, 108, 97, 110, 103, 47, 83, 116, 114, 105, 110, 103, 66, 117,
				105, 108, 100, 101, 114, 1, 0, 24, 84, 101, 115, 116, 83, 101, 114, 105, 97, 108, 105, 122,
				97, 98, 108, 101, 123, 114, 97, 110, 100, 111, 109, 61, 12, 0, 61, 0, 62, 12, 0, 61, 0, 63,
				12, 0, 61, 0, 64, 12, 0, 39, 0, 40, 1, 0, 16, 106, 97, 118, 97, 47, 108, 97, 110, 103, 47,
				79, 98, 106, 101, 99, 116, 1, 0, 20, 106, 97, 118, 97, 47, 105, 111, 47, 83, 101, 114, 105,
				97, 108, 105, 122, 97, 98, 108, 101, 1, 0, 8, 110, 101, 120, 116, 76, 111, 110, 103, 1, 0,
				3, 40, 41, 74, 1, 0, 8, 103, 101, 116, 67, 108, 97, 115, 115, 1, 0, 19, 40, 41, 76, 106, 97,
				118, 97, 47, 108, 97, 110, 103, 47, 67, 108, 97, 115, 115, 59, 1, 0, 6, 97, 112, 112, 101,
				110, 100, 1, 0, 45, 40, 76, 106, 97, 118, 97, 47, 108, 97, 110, 103, 47, 83, 116, 114, 105,
				110, 103, 59, 41, 76, 106, 97, 118, 97, 47, 108, 97, 110, 103, 47, 83, 116, 114, 105, 110,
				103, 66, 117, 105, 108, 100, 101, 114, 59, 1, 0, 28, 40, 74, 41, 76, 106, 97, 118, 97, 47,
				108, 97, 110, 103, 47, 83, 116, 114, 105, 110, 103, 66, 117, 105, 108, 100, 101, 114, 59, 1,
				0, 28, 40, 67, 41, 76, 106, 97, 118, 97, 47, 108, 97, 110, 103, 47, 83, 116, 114, 105, 110,
				103, 66, 117, 105, 108, 100, 101, 114, 59, 0, 33, 0, 7, 0, 15, 0, 1, 0, 16, 0, 2, 0, 26, 0,
				17, 0, 18, 0, 1, 0, 19, 0, 0, 0, 2, 0, 20, 0, 18, 0, 22, 0, 18, 0, 0, 0, 4, 0, 1, 0, 23, 0,
				24, 0, 1, 0, 25, 0, 0, 0, 69, 0, 3, 0, 1, 0, 0, 0, 19, 42, -73, 0, 1, 42, -69, 0, 2, 89, -73,
				0, 3, -74, 0, 4, -75, 0, 5, -79, 0, 0, 0, 2, 0, 26, 0, 0, 0, 14, 0, 3, 0, 0, 0, 30, 0, 4, 0,
				31, 0, 18, 0, 32, 0, 27, 0, 0, 0, 12, 0, 1, 0, 0, 0, 19, 0, 28, 0, 29, 0, 0, 0, 1, 0, 30, 0,
				31, 0, 1, 0, 25, 0, 0, 0, -116, 0, 4, 0, 3, 0, 0, 0, 47, 42, 43, -90, 0, 5, 4, -84, 43, -58,
				0, 14, 42, -74, 0, 6, 43, -74, 0, 6, -91, 0, 5, 3, -84, 43, -64, 0, 7, 77, 42, -76, 0, 5, 44,
				-76, 0, 5, -108, -102, 0, 7, 4, -89, 0, 4, 3, -84, 0, 0, 0, 3, 0, 26, 0, 0, 0, 18, 0, 4, 0, 0,
				0, 36, 0, 7, 0, 37, 0, 24, 0, 39, 0, 29, 0, 40, 0, 27, 0, 0, 0, 32, 0, 3, 0, 0, 0, 47, 0, 28,
				0, 29, 0, 0, 0, 0, 0, 47, 0, 32, 0, 33, 0, 1, 0, 29, 0, 18, 0, 34, 0, 29, 0, 2, 0, 35, 0, 0,
				0, 13, 0, 5, 7, 14, 1, -4, 0, 20, 7, 0, 36, 64, 1, 0, 1, 0, 37, 0, 38, 0, 1, 0, 25, 0, 0, 0,
				56, 0, 5, 0, 1, 0, 0, 0, 14, 42, -76, 0, 5, 42, -76, 0, 5, 16, 32, 125, -125, -120, -84, 0, 0,
				0, 2, 0, 26, 0, 0, 0, 6, 0, 1, 0, 0, 0, 46, 0, 27, 0, 0, 0, 12, 0, 1, 0, 0, 0, 14, 0, 28, 0,
				29, 0, 0, 0, 1, 0, 39, 0, 40, 0, 1, 0, 25, 0, 0, 0, 70, 0, 3, 0, 1, 0, 0, 0, 28, -69, 0, 8,
				89, -73, 0, 9, 18, 10, -74, 0, 11, 42, -76, 0, 5, -74, 0, 12, 16, 125, -74, 0, 13, -74, 0, 14,
				-80, 0, 0, 0, 2, 0, 26, 0, 0, 0, 6, 0, 1, 0, 0, 0, 51, 0, 27, 0, 0, 0, 12, 0, 1, 0, 0, 0, 28,
				0, 28, 0, 29, 0, 0, 0, 1, 0, 41, 0, 0, 0, 2, 0, 42};

		try {
			// define a class into the classloader
			Class<?> clazz = getUnsafe().defineClass(
					"org.apache.flink.TestSerializable",
					classData, 0, classData.length,
					targetClassLoader,
					new ProtectionDomain(new CodeSource(null, (Certificate[]) null), new Permissions()));

			return clazz.asSubclass(Serializable.class);
		}
		catch (Exception e) {
			throw new AssertionError("test setup broken", e);
		}
	}

	@SuppressWarnings("restriction")
	private static sun.misc.Unsafe getUnsafe() {
		try {
			Field unsafeField = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
			unsafeField.setAccessible(true);
			return (sun.misc.Unsafe) unsafeField.get(null);
		} catch (SecurityException e) {
			throw new RuntimeException("Could not access the sun.misc.Unsafe handle, permission denied by security manager.", e);
		} catch (NoSuchFieldException e) {
			throw new RuntimeException("The static handle field in sun.misc.Unsafe was not found.");
		} catch (IllegalArgumentException e) {
			throw new RuntimeException("Bug: Illegal argument reflection access for static field.", e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException("Access to sun.misc.Unsafe is forbidden by the runtime.", e);
		} catch (Throwable t) {
			throw new RuntimeException("Unclassified error while trying to access the sun.misc.Unsafe handle.", t);
		}
	}

	/**
	 * Checks whether the given throwable contains the given cause as a cause. The cause is not checked
	 * on equality but on type equality.
	 *
	 * @param throwable Throwable to check for the cause
	 * @param cause Cause to look for
	 * @return True if the given Throwable contains the given cause (type equality); otherwise false
	 */
	public static boolean containsCause(Throwable throwable, Class<? extends Throwable> cause) {
		Throwable current = throwable;

		while (current != null) {
			if (cause.isAssignableFrom(current.getClass())) {
				return true;
			}

			current = current.getCause();
		}

		return false;
	}
}
