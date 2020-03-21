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

import org.junit.Assert;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.Callable;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

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

	/**
	 * Checks whether an exception with a message occurs when running a piece of code.
	 */
	public static void assertThrows(String msg, Class<? extends Exception> expected, Callable<?> code) {
		try {
			Object result = code.call();
			Assert.fail("Previous method call should have failed but it returned: " + result);
		} catch (Exception e) {
			assertThat(e, instanceOf(expected));
			assertThat(e.getMessage(), containsString(msg));
		}
	}
}
