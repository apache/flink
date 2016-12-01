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

import org.junit.Assume;
import org.junit.internal.AssumptionViolatedException;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.fail;

/**
 * This class contains reusable utility methods for unit tests.
 */
public class CommonTestUtils {

	/**
	 * Reads the path to the directory for temporary files from the configuration and returns it.
	 * 
	 * @return the path to the directory for temporary files
	 */
	public static String getTempDir() {
		return System.getProperty("java.io.tmpdir");
	}

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
	 * Checks whether this code runs in a Java 8 (Java 1.8) JVM. If not, this throws a
	 * {@link AssumptionViolatedException}, which causes JUnit to skip the test that
	 * called this method.
	 */
	public static void assumeJava8() {
		try {
			String javaVersionString = System.getProperty("java.runtime.version").substring(0, 3);
			float javaVersion = Float.parseFloat(javaVersionString);
			Assume.assumeTrue(javaVersion >= 1.8f);
		}
		catch (AssumptionViolatedException e) {
			System.out.println("Skipping CassandraConnectorITCase, because the JDK is < Java 8+");
			throw e;
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Cannot determine Java version: " + e.getMessage());
		}
	}

	// This code is taken from: http://stackoverflow.com/a/7201825/568695
	// it changes the environment variables of this JVM. Use only for testing purposes!
	@SuppressWarnings("unchecked")
	public static void setEnv(Map<String, String> newenv) {
		try {
			Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
			Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
			theEnvironmentField.setAccessible(true);
			Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
			env.putAll(newenv);
			Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
			theCaseInsensitiveEnvironmentField.setAccessible(true);
			Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
			cienv.putAll(newenv);
		} catch (NoSuchFieldException e) {
			try {
				Class<?>[] classes = Collections.class.getDeclaredClasses();
				Map<String, String> env = System.getenv();
				for (Class<?> cl : classes) {
					if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
						Field field = cl.getDeclaredField("m");
						field.setAccessible(true);
						Object obj = field.get(env);
						Map<String, String> map = (Map<String, String>) obj;
						map.clear();
						map.putAll(newenv);
					}
				}
			} catch (Exception e2) {
				throw new RuntimeException(e2);
			}
		} catch (Exception e1) {
			throw new RuntimeException(e1);
		}
	}
}
