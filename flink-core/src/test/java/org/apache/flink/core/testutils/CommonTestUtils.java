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

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

/**
 * This class contains reusable utility methods for unit tests.
 */
public class CommonTestUtils {

	/**
	 * Constructs a random filename. The filename is a string of 16 hex characters followed by a <code>.dat</code>
	 * prefix.
	 * 
	 * @return the random filename
	 */
	public static String getRandomFilename() {

		final char[] alphabeth = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

		String filename = "";
		for (int i = 0; i < 16; i++) {
			filename += alphabeth[(int) (Math.random() * alphabeth.length)];
		}

		return filename + ".dat";
	}

	/**
	 * Constructs a random directory name. The directory is a string of 16 hex characters
	 * prefix.
	 * 
	 * @return the random directory name
	 */
	public static String getRandomDirectoryName() {

		final char[] alphabeth = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

		String filename = "";
		for (int i = 0; i < 16; i++) {
			filename += alphabeth[(int) (Math.random() * alphabeth.length)];
		}

		return filename;
	}

	/**
	 * Reads the path to the directory for temporary files from the configuration and returns it.
	 * 
	 * @return the path to the directory for temporary files
	 */
	public static String getTempDir() {

		return GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
			ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH).split(File.pathSeparator)[0];
	}

	/**
	 * Creates a copy of the given {@link IOReadableWritable} object by an in-memory serialization and subsequent
	 * deserialization.
	 * 
	 * @param original
	 *        the original object to be copied
	 * @return the copy of original object created by the original object's serialization/deserialization methods
	 * @throws IOException
	 *         thrown if an error occurs while creating the copy of the object
	 */
	@SuppressWarnings("unchecked")
	public static <T extends IOReadableWritable> T createCopyWritable(final T original) throws IOException {

		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		original.write(new DataOutputViewStreamWrapper(baos));

		final String className = original.getClass().getName();
		if (className == null) {
			fail("Class name is null");
		}

		Class<T> clazz = null;

		try {
			clazz = (Class<T>) Class.forName(className);
		} catch (ClassNotFoundException e) {
			fail(e.getMessage());
		}

		if (clazz == null) {
			fail("Cannot find class with name " + className);
		}

		T copy = null;
		try {
			copy = clazz.newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			fail(e.getMessage());
		}

		if (copy == null) {
			fail("Copy of object of type " + className + " is null");
		}

		final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		copy.read(new DataInputViewStreamWrapper(bais));
		return copy;
	}

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
}
