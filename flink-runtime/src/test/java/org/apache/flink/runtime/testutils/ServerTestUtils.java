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


package org.apache.flink.runtime.testutils;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.InputViewDataInputStreamWrapper;
import org.apache.flink.core.memory.OutputViewDataOutputStreamWrapper;

/**
 * This class contains a selection of utility functions which are used for testing the nephele-server module.
 */
public final class ServerTestUtils {

	/**
	 * Private constructor.
	 */
	private ServerTestUtils() {}

	/**
	 * Creates a file with a random name in the given sub directory within the directory for temporary files. The
	 * directory for temporary files is read from the configuration. The file contains a sequence of integer numbers
	 * from 0 to <code>limit</code>. The individual numbers are separated by a newline.
	 * 
	 * @param subDirectory
	 *        name of the sub directory to create the input file in
	 * @param limit
	 *        the upper bound for the sequence of integer numbers to generate
	 * @return a {@link File} object referring to the created file
	 * @throws IOException
	 *         thrown if an I/O error occurs while writing the file
	 */
	public static File createInputFile(String subDirectory, int limit) throws IOException {

		if (limit < 0) {
			throw new IllegalArgumentException("limit must be >= 0");
		}

		final File inputFile = new File(getTempDir() + File.separator + subDirectory + File.separator
			+ getRandomFilename());

		if (inputFile.exists()) {
			inputFile.delete();
		}

		inputFile.createNewFile();
		FileWriter fw = new FileWriter(inputFile);
		for (int i = 0; i < limit; i++) {

			fw.write(Integer.toString(i) + "\n");
		}
		fw.close();

		return inputFile;
	}

	/**
	 * Creates a file with a random name in the directory for temporary files. The directory for temporary files is read
	 * from the configuration. The file contains a sequence of integer numbers from 0 to <code>limit</code>. The
	 * individual numbers are separated by a newline.
	 * 
	 * @param limit
	 *        the upper bound for the sequence of integer numbers to generate
	 * @return a {@link File} object referring to the created file
	 * @throws IOException
	 *         thrown if an I/O error occurs while writing the file
	 */
	public static File createInputFile(int limit) throws IOException {
		return createInputFile("", limit);
	}

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
	 * Returns the path to the directory for temporary files.
	 * 
	 * @return the path to the directory for temporary files
	 */
	public static String getTempDir() {

		return System.getProperty("java.io.tmpdir");
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
	public static <T extends IOReadableWritable> T createCopy(final T original) throws IOException {

		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final DataOutputStream dos = new DataOutputStream(baos);

		original.write(new OutputViewDataOutputStreamWrapper(dos));

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
		} catch (InstantiationException e) {
			fail(e.getMessage());
		} catch (IllegalAccessException e) {
			fail(e.getMessage());
		}

		if (copy == null) {
			fail("Copy of object of type " + className + " is null");
		}

		final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		final DataInputStream dis = new DataInputStream(bais);

		copy.read(new InputViewDataInputStreamWrapper(dis));

		return copy;
	}
}
