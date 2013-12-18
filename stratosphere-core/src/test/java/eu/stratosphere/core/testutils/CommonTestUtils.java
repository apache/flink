/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.core.testutils;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import eu.stratosphere.configuration.ConfigConstants;
import eu.stratosphere.configuration.GlobalConfiguration;
import eu.stratosphere.core.io.IOReadableWritable;

/**
 * This class contains auxiliary methods for unit tests in the Nephele common module.
 * 
 * @author warneke
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
	public static <T extends IOReadableWritable> T createCopy(final T original) throws IOException {

		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final DataOutputStream dos = new DataOutputStream(baos);

		original.write(dos);

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

		copy.read(dis);

		return copy;
	}
}
