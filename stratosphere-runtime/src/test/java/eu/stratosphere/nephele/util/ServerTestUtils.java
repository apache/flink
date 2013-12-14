/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.util;

import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.jobmanager.JobManagerITCase;
import eu.stratosphere.nephele.protocols.ExtendedManagementProtocol;

/**
 * This class contains a selection of utility functions which are used for testing the nephele-server module.
 */
public final class ServerTestUtils {

	/**
	 * The system property key to retrieve the user directory.
	 */
	private static final String USER_DIR_KEY = "user.dir";

	/**
	 * The directory containing the correct configuration file to be used during the tests.
	 */
	private static final String CORRECT_CONF_DIR = "/confs/jobmanager";

	/**
	 * The directory the configuration directory is expected in when test are executed using Eclipse.
	 */
	private static final String ECLIPSE_PATH_EXTENSION = "/src/test/resources";

	/**
	 * Private constructor.
	 */
	private ServerTestUtils() {
	}

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
	 * Creates a jar file from the class with the given class name and stores it in the directory for temporary files.
	 * 
	 * @param className
	 *        the name of the class to create a jar file from
	 * @return a {@link File} object referring to the jar file
	 * @throws IOException
	 *         thrown if an error occurs while writing the jar file
	 */
	public static File createJarFile(String className) throws IOException {

		final String jarPath = getTempDir() + File.separator + className + ".jar";
		final File jarFile = new File(jarPath);

		if (jarFile.exists()) {
			jarFile.delete();
		}

		final JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarPath), new Manifest());
		final String classPath = JobManagerITCase.class.getResource("").getPath() + className + ".class";
		final File classFile = new File(classPath);

		String packageName = JobManagerITCase.class.getPackage().getName();
		packageName = packageName.replaceAll("\\.", "\\/");
		jos.putNextEntry(new JarEntry("/" + packageName + "/" + className + ".class"));

		final FileInputStream fis = new FileInputStream(classFile);
		final byte[] buffer = new byte[1024];
		int num = fis.read(buffer);

		while (num != -1) {
			jos.write(buffer, 0, num);
			num = fis.read(buffer);
		}

		fis.close();
		jos.close();

		return jarFile;
	}

	/**
	 * Returns the directory containing the configuration files that shall be used for the test.
	 * 
	 * @return the directory containing the configuration files or <code>null</code> if the configuration directory
	 *         could not be located
	 */
	public static String getConfigDir() {

		// This is the correct path for Maven-based tests
		String configDir = System.getProperty(USER_DIR_KEY) + CORRECT_CONF_DIR;
		if (new File(configDir).exists()) {
			return configDir;
		}

		configDir = System.getProperty(USER_DIR_KEY) + ECLIPSE_PATH_EXTENSION + CORRECT_CONF_DIR;
		if (new File(configDir).exists()) {
			return configDir;
		}

		return null;
	}

	/**
	 * Waits until the job manager for the tests has become ready to accept jobs.
	 * 
	 * @param jobManager
	 *        the instance of the job manager to wait for
	 * @throws IOException
	 *         thrown if a connection to the job manager could not be established
	 * @throws InterruptedException
	 *         thrown if the thread was interrupted while waiting for the job manager to become ready
	 */
	public static void waitForJobManagerToBecomeReady(final ExtendedManagementProtocol jobManager) throws IOException,
			InterruptedException {

		Map<InstanceType, InstanceTypeDescription> instanceMap = jobManager.getMapOfAvailableInstanceTypes();

		while (instanceMap.isEmpty()) {

			Thread.sleep(100);
			instanceMap = jobManager.getMapOfAvailableInstanceTypes();
		}
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
