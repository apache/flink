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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

/**
 * Utility class to deal with native libraries.
 * 
 * @author warneke
 */
public class NativeCodeLoader {

	/**
	 * Set of the native libraries which are already loaded.
	 */
	private static Set<String> loadedLibrarySet = new HashSet<String>();

	/**
	 * Directory prefix for native libraries inside JAR files
	 */
	private static final String JAR_PREFIX = "META-INF/lib/";

	/**
	 * Size of temporary buffer to extract native libraries in bytes.
	 */
	private static final int BUFSIZE = 8192;

	/**
	 * Empty private constructor to avoid instantiation.
	 */
	private NativeCodeLoader() {
	}

	/**
	 * Loads a native library from a file.
	 * 
	 * @param directory
	 *        the directory in which the library is supposed to be located
	 * @param filename
	 *        filename of the library to be loaded
	 * @throws SecurityException
	 *         thrown if a security exception occurs.
	 * @throws UnsatisfiedLinkError
	 *         thrown if the library contains unsatisfied links
	 * @throws IOException
	 *         thrown if an internal native library cannot be extracted
	 */
	public static void loadLibraryFromFile(String directory, String filename) throws SecurityException,
			UnsatisfiedLinkError, IOException {

		final String libraryPath = directory + File.separator + filename;

		synchronized (loadedLibrarySet) {

			final File outputFile = new File(directory, filename);
			if (!outputFile.exists()) {

				// Try to extract the library from the system resources
				final ClassLoader cl = ClassLoader.getSystemClassLoader();
				final InputStream in = cl.getResourceAsStream(JAR_PREFIX + filename);
				if (in == null) {
					throw new IOException("Unable to extract native library " + filename + " to " + directory);
				}

				final OutputStream out = new FileOutputStream(outputFile);
				copy(in, out);
			}

			System.load(libraryPath);
			loadedLibrarySet.add(filename);
		}
	}

	/**
	 * Copies all data from the given input stream to the given output stream.
	 * 
	 * @param in
	 *        the input stream to read data from
	 * @param out
	 *        the output stream to write data to
	 * @throws IOException
	 *         thrown if an I/O error occurs while copying the data
	 */
	private static void copy(InputStream in, OutputStream out) throws IOException {

		final byte[] buf = new byte[BUFSIZE];
		int len = 0;
		while (true) {
			len = in.read(buf);
			if (len <= 0) {
				break;
			}
			out.write(buf, 0, len);
		}
	}

	/**
	 * Checks if a native library is already loaded.
	 * 
	 * @param libraryName
	 *        the filename of the library to check
	 * @return <code>true</code> if the library is already loaded, <code>false</code> otherwise
	 */
	public static boolean isLibraryLoaded(String libraryName) {

		synchronized (loadedLibrarySet) {
			return loadedLibrarySet.contains(libraryName);
		}

	}
}
