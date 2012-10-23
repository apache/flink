/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

/**
 * This is an auxiliary program which creates a jar file from a set of classes.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public class JarFileCreator {

	/**
	 * The file extension of java classes.
	 */
	private static final String CLASS_EXTENSION = ".class";

	/**
	 * A set of classes which shall be included in the final jar file.
	 */
	private final Set<Class<?>> classSet = new HashSet<Class<?>>();

	/**
	 * The final jar file.
	 */
	private final File outputFile;

	/**
	 * Constructs a new jar file creator.
	 * 
	 * @param outputFile
	 *        the file which shall contain the output data, i.e. the final jar file
	 */
	public JarFileCreator(final File outputFile) {

		this.outputFile = outputFile;
	}

	/**
	 * Adds a {@link Class} object to the set of classes which shall eventually be included in the jar file.
	 * 
	 * @param clazz
	 *        the class to be added to the jar file.
	 */
	public synchronized void addClass(final Class<?> clazz) {

		this.classSet.add(clazz);
	}

	/**
	 * Creates a jar file which contains the previously added class. The content of the jar file is written to
	 * <code>outputFile</code> which has been provided to the constructor. If <code>outputFile</code> already exists, it
	 * is overwritten by this operation.
	 * 
	 * @throws IOException
	 *         thrown if an error occurs while writing to the output file
	 */
	public synchronized void createJarFile() throws IOException {

		// Temporary buffer for the stream copy
		final byte[] buf = new byte[128];

		// Check if output file is valid
		if (this.outputFile == null) {
			throw new IOException("Output file is null");
		}

		// If output file already exists, delete it
		if (this.outputFile.exists()) {
			try {
				this.outputFile.delete();
			} catch (SecurityException se) {
				throw new IOException(se);
			}
		}

		final JarOutputStream jos = new JarOutputStream(new FileOutputStream(this.outputFile), new Manifest());
		final Iterator<Class<?>> it = this.classSet.iterator();
		while (it.hasNext()) {

			final Class<?> clazz = it.next();
			final String entry = clazz.getName().replace('.', '/') + CLASS_EXTENSION;

			jos.putNextEntry(new JarEntry(entry));

			InputStream classInputStream = null;
			try {
				classInputStream = clazz.getResourceAsStream(clazz.getSimpleName() + CLASS_EXTENSION);

				int num = classInputStream.read(buf);
				while (num != -1) {
					jos.write(buf, 0, num);
					num = classInputStream.read(buf);
				}

				classInputStream.close();
				jos.closeEntry();
			} finally {
				CloseableUtils.closeSilently(classInputStream);
			}
		}

		jos.close();
	}
}
