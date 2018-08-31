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


package org.apache.flink.runtime.util;

import org.apache.flink.shaded.asm5.org.objectweb.asm.ClassReader;
import org.apache.flink.shaded.asm5.org.objectweb.asm.Opcodes;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
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
	 * The namespace of the dependencies to be packaged.
	 */
	private final Set<String> packages = new HashSet<String>();

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
	public synchronized JarFileCreator addClass(final Class<?> clazz) {

		this.classSet.add(clazz);
		String name = clazz.getName();
		int n = name.lastIndexOf('.');
		if (n > -1) {
			name = name.substring(0, n);
		}
		return addPackage(name);
	}

	/**
	 * Manually specify the package of the dependencies.
	 *
	 * @param p
	 * 		  the package to be included.
	 */
	public synchronized JarFileCreator addPackage(String p) {
		this.packages.add(p);
		return this;
	}

	/**
	 * Manually specify the packages of the dependencies.
	 *
	 * @param packages
	 *        the packages to be included.
	 */
	public synchronized JarFileCreator addPackages(String[] packages) {
		for (String p : packages) {
			addPackage(p);
		}
		return this;
	}

	/**
	 * Add the dependencies within the given packages automatically.
	 * @throws IOException
	 * 			throw if an error occurs while read the class file.
	 */
	private synchronized void addDependencies() throws IOException {
		List<String> dependencies = new ArrayList<String>();
		for (Class clazz : classSet) {
			dependencies.add(clazz.getName());
		}
		//Traverse the dependency tree using BFS.
		int head = 0;
		while (head != dependencies.size()) {
			DependencyVisitor v = new DependencyVisitor(Opcodes.ASM5);
			v.addNameSpace(this.packages);
			InputStream classInputStream = null;
			String name = dependencies.get(head);
			try {
				Class clazz = Class.forName(name);
				int n = name.lastIndexOf('.');
				String className = null;
				if (n > -1) {
					className = name.substring(n + 1, name.length());
				}
				classInputStream = clazz.getResourceAsStream(className + CLASS_EXTENSION);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e.getMessage());
			}
			new ClassReader(classInputStream).accept(v, 0);
			classInputStream.close();

			//Update the BFS queue.
			Set<String> classPackages = v.getPackages();
			for (String s : classPackages) {
				if (!dependencies.contains(s.replace('/','.'))) {
					dependencies.add(s.replace('/','.'));
				}
			}
			head++;
		}

		for (String dependency : dependencies) {
			try {
				this.classSet.add(Class.forName(dependency));
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e.getMessage());
			}
		}
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
		//Retrieve dependencies automatically
		addDependencies();

		// Temporary buffer for the stream copy
		final byte[] buf = new byte[128];

		// Check if output file is valid
		if (this.outputFile == null) {
			throw new IOException("Output file is null");
		}

		// If output file already exists, delete it
		if (this.outputFile.exists()) {
			this.outputFile.delete();
		}

		try ( FileOutputStream fos = new FileOutputStream(this.outputFile); JarOutputStream jos = new JarOutputStream(fos, new Manifest())) {
			final Iterator<Class<?>> it = this.classSet.iterator();
			while (it.hasNext()) {

				final Class<?> clazz = it.next();
				final String entry = clazz.getName().replace('.', '/') + CLASS_EXTENSION;

				jos.putNextEntry(new JarEntry(entry));

				String name = clazz.getName();
				int n = name.lastIndexOf('.');
				String className = null;
				if (n > -1) {
					className = name.substring(n + 1, name.length());
				}
				//Using the part after last dot instead of class.getSimpleName() could resolve the problem of inner class.
				final InputStream classInputStream = clazz.getResourceAsStream(className + CLASS_EXTENSION);

				int num = classInputStream.read(buf);
				while (num != -1) {
					jos.write(buf, 0, num);
					num = classInputStream.read(buf);
				}

				classInputStream.close();
				jos.closeEntry();
			}
		}
	}
}
