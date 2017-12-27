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

import org.apache.flink.api.common.Program;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.util.InstantiationUtil;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Random;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/**
 * Jar file validation tool class for {@link Program}.
 */
public class JarWithProgramUtils {
	/**
	 * Property name of the entry in JAR manifest file that describes the Flink specific entry point.
	 */
	public static final String MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS = "program-class";

	/**
	 * Property name of the entry in JAR manifest file that describes the class with the main method.
	 */
	public static final String MANIFEST_ATTRIBUTE_MAIN_CLASS = "Main-Class";

	/**
	 * Get main class from jar file.
	 *
	 * @param jarFile The given jar file.
	 * @param entryPointClassName The given class name.
	 * @param parent The given parent classloader.
	 * @return The result class.
	 * @throws Exception Any exception thrown by this method signals get main class fail.
	 */
	public static Class<?> getMainClass(File jarFile, String entryPointClassName, ClassLoader parent) throws Exception {
		if (jarFile == null) {
			throw new IllegalArgumentException("The jar file must not be null.");
		}

		URL jarFileUrl;
		try {
			jarFileUrl = jarFile.getAbsoluteFile().toURI().toURL();
		} catch (MalformedURLException e1) {
			throw new IllegalArgumentException("The jar file path is invalid.");
		}

		checkJarFile(jarFileUrl);

		// if no entryPointClassName name was given, we try and look one up through the manifest
		if (entryPointClassName == null) {
			entryPointClassName = getEntryPointClassNameFromJar(jarFileUrl);
		}

		// now that we have an entry point, we can extract the nested jar files (if any)
		List<File> extractedTempLibraries = extractContainedLibraries(jarFileUrl);
		List<URL> classpaths = new ArrayList<>();
		ClassLoader userCodeClassLoader = JobWithJars.buildUserCodeClassLoader(getAllLibraries(jarFileUrl, extractedTempLibraries), classpaths, parent);

		// load the entry point class
		return loadMainClass(entryPointClassName, userCodeClassLoader);
	}

	/**
	 * Create instance of {@link Program} from sprcific main class.
	 *
	 * @param mainClass The given main class.
	 * @return The result instance of Program
	 * @throws Exception Any exception thrown by this method signals create Program fail.
	 */
	public static Program createProgram(Class<?> mainClass) throws Exception {
		// if the entry point is a program, instantiate the class and get the plan
		if (Program.class.isAssignableFrom(mainClass)) {
			try {
				return InstantiationUtil.instantiate(mainClass.asSubclass(Program.class), Program.class);
			} catch (Exception e) {
				// validate that the class has a main method at least.
				// the main method possibly instantiates the program properly
				if (!hasMainMethod(mainClass)) {
					throw new Exception("The given program class implements the " +
						Program.class.getName() + " interface, but cannot be instantiated. " +
						"It also declares no main(String[]) method as alternative entry point", e);
				}
			} catch (Throwable t) {
				throw new Exception("Error while trying to instantiate program class.", t);
			}
		} else if (hasMainMethod(mainClass)) {
			return null;
		} else {
			throw new Exception("The given program class neither has a main(String[]) method, nor does it implement the " +
				Program.class.getName() + " interface.");
		}

		return null;
	}

	public static void checkJarFile(URL jarfile) throws Exception {
		try {
			JobWithJars.checkJarFile(jarfile);
		}
		catch (IOException e) {
			throw new Exception(e.getMessage());
		}
		catch (Throwable t) {
			throw new Exception("Cannot access jar file" + (t.getMessage() == null ? "." : ": " + t.getMessage()), t);
		}
	}

	/**
	 * Get entry point class name from the specific jar file.
	 *
	 * @param jarFile The given jar file.
	 * @return The result class name.
	 * @throws Exception Any exception thrown by this method signals get class name fail.
	 */
	public static String getEntryPointClassNameFromJar(URL jarFile) throws Exception {
		JarFile jar;
		Manifest manifest;
		String className;

		// Open jar file
		try {
			jar = new JarFile(new File(jarFile.toURI()));
		} catch (URISyntaxException use) {
			throw new Exception("Invalid file path '" + jarFile.getPath() + "'", use);
		} catch (IOException ioex) {
			throw new Exception("Error while opening jar file '" + jarFile.getPath() + "'. "
				+ ioex.getMessage(), ioex);
		}

		// jar file must be closed at the end
		try {
			// Read from jar manifest
			try {
				manifest = jar.getManifest();
			} catch (IOException ioex) {
				throw new Exception("The Manifest in the jar file could not be accessed '"
					+ jarFile.getPath() + "'. " + ioex.getMessage(), ioex);
			}

			if (manifest == null) {
				throw new Exception("No manifest found in jar file '" + jarFile.getPath() + "'. The manifest is need to point to the program's main class.");
			}

			Attributes attributes = manifest.getMainAttributes();

			// check for a "program-class" entry first
			className = attributes.getValue(MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS);
			if (className != null) {
				return className;
			}

			// check for a main class
			className = attributes.getValue(MANIFEST_ATTRIBUTE_MAIN_CLASS);
			if (className != null) {
				return className;
			} else {
				throw new Exception("Neither a '" + MANIFEST_ATTRIBUTE_MAIN_CLASS + "', nor a '" +
					MANIFEST_ATTRIBUTE_ASSEMBLER_CLASS + "' entry was found in the jar file.");
			}
		}
		finally {
			try {
				jar.close();
			} catch (Throwable t) {
				throw new Exception("Could not close the JAR file: " + t.getMessage(), t);
			}
		}
	}

	/**
	 * Takes all JAR files that are contained in this program's JAR file and extracts them
	 * to the system's temp directory.
	 *
	 * @return The file names of the extracted temporary files.
	 * @throws Exception Thrown, if the extraction process failed.
	 */
	public static List<File> extractContainedLibraries(URL jarFile) throws Exception {

		Random rnd = new Random();

		JarFile jar = null;
		try {
			jar = new JarFile(new File(jarFile.toURI()));
			final List<JarEntry> containedJarFileEntries = new ArrayList<JarEntry>();

			Enumeration<JarEntry> entries = jar.entries();
			while (entries.hasMoreElements()) {
				JarEntry entry = entries.nextElement();
				String name = entry.getName();

				if (name.length() > 8 && name.startsWith("lib/") && name.endsWith(".jar")) {
					containedJarFileEntries.add(entry);
				}
			}

			if (containedJarFileEntries.isEmpty()) {
				return Collections.emptyList();
			}
			else {
				// go over all contained jar files
				final List<File> extractedTempLibraries = new ArrayList<File>(containedJarFileEntries.size());
				final byte[] buffer = new byte[4096];

				boolean incomplete = true;

				try {
					for (int i = 0; i < containedJarFileEntries.size(); i++) {
						final JarEntry entry = containedJarFileEntries.get(i);
						String name = entry.getName();
						name = name.replace(File.separatorChar, '_');

						File tempFile;
						try {
							tempFile = File.createTempFile(rnd.nextInt(Integer.MAX_VALUE) + "_", name);
							tempFile.deleteOnExit();
						}
						catch (IOException e) {
							throw new Exception(
								"An I/O error occurred while creating temporary file to extract nested library '" +
									entry.getName() + "'.", e);
						}

						extractedTempLibraries.add(tempFile);

						// copy the temp file contents to a temporary File
						OutputStream out = null;
						InputStream in = null;
						try {

							out = new FileOutputStream(tempFile);
							in = new BufferedInputStream(jar.getInputStream(entry));

							int numRead = 0;
							while ((numRead = in.read(buffer)) != -1) {
								out.write(buffer, 0, numRead);
							}
						}
						catch (IOException e) {
							throw new Exception("An I/O error occurred while extracting nested library '"
								+ entry.getName() + "' to temporary file '" + tempFile.getAbsolutePath() + "'.");
						}
						finally {
							if (out != null) {
								out.close();
							}
							if (in != null) {
								in.close();
							}
						}
					}

					incomplete = false;
				}
				finally {
					if (incomplete) {
						deleteExtractedLibraries(extractedTempLibraries);
					}
				}

				return extractedTempLibraries;
			}
		}
		catch (Throwable t) {
			throw new Exception("Unknown I/O error while extracting contained jar files.", t);
		}
		finally {
			if (jar != null) {
				try {
					jar.close();
				} catch (Throwable t) {}
			}
		}
	}

	public static void deleteExtractedLibraries(List<File> tempLibraries) {
		for (File f : tempLibraries) {
			f.delete();
		}
	}

	/**
	 * Returns all provided libraries needed to run the program.
	 */
	public static List<URL> getAllLibraries(URL jarFile, List<File> extractedTempLibraries) {
		List<URL> libs = new ArrayList<URL>(extractedTempLibraries.size() + 1);

		if (jarFile != null) {
			libs.add(jarFile);
		}
		for (File tmpLib : extractedTempLibraries) {
			try {
				libs.add(tmpLib.getAbsoluteFile().toURI().toURL());
			}
			catch (MalformedURLException e) {
				throw new RuntimeException("URL is invalid. This should not happen.", e);
			}
		}

		return libs;
	}

	public static Class<?> loadMainClass(String className, ClassLoader cl) throws Exception {
		ClassLoader contextCl = null;
		try {
			contextCl = Thread.currentThread().getContextClassLoader();
			Thread.currentThread().setContextClassLoader(cl);
			return Class.forName(className, false, cl);
		}
		catch (ClassNotFoundException e) {
			throw new Exception("The program's entry point class '" + className
				+ "' was not found in the jar file.", e);
		}
		catch (ExceptionInInitializerError e) {
			throw new Exception("The program's entry point class '" + className
				+ "' threw an error during initialization.", e);
		}
		catch (LinkageError e) {
			throw new Exception("The program's entry point class '" + className
				+ "' could not be loaded due to a linkage failure.", e);
		}
		catch (Throwable t) {
			throw new Exception("The program's entry point class '" + className
				+ "' caused an exception during initialization: " + t.getMessage(), t);
		} finally {
			if (contextCl != null) {
				Thread.currentThread().setContextClassLoader(contextCl);
			}
		}
	}

	public static boolean hasMainMethod(Class<?> entryClass) {
		Method mainMethod;
		try {
			mainMethod = entryClass.getMethod("main", String[].class);
		} catch (NoSuchMethodException e) {
			return false;
		}
		catch (Throwable t) {
			throw new RuntimeException("Could not look up the main(String[]) method from the class " +
				entryClass.getName() + ": " + t.getMessage(), t);
		}

		return Modifier.isStatic(mainMethod.getModifiers()) && Modifier.isPublic(mainMethod.getModifiers());
	}

	/**
	 * Returns the description provided by the Program class. This
	 * may contain a description of the plan itself and its arguments.
	 *
	 * @return The description of the PactProgram's input parameters.
	 * @throws Exception
	 *         This invocation is thrown if the Program can't be properly loaded. Causes
	 *         may be a missing / wrong class or manifest files.
	 */
	public static String getDescription(Class<?> mainClass, Program program) throws Exception {
		if (ProgramDescription.class.isAssignableFrom(mainClass)) {

			ProgramDescription descr;
			if (program != null) {
				descr = (ProgramDescription) program;
			} else {
				try {
					descr =  InstantiationUtil.instantiate(
						mainClass.asSubclass(ProgramDescription.class), ProgramDescription.class);
				} catch (Throwable t) {
					return null;
				}
			}

			try {
				return descr.getDescription();
			}
			catch (Throwable t) {
				throw new Exception("Error while getting the program description" +
					(t.getMessage() == null ? "." : ": " + t.getMessage()), t);
			}

		} else {
			return null;
		}
	}
}
