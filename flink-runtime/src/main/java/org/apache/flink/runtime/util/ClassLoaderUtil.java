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

import org.apache.flink.util.ExceptionUtils;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.jar.JarFile;

/**
 * Utilities for information with respect to class loaders, specifically class loaders for
 * the dynamic loading of user defined classes.
 */
public final class ClassLoaderUtil {

	/**
	 * Gets information about URL class loaders. The returned info string contains all URLs of the
	 * class loader. For file URLs, it contains in addition whether the referenced file exists,
	 * is a valid JAR file, or is a directory.
	 * 
	 * <p>NOTE: This method makes a best effort to provide information about the classloader, and
	 * never throws an exception.</p>
	 * 
	 * @param loader The classloader to get the info string for.
	 * @return The classloader information string.
	 */
	public static String getUserCodeClassLoaderInfo(ClassLoader loader) {
		if (loader instanceof URLClassLoader) {
			URLClassLoader cl = (URLClassLoader) loader;
			
			try {
				StringBuilder bld = new StringBuilder();
				
				if (cl == ClassLoader.getSystemClassLoader()) {
					bld.append("System ClassLoader: ");
				}
				else {
					bld.append("URL ClassLoader:");
				}
				
				for (URL url : cl.getURLs()) {
					bld.append("\n    ");
					if (url == null) {
						bld.append("(null)");
					}
					else if ("file".equals(url.getProtocol())) {
						String filePath = url.getPath();
						File fileFile = new File(filePath);
						
						bld.append("file: '").append(filePath).append('\'');
						
						if (fileFile.exists()) {
							if (fileFile.isDirectory()) {
								bld.append(" (directory)");
							}
							else {
								JarFile jar = null;
								try {
									jar = new JarFile(filePath);
									bld.append(" (valid JAR)");
								}
								catch (Exception e) {
									bld.append(" (invalid JAR: ").append(e.getMessage()).append(')');
								}
								finally {
									if (jar != null) {
										jar.close();
									}
								}
							}
						}
						else {
							bld.append(" (missing)");
						}
					}
					else {
						bld.append("url: ").append(url);
					}
				}
				
				return bld.toString();
			}
			catch (Throwable t) {
				return "Cannot access classloader info due to an exception.\n"
						+ ExceptionUtils.stringifyException(t);
			}
		}
		else {
			return "No user code ClassLoader";
		}
	}

	/**
	 * Checks, whether the class that was not found in the given exception, can be resolved through
	 * the given class loader.
	 * 
	 * @param cnfe The ClassNotFoundException that defines the name of the class.
	 * @param cl The class loader to use for the class resolution.
	 * @return True, if the class can be resolved with the given class loader, false if not. 
	 */
	public static boolean validateClassLoadable(ClassNotFoundException cnfe, ClassLoader cl) {
		try {
			String className = cnfe.getMessage();
			Class.forName(className, false, cl);
			return true;
		}
		catch (ClassNotFoundException e) {
			return false;
		}
		catch (Exception e) {
			return false;
		}
	}

	/**
	 * Private constructor to prevent instantiation.
	 */
	private ClassLoaderUtil() {
		throw new RuntimeException();
	}
}
