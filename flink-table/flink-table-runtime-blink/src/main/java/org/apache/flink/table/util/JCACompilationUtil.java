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

package org.apache.flink.table.util;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility for compiling generated code by Java Compiler API (JCA).
 */
public class JCACompilationUtil {

	protected static final Logger LOG = LoggerFactory.getLogger(JCACompilationUtil.class);

	private static JCACompilationUtil instance = new JCACompilationUtil();

	/**
	 * The root directory name for storing generated source files.
	 */
	public static final String CODE_ROOT 	= "codegen";

	public static final File CODE_ROOT_FILE = new File(CODE_ROOT);

	private static final URL CODE_ROOT_URL;

	static {
		try {
			// Note that if the codegen directory does not already exists,
			// the URL can be incorrect, and the class loading may fail.
			if (!CODE_ROOT_FILE.exists()) {
				CODE_ROOT_FILE.mkdirs();
			}
			CODE_ROOT_URL = CODE_ROOT_FILE.toURI().toURL();
		} catch (MalformedURLException e) {
			throw new RuntimeException("Failed to construct code root URL");
		}
	}

	/**
	 * A cache containing the class objects of generated code.
	 * The structure is: class name -> list of (code, class)
	 */
	private Map<String, List<ImmutablePair<String, Class<?>>>> clazzCache = new HashMap<>();

	private JCACompilationUtil() {
	}

	public static JCACompilationUtil getInstance() {
		return instance;
	}

	/**
	 * Retrieve the class file path, given the source file path.
	 * @param srcPath the source file path.
	 * @return the class file path.
	 */
	String getClassFilePath(String srcPath) {
		File srcFile = new File(srcPath);
		File parentFile = srcFile.getParentFile();
		String srcName = srcFile.getName();

		int idx = srcName.lastIndexOf('.');
		if (idx == -1) {
			throw new IllegalArgumentException(srcPath + " is not a valid java source file path");
		}

		String className = srcName.substring(0, idx) + ".class";
		return new File(parentFile, className).getAbsolutePath();
	}

	/**
	 * Get the class name, given either the source file path, or the class file path.
	 * @param filePath full path of the source/class file.
	 * @return the fully qualified name of the class.
	 */
	String getClassName(String filePath) {
		File file = new File(filePath);
		String name = file.getName();
		int idx = name.lastIndexOf(".");
		if (idx == -1) {
			throw new IllegalArgumentException(filePath + " is not a valid java source/class file path");
		}

		StringBuilder className = new StringBuilder(name.substring(0, idx));
		String codeRoot = new File(CODE_ROOT).getAbsolutePath();
		if (!filePath.startsWith(codeRoot)) {
			throw new IllegalArgumentException(filePath + " is not in the root code path");
		}

		File curFile = new File(filePath);
		while (true) {
			curFile = curFile.getParentFile();
			if (!curFile.getAbsolutePath().equals(codeRoot)) {
				className.insert(0, curFile.getName() + ".");
			} else {
				break;
			}
		}
		return className.toString();
	}

	/**
	 * Load the class, given its source file or class file path.
	 * @param classPath path of the source file or class file.
	 * @return the class object for the input file.
	 */
	Class<?> loadClass(String classPath) {
		ClassLoader classLoader = new URLClassLoader(new URL[] {CODE_ROOT_URL}, this.getClass().getClassLoader());
		String className = getClassName(classPath);
		try {
			return classLoader.loadClass(className);
		} catch (ClassNotFoundException e) {
			String msg = "Failed to load class " + getClassName(classPath) + " from path: " + classPath;
			throw new RuntimeException(msg, e);
		}
	}

	/**
	 * Compile source files in batch, given their paths.
	 * @param paths paths of source files to compile.
	 */
	void compile(String ...paths) {
		List<String> pathList = Arrays.stream(paths).map(p -> new File(p).getAbsolutePath()).collect(Collectors.toList());
		String pathStr = String.join(", ", pathList);
		try {
			long start = System.currentTimeMillis();

			JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
			StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
			Iterable<? extends JavaFileObject> compilationUnits = fileManager.getJavaFileObjectsFromStrings(pathList);
			compiler.getTask(null, fileManager, null, null, null, compilationUnits).call();
			fileManager.close();

			long end = System.currentTimeMillis();
			LOG.info("Time for compiling " + paths.length + " files " + pathStr + ": " +  (end - start));
		} catch (Exception e) {
			throw new RuntimeException("Failed to compile generated code from: " + pathStr + ", due to " + e.getMessage(), e);
		}
	}

	/**
	 * Get the path of the source file, given the class name.
	 * @param className the full class name.
	 * @return the path of the source file.
	 */
	String getSourceFilePath(String className) {
		String[] packageSegs = className.split("\\.");
		String shortName = packageSegs[packageSegs.length - 1];
		packageSegs[packageSegs.length - 1] = shortName + ".java";

		return Paths.get(CODE_ROOT_FILE.getAbsolutePath(), packageSegs).toString();
	}

	/**
	 * Write the source file to disk, given its full class name and source code.
	 * If the source file already exists, it will be overwritten.
	 * @param name full name of the class
	 * @param code the code of the class
	 * @return the path of the source file on disk.
	 */
	String writeSource(String name, String code) {
		String srcPath = getSourceFilePath(name);
		File srcFile = new File(srcPath);
		try {
			org.apache.commons.io.FileUtils.writeStringToFile(srcFile, code);
		} catch (Throwable e) {
			throw new RuntimeException("Failed to write source file for " + name + ", the cause is: " + e.getMessage(), e);
		}
		return srcPath;
	}

	/**
	 * Try to find a class in class cache, given the name and code.
	 * @param name the full class name
	 * @return the class object, if any, and null, otherwise.
	 */
	public Class<?> findClassInCache(String name, String code) {
		List<ImmutablePair<String, Class<?>>> clazzList = clazzCache.get(name);
		if (clazzList == null) {
			return null;
		}

		for (int i = 0; i < clazzList.size(); i++) {
			ImmutablePair<String, Class<?>> pair = clazzList.get(i);
			if (pair.left.equals(code)) {
				return pair.right;
			}
		}

		return null;
	}

	/**
	 * Compile a set of classes in batch.
	 * This is used in a Flink task, where a chain of operators are compiled together.
	 * Compiling classes in batch takes shorter time, compared with compiling classes separately.
	 * @param names the class names.
	 * @param sources the class code.
	 */
	public void compileSourcesInBatch(List<String> names, List<String> sources) {
		assert names.size() == sources.size();
		if (names.isEmpty()) {
			return;
		}

		String firstName = names.get(0);
		String firstCode = sources.get(0);
		if (findClassInCache(firstName, firstCode) != null) {
			// Another task has compiled the classes, so we are done.
			return;
		}

		synchronized (clazzCache) {
			if (findClassInCache(firstName, firstCode) == null) {
				// write source files to disk
				String[] sourcePaths = new String[names.size()];
				for (int i = 0; i < names.size(); i++) {
					sourcePaths[i] = writeSource(names.get(i), sources.get(i));
				}

				// compile sources
				compile(sourcePaths);

				// load classes and insert them into cache
				for (int i = 0; i < names.size(); i++) {
					Class<?> clazz = loadClass(getClassFilePath(sourcePaths[i]));
					List<ImmutablePair<String, Class<?>>> clazzList = clazzCache.get(names.get(i));
					if (clazzList == null) {
						clazzList = new ArrayList<>();
						clazzCache.put(names.get(i), clazzList);
					}
					clazzList.add(new ImmutablePair<>(sources.get(i), clazz));
				}
			}
		}
	}

	/**
	 * Given the class name and the code, get the class.
	 * It first tries to find the class in the cache.
	 * If not found in the cache, it compiles the class and insert the result into cache.
	 * @param name the full class name.
	 * @param code the class code.
	 * @return the class object.
	 */
	public Class<?> getCodeClass(String name, String code) {
		Class<?> ret = findClassInCache(name, code);
		if (ret != null) {
			// The class is in the cache.
			return ret;
		}

		synchronized (clazzCache) {
			// the list of all classes with the same name.
			List<ImmutablePair<String, Class<?>>> clazzList = clazzCache.get(name);
			if (clazzList == null) {
				clazzList = new ArrayList<>();
				clazzCache.put(name, clazzList);
			}

			// write source file
			String srcPath = writeSource(name, code);

			// compile source
			compile(srcPath);

			// load class
			ret = loadClass(getClassFilePath(srcPath));

			// insert class to cache
			clazzList.add(new ImmutablePair<>(code, ret));
		}
		return ret;
	}
}
