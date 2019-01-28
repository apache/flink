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

package org.apache.flink.streaming.util;

import org.apache.commons.lang3.tuple.ImmutablePair;

import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Compiler based on Java Compiler API (JCA).
 */
public class JCACompiler {

	private static JCACompiler instance = new JCACompiler();

	/**
	 * The root path of the source/binary code.
	 */
	private static final String codeRoot = "codegen";

	/**
	 * A cache containing all compiled classes.
	 * The structure is: class name -> list of (code, class)
	 */
	private Map<String, List<ImmutablePair<String, Class<?>>>> clazzCache = new HashMap<>();

	private JCACompiler() {
	}

	public static JCACompiler getInstance() {
		return instance;
	}

	/**
	 * Retrieve the class file path, given the source file path.
	 *
	 * @param srcPath
	 * @return
	 */
	private String getClassFilePath(String srcPath) {
		File srcFile = new File(srcPath);
		String srcName = srcFile.getName();
		int idx = srcName.lastIndexOf('.');
		if (idx == -1) {
			throw new IllegalArgumentException(srcPath + " is not a valid java source file path");
		}
		String className = srcName.substring(0, idx) + ".class";
		return new File(srcFile.getParentFile(), className).getAbsolutePath();
	}

	/**
	 * Get the class name, given either the source file, or the class file path.
	 *
	 * @param filePath
	 * @return
	 */
	private String getClassName(String filePath) {
		File file = new File(filePath);
		String name = file.getName();
		int idx = name.lastIndexOf(".");
		if (idx == -1) {
			throw new IllegalArgumentException(filePath + " is not a valid java source/class file path");
		}
		return name.substring(0, idx);
	}

	/**
	 * Load the class, given its class file path.
	 *
	 * @param classPath
	 * @return
	 */
	private Class<?> loadClass(String classPath) {
		try {
			File parentDir = new File(classPath).getParentFile();
			URL[] urls = new URL[]{ parentDir.toURI().toURL() };
			ClassLoader cl = new URLClassLoader(urls);
			return cl.loadClass(getClassName(classPath));
		} catch (Exception e) {
			String msg = "Failed to load class " + getClassName(classPath) + " from path: " + new File(classPath).getAbsolutePath();
			throw new RuntimeException(msg, e);
		}
	}

	/**
	 * Compile source files, given their paths.
	 *
	 * @param paths
	 */
	public void compile(String ...paths) {
		List<String> pathList = Arrays.stream(paths).map(p -> new File(p).getAbsolutePath()).collect(Collectors.toList());
		String pathStr = String.join(", ", pathList);
		try {
			JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
			StandardJavaFileManager fileManager = compiler.getStandardFileManager(null, null, null);
			Iterable<? extends JavaFileObject> compilationUnits = fileManager.getJavaFileObjectsFromStrings(pathList);
			compiler.getTask(null, fileManager, null, null, null, compilationUnits).call();
			fileManager.close();
		} catch (Exception e) {
			throw new RuntimeException("Failed to compile generated code from: " + pathStr + ", due to " + e.getMessage(), e);
		}
	}

	/**
	 * Write the source file to disk, given its class name and source code.
	 * If the source file already exists, it will be overwritten.
	 *
	 * @param name
	 * @param code
	 * @return the path of the source file on disk.
	 */
	private String writeSource(String name, String code) {
		File srcFile = new File(codeRoot, name + ".java");
		try {
			org.apache.commons.io.FileUtils.writeStringToFile(srcFile, code);
		} catch (Throwable e) {
			throw new RuntimeException("Failed to write source file for " + name + ", reason: " + e.getMessage(), e);
		}
		return srcFile.getAbsolutePath();
	}

	/**
	 * Try to find a class in class cache, given its name and code.
	 *
	 * @param name
	 * @param code
	 * @return
	 */
	private Class<?> findClassInCache(String name, String code) {
		List<ImmutablePair<String, Class<?>>> clazzList = clazzCache.get(name);
		if (clazzList == null) {
			return null;
		}
		for (ImmutablePair<String, Class<?>> pair : clazzList) {
			if (pair.left.equals(code)) {
				return pair.right;
			}
		}
		return null;
	}

	/**
	 * Check if a batch of source files have all been compiled, and inserted into the cache.
	 * @param names
	 * @param sources
	 * @return
	 */
	private boolean allCompiled(List<String> names, List<String> sources) {
		if (names.size() != sources.size()) {
			throw new IllegalArgumentException("Source file names and code are not of equal size.");
		}

		for (int i = 0; i < names.size(); i++) {
			if (findClassInCache(names.get(i), sources.get(i)) == null) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Compile a number of source files in batch.
	 * This will be much faster than compiling the files individually.
	 * @param names
	 * @param sources
	 */
	public void compileSourceInBatch(List<String> names, List<String> sources) {
		if (allCompiled(names, sources)) {
			return;
		}

		synchronized (clazzCache) {
			if (!allCompiled(names, sources)) {
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
	 *
	 * @param name
	 * @param code
	 * @return
	 */
	public Class<?> getCodeClass(String name, String code) {
		Class<?> ret = findClassInCache(name, code);
		if (ret != null) {
			// The class is in the cache.
			return ret;
		}

		// get the list of all classes with the same name.
		List<ImmutablePair<String, Class<?>>> clazzList = clazzCache.get(name);
		if (clazzList == null) {
			synchronized (clazzCache) {
				clazzList = clazzCache.get(name);
				if (clazzList == null) {
					clazzList = new ArrayList<>();
					clazzCache.put(name, clazzList);
				}
			}
		}

		synchronized (clazzList) {
			ret = findClassInCache(name, code);
			if (ret == null) {
				// write source file
				String srcPath = writeSource(name, code);

				// compile source
				compile(srcPath);

				// load class
				ret = loadClass(getClassFilePath(srcPath));

				// insert class to cache
				clazzList.add(new ImmutablePair<>(code, ret));
			}
		}

		return ret;
	}
}
