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

package org.apache.flink.yarn.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;

/**
 * Utility methods for YARN tests.
 */
public class TestUtils {
	public static File getTestJarPath(String fileName) throws FileNotFoundException {
		File f = new File("target/programs/" + fileName);
		if (!f.exists()) {
			throw new FileNotFoundException("Test jar " + f.getPath() + " not present. Invoke tests using maven "
				+ "or build the jar using 'mvn process-test-classes' in flink-yarn-tests");
		}
		return f;
	}

	/**
	 * Locate a file or directory.
	 */
	public static File findFile(String startAt, FilenameFilter fnf) {
		File root = new File(startAt);
		String[] files = root.list();
		if (files == null) {
			return null;
		}
		for (String file : files) {
			File f = new File(startAt + File.separator + file);
			if (f.isDirectory()) {
				File r = findFile(f.getAbsolutePath(), fnf);
				if (r != null) {
					return r;
				}
			} else if (fnf.accept(f.getParentFile(), f.getName())) {
				return f;
			}
		}
		return null;
	}

	/**
	 * Filename filter which finds the test jar for the given name.
	 */
	public static class TestJarFinder implements FilenameFilter {

		private final String jarName;

		public TestJarFinder(final String jarName) {
			this.jarName = jarName;
		}

		@Override
		public boolean accept(File dir, String name) {
			return name.startsWith(jarName) && name.endsWith("-tests.jar") &&
				dir.getAbsolutePath().contains(File.separator + jarName + File.separator);
		}
	}

	/**
	 * Filter to find root dir of the flink-yarn dist.
	 */
	public static class RootDirFilenameFilter implements FilenameFilter {
		@Override
		public boolean accept(File dir, String name) {
			return name.startsWith("flink-dist") && name.endsWith(".jar") && dir.toString().contains("/lib");
		}
	}
}
