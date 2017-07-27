/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.flink.python.api;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.python.api.streaming.data.PythonStreamer;
import org.apache.flink.test.util.JavaProgramTestBase;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests for the PythonPlanBinder.
 */
public class PythonPlanBinderTest extends JavaProgramTestBase {

	@Override
	protected boolean skipCollectionExecution() {
		return true;
	}

	private static String findUtilsFile() throws Exception {
		FileSystem fs = FileSystem.getLocalFileSystem();
		return fs.getWorkingDirectory().toString()
				+ "/src/test/python/org/apache/flink/python/api/utils/utils.py";
	}

	private static List<String> findTestFiles() throws Exception {
		List<String> files = new ArrayList<>();
		FileSystem fs = FileSystem.getLocalFileSystem();
		FileStatus[] status = fs.listStatus(
				new Path(fs.getWorkingDirectory().toString()
						+ "/src/test/python/org/apache/flink/python/api"));
		for (FileStatus f : status) {
			String file = f.getPath().toString();
			if (file.endsWith(".py")) {
				files.add(file);
			}
		}
		return files;
	}

	/**
	 * Finds the python binary for the given version.
	 *
	 * @param possibleBinaries
	 * 		binaries to test for
	 * @param expectedVersionPrefix
	 * 		expected output prefix for <tt>&lt;binary&gt; -V</tt>, e.g. <tt>"Python 2."</tt>
	 *
	 * @return python binary or <tt>null</tt> if not supported
	 *
	 * @throws IOException
	 * 		if the process to test for the binaries failed to exit properly
	 */
	private static String getPythonPath(String[] possibleBinaries, String expectedVersionPrefix)
		throws IOException {
		Process process = null;
		for (String python : possibleBinaries) {
			try {
				process = new ProcessBuilder(python, "-V").redirectErrorStream(true).start();
				BufferedReader stdInput = new BufferedReader(new
					InputStreamReader(process.getInputStream()));

				if (stdInput.readLine().startsWith(expectedVersionPrefix)) {
					return python;
				}
			} catch (IOException ignored) {
			} finally {
				if (process != null) {
					PythonStreamer.destroyProcess(process);
				}
			}
		}
		return null;
	}

	/**
	 * Finds the binary that executes python2 programs.
	 *
	 * @return python2 binary or <tt>null</tt> if not supported
	 *
	 * @throws IOException
	 * 		if the process to test for the binaries failed to exit properly
	 */
	private static String getPython2Path() throws IOException {
		return getPythonPath(new String[] {"python2", "python"}, "Python 2.");
	}

	/**
	 * Finds the binary that executes python3 programs.
	 *
	 * @return python3 binary or <tt>null</tt> if not supported
	 *
	 * @throws IOException
	 * 		if the process to test for the binaries failed to exit properly
	 */
	private static String getPython3Path() throws IOException {
		return getPythonPath(new String[] {"python3", "python"}, "Python 3.");
	}

	@Override
	protected void testProgram() throws Exception {
		String utils = findUtilsFile();
		String python2 = getPython2Path();
		if (python2 != null) {
			for (String file : findTestFiles()) {
				Configuration configuration = new Configuration();
				configuration.setString(PythonOptions.PYTHON_BINARY_PATH, python2);
				new PythonPlanBinder(configuration).runPlan(new String[]{file, utils});
			}
		}
		String python3 = getPython3Path();
		if (python3 != null) {
			for (String file : findTestFiles()) {
				Configuration configuration = new Configuration();
				configuration.setString(PythonOptions.PYTHON_BINARY_PATH, python3);
				new PythonPlanBinder(configuration).runPlan(new String[]{file, utils});
			}
		}
	}
}
