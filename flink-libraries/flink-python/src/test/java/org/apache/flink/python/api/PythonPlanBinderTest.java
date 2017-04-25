/**
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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

	private static boolean isPython2Supported() throws IOException {
		Process process = null;

		try {
			process = Runtime.getRuntime().exec("python");
			return true;
		} catch (IOException ex) {
			return false;
		} finally {
			if (process != null) {
				PythonStreamer.destroyProcess(process);
			}
		}
	}

	private static boolean isPython3Supported() throws IOException {
		Process process = null;

		try {
			process = Runtime.getRuntime().exec("python3");
			return true;
		} catch (IOException ex) {
			return false;
		} finally {
			if (process != null) {
				PythonStreamer.destroyProcess(process);
			}
		}
	}

	@Override
	protected void testProgram() throws Exception {
		String utils = findUtilsFile();
		if (isPython2Supported()) {
			for (String file : findTestFiles()) {
				Configuration configuration = new Configuration();
				config.setString(PythonOptions.PYTHON_BINARY_PATH, "python");
				new PythonPlanBinder(configuration).runPlan(new String[]{file, utils});
			}
		}
		if (isPython3Supported()) {
			for (String file : findTestFiles()) {
				Configuration configuration = new Configuration();
				config.setString(PythonOptions.PYTHON_BINARY_PATH, "python3");
				new PythonPlanBinder(configuration).runPlan(new String[]{file, utils});
			}
		}
	}
}
