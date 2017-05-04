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
package org.apache.flink.streaming.python.api;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.apache.flink.streaming.python.api.PythonStreamBinder;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class PythonStreamBinderTest extends StreamingProgramTestBase {
	final private static String defaultPythonScriptName = "run_all_tests.py";
	final private static String flinkPythonRltvPath = "flink-libraries/flink-streaming-python";
	final private static String pathToStreamingTests = "src/test/python/org/apache/flink/streaming/python/api";

	public PythonStreamBinderTest() {
	}

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			args = prepareDefaultArgs();
		} else {
			args[0] = findStreamTestFile(args[0]).getAbsolutePath();
		}
		PythonStreamBinder.main(args);
	}

	@Override
	public void testProgram() throws Exception {
		this.main(new String[]{});
	}

	private static String[] prepareDefaultArgs() throws Exception {
		File testFullPath = findStreamTestFile(defaultPythonScriptName);
		List<String> filesInTestPath = getFilesInFolder(testFullPath.getParent());

		String[] args = new String[filesInTestPath.size() + 1];
		args[0] = testFullPath.getAbsolutePath();

		for (final ListIterator<String> it = filesInTestPath.listIterator(); it.hasNext();) {
			final String p = it.next();
			args[it.previousIndex() + 1] = p;
		}
		return args;
	}

	private static File findStreamTestFile(String name) throws Exception {
		if (new File(name).exists()) {
			return new File(name);
		}
		FileSystem fs = FileSystem.getLocalFileSystem();
		String workingDir = fs.getWorkingDirectory().getPath();
		if (!workingDir.endsWith(flinkPythonRltvPath)) {
			workingDir += File.separator + flinkPythonRltvPath;
		}
		FileStatus[] status = fs.listStatus(
			new Path( workingDir + File.separator + pathToStreamingTests));
		for (FileStatus f : status) {
			String file_name = f.getPath().getName();
			if (file_name.equals(name)) {
				return new File(f.getPath().getPath());
			}
		}
		throw new FileNotFoundException();
	}

	private static List<String> getFilesInFolder(String path) {
		List<String> results = new ArrayList<>();
		File[] files = new File(path).listFiles();
		if (files != null) {
			for (File file : files) {
				if (file.isDirectory() || file.getName().startsWith("test_")) {
					results.add("." + File.separator + file.getName());
				}
			}
		}
		return results;
	}
}
