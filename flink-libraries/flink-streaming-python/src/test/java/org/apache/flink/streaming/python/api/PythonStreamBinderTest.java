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

import java.io.File;
import java.io.FileNotFoundException;

/**
 * A single unit-test class for the streaming python API. Internally, it executes all the python
 * unit-test using the 'run_all_tests.py' located under
 * 'flink-libraries/flink-streaming-python/src/test/python/org/apache/flink/streaming/python/api'
 */
public class PythonStreamBinderTest extends StreamingProgramTestBase {
	private static final String DEFAULT_PYTHON_SCRIPT_NAME = "run_all_tests.py";
	private static final String FLINK_PYTHON_RLTV_PATH = "flink-libraries/flink-streaming-python";
	private static final String PATH_TO_STREAMING_TESTS = "src/test/python/org/apache/flink/streaming/python/api";

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
		main(new String[]{});
	}

	private static String[] prepareDefaultArgs() throws Exception {
		File testFullPath = findStreamTestFile(DEFAULT_PYTHON_SCRIPT_NAME);
		File[] filesInTestPath = testFullPath.getParentFile().listFiles((dir, name) ->
			name.startsWith("test_") ||
				new File(dir.getAbsolutePath() + File.separator + name).isDirectory());

		String[] args = new String[filesInTestPath.length + 1];
		args[0] = testFullPath.getAbsolutePath();

		for (int index = 0; index < filesInTestPath.length; index++) {
			args[index + 1] = filesInTestPath[index].getCanonicalPath();
		}

		return args;
	}

	private static File findStreamTestFile(String name) throws Exception {
		if (new File(name).exists()) {
			return new File(name);
		}
		FileSystem fs = FileSystem.getLocalFileSystem();
		String workingDir = fs.getWorkingDirectory().getPath();
		if (!workingDir.endsWith(FLINK_PYTHON_RLTV_PATH)) {
			workingDir += File.separator + FLINK_PYTHON_RLTV_PATH;
		}
		FileStatus[] status = fs.listStatus(
			new Path(workingDir + File.separator + PATH_TO_STREAMING_TESTS));
		for (FileStatus f : status) {
			String fileName = f.getPath().getName();
			if (fileName.equals(name)) {
				return new File(f.getPath().getPath());
			}
		}
		throw new FileNotFoundException();
	}
}
