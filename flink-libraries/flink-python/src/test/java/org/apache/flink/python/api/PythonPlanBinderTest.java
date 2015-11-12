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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import static org.apache.flink.python.api.PythonPlanBinder.ARGUMENT_PYTHON_2;
import static org.apache.flink.python.api.PythonPlanBinder.ARGUMENT_PYTHON_3;
import org.junit.Test;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PythonPlanBinderTest {
	private static final Logger LOG = LoggerFactory.getLogger(PythonPlanBinder.class);
	
	private static boolean python2Supported = true;
	private static boolean python3Supported = true;
	private static List<String> TEST_FILES;
	
	@BeforeClass
	public static void setup() throws Exception {
		findTestFiles();
		checkPythonSupport();
	}
	
	private static void findTestFiles() throws Exception {
		TEST_FILES = new ArrayList();
		FileSystem fs = FileSystem.getLocalFileSystem();
		FileStatus[] status = fs.listStatus(
				new Path(fs.getWorkingDirectory().toString()
						+ "/src/test/python/org/apache/flink/python/api"));
		for (FileStatus f : status) {
			String file = f.getPath().toString();
			if (file.endsWith(".py")) {
				TEST_FILES.add(file);
			}
		}
	}
	
	private static void checkPythonSupport() {	
		try {
			Runtime.getRuntime().exec("python");
		} catch (IOException ex) {
			python2Supported = false;
			LOG.info("No Python 2 runtime detected.");
		}
		try {
			Runtime.getRuntime().exec("python3");
		} catch (IOException ex) {
			python3Supported = false;
			LOG.info("No Python 3 runtime detected.");
		}
	}
	
	@Test
	public void testPython2() throws Exception {
		if (python2Supported) {
			for (String file : TEST_FILES) {
				LOG.info("testing " + file);
				PythonPlanBinder.main(new String[]{ARGUMENT_PYTHON_2, file});
			}
		}
	}
	
	@Test
	public void testPython3() throws Exception {
		if (python3Supported) {
			for (String file : TEST_FILES) {
				LOG.info("testing " + file);
				PythonPlanBinder.main(new String[]{ARGUMENT_PYTHON_3, file});
			}
		}
	}
}
