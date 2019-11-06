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

package org.apache.flink.tests.util;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * General test utilities.
 */
public class TestUtils {

	private static final String END_TO_END_TESTS_MODULE = "/flink-end-to-end-tests/";

	public static final Path END_TO_END_MODULE_PATH = getEnd2EndModuleDir();

	/**
	 * Get the path of FLINK_ROOT_DIR/flink-end-to-end-tests based on the class loading.
	 */
	public static Path getEnd2EndModuleDir() {
		URL url = TestUtils.class.getResource("TestUtils.class");
		assert url != null;
		String path = url.getPath();
		assert path.contains(END_TO_END_TESTS_MODULE);
		int index = path.indexOf(END_TO_END_TESTS_MODULE);
		return Paths.get(path.substring(0, index), END_TO_END_TESTS_MODULE);
	}

	/**
	 * Generate a test directory path under the FLINK_ROOT_DIR/flink-end-to-end-tests/target, each time with a
	 * different directory.
	 *
	 * @return the test directory to put the temporary end-to-end test data.
	 */
	public static Path getTestDataDir() {
		return getEnd2EndModuleDir().resolve("target").resolve("temp-test-directory-" + System.nanoTime());
	}
}
