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

public class End2EndUtil {

	private static final String END_TO_END_TESTS_MODULE = "/flink-end-to-end-tests/";

	public static Path getEnd2EndModuleDir() {
		URL url = End2EndUtil.class.getResource("End2EndUtil.class");
		assert url != null;
		String path = url.getPath();
		assert path.contains(END_TO_END_TESTS_MODULE);
		int index = path.indexOf(END_TO_END_TESTS_MODULE);
		return Paths.get(path.substring(0, index), END_TO_END_TESTS_MODULE);
	}

	public static Path getTestDataDir() {
		return getEnd2EndModuleDir().resolve("target").resolve("temp-test-directory-" + System.nanoTime());
	}
}
