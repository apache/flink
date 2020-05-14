/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.python;

import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.python.util.PythonEnvironmentManagerUtils;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * The base class of all the Python function operators.
 */
public abstract class PythonFunctionOperatorTestBase {

	private static Map<String, String> env;

	@BeforeClass
	public static void setTestEnv() {
		env = new HashMap<>(System.getenv());
		Map<String, String> testEnv = new HashMap<>(env);
		testEnv.put(
			PythonEnvironmentManagerUtils.PYFLINK_UDF_RUNNER_DIR,
			new File("").getAbsolutePath() + File.separator + "bin");
		CommonTestUtils.setEnv(testEnv);
	}

	@AfterClass
	public static void restoreEnv() {
		CommonTestUtils.setEnv(env);
	}
}
