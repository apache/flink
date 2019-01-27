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

package org.apache.flink.test.api.java.operators.lambdas;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.test.util.JavaProgramTestBase;

/**
 * IT cases for lambda flatmap functions.
 */
public class FlatMapITCase extends JavaProgramTestBase {

	private static final String EXPECTED_RESULT = "bb\n" +
			"bb\n" +
			"bc\n" +
			"bd\n";

	private String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> stringDs = env.fromElements("aa", "ab", "ac", "ad");
		DataSet<String> flatMappedDs = stringDs.flatMap((s, out) -> out.collect(s.replace("a", "b")));
		flatMappedDs.writeAsText(resultPath);
		env.execute();
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED_RESULT, resultPath);
	}
}
