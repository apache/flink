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
 * IT cases for lambda map functions.
 */
public class MapITCase extends JavaProgramTestBase {

	private static class Trade {

		public String v;

		public Trade(String v) {
			this.v = v;
		}

		@Override
		public String toString() {
			return v;
		}
	}

	private static final String EXPECTED_RESULT = "22\n" +
			"22\n" +
			"23\n" +
			"24\n";

	private String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Integer> stringDs = env.fromElements(11, 12, 13, 14);
		DataSet<String> mappedDs = stringDs
			.map(Object::toString)
			.map (s -> s.replace("1", "2"))
			.map(Trade::new)
			.map(Trade::toString);
		mappedDs.writeAsText(resultPath);
		env.execute();
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED_RESULT, resultPath);
	}
}
