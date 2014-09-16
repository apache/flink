/**
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

package org.apache.flink.test.javaApiOperators.lambdas;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.test.util.JavaProgramTestBase;

public class CrossITCase extends JavaProgramTestBase {

	private static final String EXPECTED_RESULT = "2,hello not\n" +
			"3,what's not\n" +
			"3,up not\n" +
			"2,hello much\n" +
			"3,what's much\n" +
			"3,up much\n" +
			"3,hello really\n" +
			"4,what's really\n" +
			"4,up really";

	private String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void testProgram() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<Integer, String>> left = env.fromElements(
				new Tuple2<Integer, String>(1, "hello"),
				new Tuple2<Integer, String>(2, "what's"),
				new Tuple2<Integer, String>(2, "up")
				);
		DataSet<Tuple2<Integer, String>> right = env.fromElements(
				new Tuple2<Integer, String>(1, "not"),
				new Tuple2<Integer, String>(1, "much"),
				new Tuple2<Integer, String>(2, "really")
				);
		DataSet<Tuple2<Integer,String>> joined = left.cross(right)
				.with((t,s) -> new Tuple2<Integer, String> (t.f0 + s.f0, t.f1 + " " + s.f1));
		joined.writeAsCsv(resultPath);
		env.execute();
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED_RESULT, resultPath);
	}
}
