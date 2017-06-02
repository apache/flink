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

package org.apache.flink.test.api.java.operators.lambdas;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.test.util.JavaProgramTestBase;

/**
 * IT cases for lambda join functions.
 */
public class FlatJoinITCase extends JavaProgramTestBase {

	private static final String EXPECTED_RESULT = "2,what's really\n" +
			"2,up really\n" +
			"1,hello not\n" +
			"1,hello much\n";

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
		DataSet<Tuple2<Integer, String>> joined = left.join(right).where(0).equalTo(0)
				.with((t, s, out) -> out.collect(new Tuple2<Integer, String>(t.f0, t.f1 + " " + s.f1)));
		joined.writeAsCsv(resultPath);
		env.execute();
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED_RESULT, resultPath);
	}
}
