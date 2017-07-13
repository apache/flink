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

package org.apache.flink.test.iterative;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.test.util.JavaProgramTestBase;

import java.util.List;

/**
 * Test iterator with an all-reduce.
 */
public class IterationWithAllReducerITCase extends JavaProgramTestBase {
	private static final String EXPECTED = "1\n";

	@Override
	protected void testProgram() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);

		DataSet<String> initialInput = env.fromElements("1", "1", "1", "1", "1", "1", "1", "1");

		IterativeDataSet<String> iteration = initialInput.iterate(5).name("Loop");

		DataSet<String> sumReduce = iteration.reduce(new ReduceFunction<String>(){
			@Override
			public String reduce(String value1, String value2) throws Exception {
				return value1;
			}
		}).name("Compute sum (Reduce)");

		List<String> result = iteration.closeWith(sumReduce).collect();

		compareResultAsText(result, EXPECTED);
	}
}
