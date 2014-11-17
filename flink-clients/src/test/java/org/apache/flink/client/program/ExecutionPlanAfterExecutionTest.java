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

package org.apache.flink.client.program;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.junit.Test;

import static org.junit.Assert.*;

public class ExecutionPlanAfterExecutionTest {

	@Test
	public void testExecuteAfterGetExecutionPlan() throws Exception {
		Plan.test();
	}
}

class Plan {
	public static void test() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		DataSet<Integer> baseSet = env.fromElements(1, 2);

		DataSet<Integer> result = baseSet.map(new MapFunction<Integer, Integer>() {
			@Override public Integer map(Integer value) throws Exception { return value * 2; }
		});
		result.print();

		try {
			env.getExecutionPlan();
			env.execute();
		} catch (Exception e) {
			fail("Cannot run both #getExecutionPlan and #execute.");
		}

	}
}
