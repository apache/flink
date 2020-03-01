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
 *
 */

package org.apache.flink.optimizer.plandump;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.ExecutionPlanUtil;
import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringContains.containsString;

/**
 * Tests for {@link ExecutionPlanUtil}. We have to test this here in flink-optimizer because the
 * util only works when {@link ExecutionPlanJSONGenerator} is available, which is in
 * flink-optimizer.
 */
public class ExecutionPlanUtilTest {

	@Test
	public void executionPlanCanBeRetrieved() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(8);

		env
				.readCsvFile("file:///will/never/be/executed")
				.types(String.class, Double.class)
				.name("sourceThatWillNotRun")
				.map((in) -> in)
				.returns(new TypeHint<Tuple2<String, Double>>() {})
				.name("theMap")
				.writeAsText("file:///will/not/be/executed")
				.name("sinkThatWillNotRun");

		Plan plan = env.createProgramPlan();
		String executionPlanAsJSON = ExecutionPlanUtil.getExecutionPlanAsJSON(plan);

		assertThat(executionPlanAsJSON, containsString("sourceThatWillNotRun"));
		assertThat(executionPlanAsJSON, containsString("sinkThatWillNotRun"));
		assertThat(executionPlanAsJSON, containsString("theMap"));
	}
}
