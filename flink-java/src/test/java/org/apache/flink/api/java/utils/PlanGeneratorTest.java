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

package org.apache.flink.api.java.utils;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSink;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link PlanGenerator}.
 */
public class PlanGeneratorTest {

	@Test
	public void testGenerate() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(10);

		DataSink<?> sink = env
				.fromElements(1, 3, 5)
				.map((MapFunction<Integer, String>) value -> String.valueOf(value + 1))
				.writeAsText("/tmp/csv");

		PlanGenerator generator = new PlanGenerator(
				Collections.singletonList(sink),
				env.getConfig(),
				env.getParallelism(),
				Collections.emptyList(),
				"test");
		Plan plan = generator.generate();
		assertEquals(1, plan.getDataSinks().size());
		assertEquals(10, plan.getDefaultParallelism());
		assertEquals(env.getConfig(), plan.getExecutionConfig());
		assertEquals("test", plan.getJobName());
	}
}
