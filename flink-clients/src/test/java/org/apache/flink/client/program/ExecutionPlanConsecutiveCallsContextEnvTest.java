/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.client.program;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.Serializable;
import java.util.ArrayList;

import static junit.framework.TestCase.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that verifies consecutive calls to {@link ContextEnvironment#getExecutionPlan()} do not cause any exceptions.
 * {@link ContextEnvironment#getExecutionPlan()} must not modify the state of the plan
 */
@SuppressWarnings("serial") @RunWith(PowerMockRunner.class) @PrepareForTest(ClusterClient.class)
public class ExecutionPlanConsecutiveCallsContextEnvTest extends TestLogger implements Serializable {

	@Test public void testExecuteAfterGetExecutionPlanContextEnvironment() {
		final OptimizedPlan optimizedPlan = mock(OptimizedPlan.class);
		final ClusterClient clusterClient = mock(ClusterClient.class);

		PowerMockito.mockStatic(ClusterClient.class);
		when(ClusterClient.getOptimizedPlan(any(), (Plan) any(), anyInt())).thenReturn(optimizedPlan);
		ExecutionEnvironment env = new ContextEnvironment(clusterClient, new ArrayList<>(), new ArrayList<>(), null, null);
		env.getConfig().disableSysoutLogging();

		DataSet<Integer> baseSet = env.fromElements(1, 2);

		DataSet<Integer> result = baseSet.map((MapFunction<Integer, Integer>) value -> value * 2);
		result.output(new DiscardingOutputFormat<>());

		try {
			env.getExecutionPlan();
			env.getExecutionPlan();
		} catch (Exception e) {
			e.printStackTrace();
			fail("Consecutive #getExecutionPlan calls caused an exception.");
		}
	}
}
