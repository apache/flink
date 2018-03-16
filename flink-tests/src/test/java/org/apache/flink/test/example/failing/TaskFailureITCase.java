/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.example.failing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.test.util.MultipleProgramsTestBase;

import org.junit.Assert;

import java.util.List;

/**
 * Tests that both jobs, the failing and the working one, are handled correctly. The first (failing) job must be
 * canceled and the client must report the failure. The second (working) job must finish successfully and compute the
 * correct result.
 *
 */
public class TaskFailureITCase extends JavaProgramTestBase {

	private static final String EXCEPTION_STRING = "This is an expected Test Exception";

	@Override
	protected void testProgram() throws Exception {
		//test failing version
		try {
			executeTask(new FailingTestMapper(), 1);
		} catch (RuntimeException e) { //expected for collection execution
			if (!isCollectionExecution()) {
				Assert.fail();
			}
			// for collection execution, no restarts. So, exception should be appended with 0.
			Assert.assertEquals(EXCEPTION_STRING + ":0", e.getMessage());
		} catch (JobExecutionException e) { //expected for cluster execution
			if (isCollectionExecution()) {
				Assert.fail();
			}
			// for cluster execution, one restart. So, exception should be appended with 1.
			Assert.assertEquals(EXCEPTION_STRING + ":1", e.getCause().getMessage());
		}
		//test correct version
		executeTask(new TestMapper(), 0);
	}

	private void executeTask(MapFunction<Long, Long> mapper, int retries) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(retries, 0));
		List<Long> result = env.generateSequence(1, 9)
				.map(mapper)
				.collect();
		MultipleProgramsTestBase.compareResultAsText(result, "1\n2\n3\n4\n5\n6\n7\n8\n9");
	}

	/**
	 * Working map function.
	 */
	public static class TestMapper implements MapFunction<Long, Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long map(Long value) throws Exception {
			return value;
		}
	}

	/**
	 * Failing map function.
	 */
	public static class FailingTestMapper extends RichMapFunction<Long, Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long map(Long value) throws Exception {
			throw new RuntimeException(EXCEPTION_STRING + ":" + getRuntimeContext().getAttemptNumber());
		}
	}
}
