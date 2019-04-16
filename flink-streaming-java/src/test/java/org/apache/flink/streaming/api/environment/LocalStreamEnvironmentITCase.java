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

package org.apache.flink.streaming.api.environment;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobListener;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link LocalStreamEnvironment}.
 */
@SuppressWarnings("serial")
public class LocalStreamEnvironmentITCase extends TestLogger {

	/**
	 * Test test verifies that the execution environment can be used to execute a
	 * single job with multiple slots.
	 */
	@Test
	public void testRunIsolatedJob() throws Exception {
		LocalStreamEnvironment env = new LocalStreamEnvironment();
		assertEquals(1, env.getParallelism());

		addSmallBoundedJob(env, 3);
		env.execute();
	}

	/**
	 * Test test verifies that the execution environment can be used to execute multiple
	 * bounded streaming jobs after one another.
	 */
	@Test
	public void testMultipleJobsAfterAnother() throws Exception {
		LocalStreamEnvironment env = new LocalStreamEnvironment();

		addSmallBoundedJob(env, 3);
		env.execute();

		addSmallBoundedJob(env, 5);
		env.execute();
	}

	@Test
	public void testLocalEnvironmentWithJobListener() throws Exception {
		LocalStreamEnvironment env = new LocalStreamEnvironment();
		TestJobListener testJobListener = new TestJobListener();
		env.addJobListener(testJobListener);
		env.fromElements(1, 2, 3).print();
		env.execute();

		assertEquals(1, testJobListener.jobSubmissionCount);
		assertEquals(1, testJobListener.jobExecutedCount);
		assertEquals(0, testJobListener.jobCanceledCount);
	}

	// ------------------------------------------------------------------------

	private static void addSmallBoundedJob(StreamExecutionEnvironment env, int parallelism) {
		DataStream<Long> stream = env.generateSequence(1, 100).setParallelism(parallelism);

		stream
				.filter(ignored -> false).setParallelism(parallelism)
					.startNewChain()
					.print().setParallelism(parallelism);
	}

	private static class TestJobListener implements JobListener {

		int jobSubmissionCount = 0;
		int jobExecutedCount = 0;
		int jobCanceledCount = 0;

		@Override
		public void onJobSubmitted(JobID jobId) {
			jobSubmissionCount++;
		}

		@Override
		public void onJobExecuted(JobExecutionResult jobResult) {
			jobExecutedCount++;
		}

		@Override
		public void onJobCanceled(JobID jobId, String savepointPath) {

		}
	}

}
