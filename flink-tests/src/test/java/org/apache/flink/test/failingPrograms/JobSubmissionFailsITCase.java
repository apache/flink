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

package org.apache.flink.test.failingPrograms;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.Tasks;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class JobSubmissionFailsITCase {
	
	private static final int NUM_SLOTS = 20;
	
	private static ForkableFlinkMiniCluster cluster;
	private static JobGraph workingJobGraph;

	@BeforeClass
	public static void setup() {
		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 4);
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 2);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, NUM_SLOTS / 2);
			
			cluster = new ForkableFlinkMiniCluster(config);

			cluster.start();
			
			final JobVertex jobVertex = new JobVertex("Working job vertex.");
			jobVertex.setInvokableClass(Tasks.NoOpInvokable.class);
			workingJobGraph = new JobGraph("Working testing job", new ExecutionConfig(), jobVertex);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@AfterClass
	public static void teardown() {
		try {
			cluster.shutdown();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------

	private boolean detached;

	public JobSubmissionFailsITCase(boolean detached) {
		this.detached = detached;
	}

	@Parameterized.Parameters(name = "Detached mode = {0}")
	public static Collection<Boolean[]> executionModes(){
		return Arrays.asList(new Boolean[]{false},
				new Boolean[]{true});
	}

	// --------------------------------------------------------------------------------------------
	
	private JobExecutionResult submitJob(JobGraph jobGraph) throws Exception {
		if (detached) {
			cluster.submitJobDetached(jobGraph);
			return null;
		}
		else {
			return cluster.submitJobAndWait(jobGraph, false, TestingUtils.TESTING_DURATION());
		}
	}

	@Test
	public void testExceptionInInitializeOnMaster() {
		try {
			final JobVertex failingJobVertex = new FailingJobVertex("Failing job vertex");
			failingJobVertex.setInvokableClass(Tasks.NoOpInvokable.class);

			final JobGraph failingJobGraph = new JobGraph("Failing testing job", new ExecutionConfig(), failingJobVertex);

			try {
				submitJob(failingJobGraph);
				fail("Expected JobExecutionException.");
			}
			catch (JobExecutionException e) {
				assertEquals("Test exception.", e.getCause().getMessage());
			}
			catch (Throwable t) {
				t.printStackTrace();
				fail("Caught wrong exception of type " + t.getClass() + ".");
			}

			cluster.submitJobAndWait(workingJobGraph, false);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSubmitEmptyJobGraph() {
		try {
			final JobGraph jobGraph = new JobGraph("Testing job", new ExecutionConfig());
	
			try {
				submitJob(jobGraph);
				fail("Expected JobSubmissionException.");
			}
			catch (JobSubmissionException e) {
				assertTrue(e.getMessage() != null && e.getMessage().contains("empty"));
			}
			catch (Throwable t) {
				t.printStackTrace();
				fail("Caught wrong exception of type " + t.getClass() + ".");
			}
	
			cluster.submitJobAndWait(workingJobGraph, false);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testSubmitNullJobGraph() {
		try {
			try {
				submitJob(null);
				fail("Expected JobSubmissionException.");
			}
			catch (NullPointerException e) {
				// yo!
			}
			catch (Throwable t) {
				t.printStackTrace();
				fail("Caught wrong exception of type " + t.getClass() + ".");
			}

			cluster.submitJobAndWait(workingJobGraph, false);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// --------------------------------------------------------------------------------------------
	
	public static class FailingJobVertex extends JobVertex {
		private static final long serialVersionUID = -6365291240199412135L;

		public FailingJobVertex(final String msg) {
			super(msg);
		}

		@Override
		public void initializeOnMaster(ClassLoader loader) throws Exception {
			throw new Exception("Test exception.");
		}
	}
}
