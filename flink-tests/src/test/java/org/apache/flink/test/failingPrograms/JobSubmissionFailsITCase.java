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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
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

	private static ActorSystem system;

	private static JobGraph workingJobGraph;

	@BeforeClass
	public static void setup() {
		system = ActorSystem.create("TestingActorSystem", AkkaUtils.getDefaultAkkaConfig());

		final AbstractJobVertex jobVertex = new AbstractJobVertex("Working job vertex.");
		jobVertex.setInvokableClass(Tasks.NoOpInvokable.class);

		workingJobGraph = new JobGraph("Working testing job", jobVertex);
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(system);
		system = null;
	}

	private boolean detached;

	public JobSubmissionFailsITCase(boolean detached) {
		this.detached = detached;
	}

	@Parameterized.Parameters(name = "Detached mode = {0}")
	public static Collection<Boolean[]> executionModes(){
		return Arrays.asList(new Boolean[]{false},
				new Boolean[]{true});
	}

	private JobExecutionResult submitJob(JobGraph jobGraph, ActorRef jobClient) throws Exception {
		if(detached) {
			JobClient.submitJobDetached(jobGraph, jobClient, TestingUtils.TESTING_DURATION());
			return null;
		} else {
			return JobClient.submitJobAndWait(jobGraph, false, jobClient, TestingUtils.TESTING_DURATION());
		}
	}

	@Test
	public void testExceptionInInitializeOnMaster() {
		new JavaTestKit(system) {{
			final int numSlots = 20;

			final ForkableFlinkMiniCluster cluster =
					ForkableFlinkMiniCluster.startCluster(numSlots/2, 2,
							TestingUtils.TESTING_DURATION().toString());

			final ActorRef jobClient = cluster.getJobClient();

			final AbstractJobVertex failingJobVertex = new FailingJobVertex("Failing job vertex");
			failingJobVertex.setInvokableClass(Tasks.NoOpInvokable.class);

			final JobGraph failingJobGraph = new JobGraph("Failing testing job", failingJobVertex);

			try {
				new Within(TestingUtils.TESTING_DURATION()) {

					@Override
					protected void run() {
						try {
							submitJob(failingJobGraph, jobClient);
							fail("Expected JobExecutionException.");
						} catch (JobExecutionException e) {
							assertEquals("Test exception.", e.getCause().getMessage());
						} catch (Throwable t) {
							fail("Caught wrong exception of type " + t.getClass() + ".");
							t.printStackTrace();
						}

						try {
							JobClient.submitJobAndWait(workingJobGraph, false, jobClient,
									TestingUtils.TESTING_DURATION());
						} catch (Throwable t) {
							fail("Caught unexpected exception " + t.getMessage() + ".");
						}
					}
				};
			} finally {
				cluster.stop();
			}
		}};
	}

	@Test
	public void testSubmitEmptyJobGraph() {
		new JavaTestKit(system) {{
			final int numSlots = 20;

			final ForkableFlinkMiniCluster cluster =
					ForkableFlinkMiniCluster.startCluster(numSlots/2, 2,
							TestingUtils.TESTING_DURATION().toString());

			final ActorRef jobClient = cluster.getJobClient();

			final JobGraph jobGraph = new JobGraph("Testing job");

			try {
				new Within(TestingUtils.TESTING_DURATION()) {

					@Override
					protected void run() {
						try {
							submitJob(jobGraph, jobClient);
							fail("Expected JobSubmissionException.");
						}
						catch (JobSubmissionException e) {
							assertTrue(e.getMessage() != null && e.getMessage().contains("empty"));
						}
						catch (Throwable t) {
							t.printStackTrace();
							fail("Caught wrong exception of type " + t.getClass() + ".");
						}

						try {
							JobClient.submitJobAndWait(workingJobGraph, false, jobClient,
									TestingUtils.TESTING_DURATION());
						} catch (Throwable t) {
							fail("Caught unexpected exception " + t.getMessage() + ".");
						}
					}
				};
			} finally {
				cluster.stop();
			}
		}};
	}

	@Test
	public void testSubmitNullJobGraph() {
		new JavaTestKit(system) {{
			final int numSlots = 20;

			final ForkableFlinkMiniCluster cluster =
					ForkableFlinkMiniCluster.startCluster(numSlots/2, 2,
							TestingUtils.TESTING_DURATION().toString());

			final ActorRef jobClient = cluster.getJobClient();

			try {
				new Within(TestingUtils.TESTING_DURATION()) {

					@Override
					protected void run() {
						try {
							submitJob(null, jobClient);
							fail("Expected JobSubmissionException.");
						} catch (JobSubmissionException e) {
							assertEquals("JobGraph must not be null.", e.getMessage());
						} catch (Throwable t) {
							fail("Caught wrong exception of type " + t.getClass() + ".");
							t.printStackTrace();
						}

						try {
							JobClient.submitJobAndWait(workingJobGraph, false, jobClient,
									TestingUtils.TESTING_DURATION());
						} catch (Throwable t) {
							fail("Caught unexpected exception " + t.getMessage() + ".");
						}
					}
				};
			} finally {
				cluster.stop();
			}
		}};
	}

	public static class FailingJobVertex extends AbstractJobVertex {
		public FailingJobVertex(final String msg) {
			super(msg);
		}

		@Override
		public void initializeOnMaster(ClassLoader loader) throws Exception {
			throw new Exception("Test exception.");
		}
	}
}
