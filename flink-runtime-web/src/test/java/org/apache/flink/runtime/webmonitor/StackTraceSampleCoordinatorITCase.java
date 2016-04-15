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

package org.apache.flink.runtime.webmonitor;

import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.Tasks;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.AllVerticesRunning;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.ExecutionGraphFound;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.RequestExecutionGraph;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.WaitForAllVerticesToBeRunning;
import static org.junit.Assert.fail;

/**
 * Simple stack trace sampling test.
 */
public class StackTraceSampleCoordinatorITCase extends TestLogger {

	private static ActorSystem testActorSystem;

	@BeforeClass
	public static void setup() {
		testActorSystem = AkkaUtils.createLocalActorSystem(new Configuration());
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(testActorSystem);
	}

	/**
	 * Tests that a cleared task is answered with a partial success response.
	 */
	@Test
	public void testTaskClearedWhileSampling() throws Exception {
		new JavaTestKit(testActorSystem) {{
			final FiniteDuration deadline = new FiniteDuration(60, TimeUnit.SECONDS);

			// The JobGraph
			final JobGraph jobGraph = new JobGraph(new ExecutionConfig());
			final int parallelism = 1;

			final JobVertex task = new JobVertex("Task");
			task.setInvokableClass(Tasks.BlockingNoOpInvokable.class);
			task.setParallelism(parallelism);

			jobGraph.addVertex(task);

			ActorGateway jobManger = null;
			ActorGateway taskManager = null;

			try {
				jobManger = TestingUtils.createJobManager(testActorSystem, new Configuration());

				Configuration config = new Configuration();
				config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, parallelism);

				taskManager = TestingUtils.createTaskManager(
						testActorSystem, jobManger, config, true, true);

				final ActorGateway jm = jobManger;

				new Within(deadline) {
					@Override
					protected void run() {
						try {
							ActorGateway testActor = new AkkaActorGateway(getTestActor(), null);

							int maxAttempts = 10;
							int sleepTime = 100;

							for (int i = 0; i < maxAttempts; i++, sleepTime *= 2) {
								// Submit the job and wait until it is running
								JobClient.submitJobDetached(
										jm,
										jobGraph,
										deadline,
										ClassLoader.getSystemClassLoader());

								jm.tell(new WaitForAllVerticesToBeRunning(jobGraph.getJobID()), testActor);

								expectMsgEquals(new AllVerticesRunning(jobGraph.getJobID()));

								// Get the ExecutionGraph
								jm.tell(new RequestExecutionGraph(jobGraph.getJobID()), testActor);
								ExecutionGraphFound executionGraphResponse =
										expectMsgClass(ExecutionGraphFound.class);
								ExecutionGraph executionGraph = executionGraphResponse.executionGraph();
								ExecutionJobVertex vertex = executionGraph.getJobVertex(task.getID());

								StackTraceSampleCoordinator coordinator = new StackTraceSampleCoordinator(
										testActorSystem, 60000);

								Future<?> sampleFuture = coordinator.triggerStackTraceSample(
										vertex.getTaskVertices(),
										// Do this often so we have a good
										// chance of removing the job during
										// sampling.
										Integer.MAX_VALUE,
										new FiniteDuration(10, TimeUnit.MILLISECONDS),
										0);

								// Wait before cancelling so that some samples
								// are actually taken.
								Thread.sleep(sleepTime);

								// Cancel job
								Future<?> removeFuture = jm.ask(
										new TestingJobManagerMessages.NotifyWhenJobRemoved(jobGraph.getJobID()),
										remaining());

								jm.tell(new JobManagerMessages.CancelJob(jobGraph.getJobID()));

								try {
									// Throws Exception on failure
									Await.result(sampleFuture, remaining());

									// OK, we are done. Got the expected
									// partial result.
									break;
								} catch (Throwable t) {
									// We were too fast in cancelling the job.
									// Fall through and retry.
								} finally {
									Await.ready(removeFuture, remaining());
								}
							}
						} catch (Exception e) {
							e.printStackTrace();
							fail(e.getMessage());
						}
					}
				};
			} finally {
				TestingUtils.stopActor(jobManger);
				TestingUtils.stopActor(taskManager);
			}
		}};
	}
}
