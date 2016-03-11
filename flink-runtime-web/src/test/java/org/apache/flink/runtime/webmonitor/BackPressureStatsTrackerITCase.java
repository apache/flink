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
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Option;
import scala.concurrent.duration.FiniteDuration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.AllVerticesRunning;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.ExecutionGraphFound;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.RequestExecutionGraph;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.WaitForAllVerticesToBeRunning;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Simple back pressured task test.
 */
public class BackPressureStatsTrackerITCase extends TestLogger {

	private static NetworkBufferPool networkBufferPool;
	private static ActorSystem testActorSystem;

	/** Shared as static variable with the test task. */
	private static BufferPool testBufferPool;

	@BeforeClass
	public static void setup() {
		testActorSystem = AkkaUtils.createLocalActorSystem(new Configuration());
		networkBufferPool = new NetworkBufferPool(100, 8192, MemoryType.HEAP);
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(testActorSystem);
		networkBufferPool.destroy();
	}

	/**
	 * Tests a simple fake-back pressured task. Back pressure is assumed when
	 * sampled stack traces are in blocking buffer requests.
	 */
	@Test
	public void testBackPressuredProducer() throws Exception {
		new JavaTestKit(testActorSystem) {{
			final FiniteDuration deadline = new FiniteDuration(60, TimeUnit.SECONDS);

			// The JobGraph
			final JobGraph jobGraph = new JobGraph(new ExecutionConfig());
			final int parallelism = 4;

			final JobVertex task = new JobVertex("Task");
			task.setInvokableClass(BackPressuredTask.class);
			task.setParallelism(parallelism);

			jobGraph.addVertex(task);

			ActorGateway jobManger = null;
			ActorGateway taskManager = null;

			//
			// 1) Consume all buffers at first (no buffers for the test task)
			//
			testBufferPool = networkBufferPool.createBufferPool(1, false);
			final List<Buffer> buffers = new ArrayList<>();
			while (true) {
				Buffer buffer = testBufferPool.requestBuffer();
				if (buffer != null) {
					buffers.add(buffer);
				} else {
					break;
				}
			}

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

							// Verify back pressure (clean up interval can be ignored)
							BackPressureStatsTracker statsTracker = new BackPressureStatsTracker(
									coordinator,
									100 * 1000,
									20,
									new FiniteDuration(10, TimeUnit.MILLISECONDS));

							int numAttempts = 10;

							int nextSampleId = 0;

							// Verify that all tasks are back pressured. This
							// can fail if the task takes longer to request
							// the buffer.
							for (int attempt = 0; attempt < numAttempts; attempt++) {
								try {
									OperatorBackPressureStats stats = triggerStatsSample(statsTracker, vertex);

									assertEquals(nextSampleId + attempt, stats.getSampleId());
									assertEquals(parallelism, stats.getNumberOfSubTasks());
									assertEquals(1.0, stats.getMaxBackPressureRatio(), 0.0);

									for (int i = 0; i < parallelism; i++) {
										assertEquals(1.0, stats.getBackPressureRatio(i), 0.0);
									}

									nextSampleId = stats.getSampleId() + 1;

									break;
								} catch (Throwable t) {
									if (attempt == numAttempts - 1) {
										throw t;
									} else {
										Thread.sleep(500);
									}
								}
							}

							//
							// 2) Release all buffers and let the tasks grab one
							//
							for (Buffer buf : buffers) {
								buf.recycle();
							}

							// Wait for all buffers to be available. The tasks
							// grab them and then immediately release them.
							while (testBufferPool.getNumberOfAvailableMemorySegments() < 100) {
								Thread.sleep(100);
							}

							// Verify that no task is back pressured any more.
							for (int attempt = 0; attempt < numAttempts; attempt++) {
								try {
									OperatorBackPressureStats stats = triggerStatsSample(statsTracker, vertex);

									assertEquals(nextSampleId + attempt, stats.getSampleId());
									assertEquals(parallelism, stats.getNumberOfSubTasks());

									// Verify that no task is back pressured
									for (int i = 0; i < parallelism; i++) {
										assertEquals(0.0, stats.getBackPressureRatio(i), 0.0);
									}

									break;
								} catch (Throwable t) {
									if (attempt == numAttempts - 1) {
										throw t;
									} else {
										Thread.sleep(500);
									}
								}
							}

							// Shut down
							jm.tell(new TestingJobManagerMessages.NotifyWhenJobRemoved(jobGraph.getJobID()), testActor);

							// Cancel job
							jm.tell(new JobManagerMessages.CancelJob(jobGraph.getJobID()));

							// Response to removal notification
							expectMsgEquals(true);

							//
							// 3) Trigger stats for archived job
							//
							statsTracker.invalidateOperatorStatsCache();
							assertFalse("Unexpected trigger", statsTracker.triggerStackTraceSample(vertex));

						} catch (Exception e) {
							e.printStackTrace();
							fail(e.getMessage());
						}
					}
				};
			} finally {
				TestingUtils.stopActor(jobManger);
				TestingUtils.stopActor(taskManager);

				for (Buffer buf : buffers) {
					buf.recycle();
				}

				testBufferPool.lazyDestroy();
			}
		}};
	}

	/**
	 * Triggers a new stats sample.
	 */
	private OperatorBackPressureStats triggerStatsSample(
			BackPressureStatsTracker statsTracker,
			ExecutionJobVertex vertex) throws InterruptedException {

		statsTracker.invalidateOperatorStatsCache();
		assertTrue("Failed to trigger", statsTracker.triggerStackTraceSample(vertex));

		// Sleep minimum duration
		Thread.sleep(20 * 10);

		Option<OperatorBackPressureStats> stats;

		// Get the stats
		while ((stats = statsTracker.getOperatorBackPressureStats(vertex)).isEmpty()) {
			Thread.sleep(10);
		}

		return stats.get();
	}

	/**
	 * A back pressured producer sharing a {@link BufferPool} with the
	 * test driver.
	 */
	public static class BackPressuredTask extends AbstractInvokable {

		@Override
		public void invoke() throws Exception {
			while (true) {
				Buffer buffer = testBufferPool.requestBufferBlocking();
				// Got a buffer, yay!
				buffer.recycle();

				new CountDownLatch(1).await();
			}
		}
	}
}
