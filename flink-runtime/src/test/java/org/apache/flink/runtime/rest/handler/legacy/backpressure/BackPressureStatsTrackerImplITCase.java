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

package org.apache.flink.runtime.rest.handler.legacy.backpressure;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.akka.AkkaJobManagerGateway;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobClient;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.FiniteDuration;

import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.AllVerticesRunning;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.ExecutionGraphFound;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.RequestExecutionGraph;
import static org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.WaitForAllVerticesToBeRunning;

/**
 * Simple back pressured task test.
 */
public class BackPressureStatsTrackerImplITCase extends TestLogger {

	private static NetworkBufferPool networkBufferPool;
	private static ActorSystem testActorSystem;

	/** Shared as static variable with the test task. */
	private static BufferPool testBufferPool;

	@BeforeClass
	public static void setup() {
		testActorSystem = AkkaUtils.createLocalActorSystem(new Configuration());
		networkBufferPool = new NetworkBufferPool(100, 8192);
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(testActorSystem);
		networkBufferPool.destroyAllBufferPools();
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
			final JobGraph jobGraph = new JobGraph();
			final int parallelism = 4;

			final JobVertex task = new JobVertex("Task");
			task.setInvokableClass(BackPressuredTask.class);
			task.setParallelism(parallelism);

			jobGraph.addVertex(task);

			final Configuration config = new Configuration();

			final HighAvailabilityServices highAvailabilityServices = HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(
				config,
				TestingUtils.defaultExecutor());

			ActorGateway jobManger = null;
			ActorGateway taskManager = null;

			//
			// 1) Consume all buffers at first (no buffers for the test task)
			//
			testBufferPool = networkBufferPool.createBufferPool(1, Integer.MAX_VALUE);
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
				jobManger = TestingUtils.createJobManager(
					testActorSystem,
					TestingUtils.defaultExecutor(),
					TestingUtils.defaultExecutor(),
					config,
					highAvailabilityServices);

				config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, parallelism);

				taskManager = TestingUtils.createTaskManager(
					testActorSystem,
					highAvailabilityServices,
					config,
					true,
					true);

				final ActorGateway jm = jobManger;

				new Within(deadline) {
					@Override
					protected void run() {
						try {
							ActorGateway testActor = new AkkaActorGateway(getTestActor(), HighAvailabilityServices.DEFAULT_LEADER_ID);

							// Submit the job and wait until it is running
							JobClient.submitJobDetached(
									new AkkaJobManagerGateway(jm),
									config,
									jobGraph,
									Time.milliseconds(deadline.toMillis()),
									ClassLoader.getSystemClassLoader());

							jm.tell(new WaitForAllVerticesToBeRunning(jobGraph.getJobID()), testActor);

							expectMsgEquals(new AllVerticesRunning(jobGraph.getJobID()));

							// Get the ExecutionGraph
							jm.tell(new RequestExecutionGraph(jobGraph.getJobID()), testActor);

							ExecutionGraphFound executionGraphResponse =
									expectMsgClass(ExecutionGraphFound.class);

							ExecutionGraph executionGraph = (ExecutionGraph) executionGraphResponse.executionGraph();
							ExecutionJobVertex vertex = executionGraph.getJobVertex(task.getID());

							StackTraceSampleCoordinator coordinator = new StackTraceSampleCoordinator(
									testActorSystem.dispatcher(), 60000);

							// Verify back pressure (clean up interval can be ignored)
							BackPressureStatsTrackerImpl statsTracker = new BackPressureStatsTrackerImpl(
								coordinator,
								100 * 1000,
								20,
								Integer.MAX_VALUE,
								Time.milliseconds(10L));

							int numAttempts = 10;

							int nextSampleId = 0;

							// Verify that all tasks are back pressured. This
							// can fail if the task takes longer to request
							// the buffer.
							for (int attempt = 0; attempt < numAttempts; attempt++) {
								try {
									OperatorBackPressureStats stats = triggerStatsSample(statsTracker, vertex);

									Assert.assertEquals(nextSampleId + attempt, stats.getSampleId());
									Assert.assertEquals(parallelism, stats.getNumberOfSubTasks());
									Assert.assertEquals(1.0, stats.getMaxBackPressureRatio(), 0.0);

									for (int i = 0; i < parallelism; i++) {
										Assert.assertEquals(1.0, stats.getBackPressureRatio(i), 0.0);
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
								buf.recycleBuffer();
								Assert.assertTrue(buf.isRecycled());
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

									Assert.assertEquals(nextSampleId + attempt, stats.getSampleId());
									Assert.assertEquals(parallelism, stats.getNumberOfSubTasks());

									// Verify that no task is back pressured
									for (int i = 0; i < parallelism; i++) {
										Assert.assertEquals(0.0, stats.getBackPressureRatio(i), 0.0);
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
							Assert.assertFalse("Unexpected trigger", statsTracker.triggerStackTraceSample(vertex));

						} catch (Exception e) {
							e.printStackTrace();
							Assert.fail(e.getMessage());
						}
					}
				};
			} finally {
				TestingUtils.stopActor(jobManger);
				TestingUtils.stopActor(taskManager);

				highAvailabilityServices.closeAndCleanupAllData();

				testBufferPool.lazyDestroy();
			}
		}};
	}

	/**
	 * Triggers a new stats sample.
	 */
	private OperatorBackPressureStats triggerStatsSample(
			BackPressureStatsTrackerImpl statsTracker,
			ExecutionJobVertex vertex) throws InterruptedException {

		statsTracker.invalidateOperatorStatsCache();
		Assert.assertTrue("Failed to trigger", statsTracker.triggerStackTraceSample(vertex));

		// Sleep minimum duration
		Thread.sleep(20 * 10);

		Optional<OperatorBackPressureStats> stats;

		// Get the stats
		while (!(stats = statsTracker.getOperatorBackPressureStats(vertex)).isPresent()) {
			Thread.sleep(10);
		}

		return stats.get();
	}

	/**
	 * A back pressured producer sharing a {@link BufferPool} with the
	 * test driver.
	 */
	public static class BackPressuredTask extends AbstractInvokable {

		public BackPressuredTask(Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() throws Exception {
			while (true) {
				final BufferBuilder bufferBuilder = testBufferPool.requestBufferBuilderBlocking();
				// Got a buffer, yay!
				BufferBuilderTestUtils.buildSingleBuffer(bufferBuilder).recycleBuffer();

				new CountDownLatch(1).await();
			}
		}
	}
}
