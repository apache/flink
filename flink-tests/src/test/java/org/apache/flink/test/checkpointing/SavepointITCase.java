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

package org.apache.flink.test.checkpointing;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV1;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.messages.JobManagerMessages.CancelJob;
import org.apache.flink.runtime.messages.JobManagerMessages.DisposeSavepoint;
import org.apache.flink.runtime.messages.JobManagerMessages.TriggerSavepoint;
import org.apache.flink.runtime.messages.JobManagerMessages.TriggerSavepointSuccess;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.RequestSavepoint;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.ResponseSavepoint;
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages.ResponseSubmitTaskListener;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.runtime.messages.JobManagerMessages.getDisposeSavepointSuccess;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration test for triggering and resuming from savepoints.
 */
public class SavepointITCase extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(SavepointITCase.class);

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	/**
	 * Tests that it is possible to submit a job, trigger a savepoint, and
	 * later restart the job on a new cluster. The savepoint is written to
	 * a file.
	 *
	 * <ol>
	 * <li>Submit job, wait for some checkpoints to complete</li>
	 * <li>Trigger savepoint and verify that savepoint has been created</li>
	 * <li>Shut down the cluster, re-submit the job from the savepoint,
	 * verify that the initial state has been reset, and
	 * all tasks are running again</li>
	 * <li>Cancel job, dispose the savepoint, and verify that everything
	 * has been cleaned up</li>
	 * </ol>
	 */
	@Test
	public void testTriggerSavepointAndResume() throws Exception {
		// Config
		int numTaskManagers = 2;
		int numSlotsPerTaskManager = 2;
		int parallelism = numTaskManagers * numSlotsPerTaskManager;

		// Test deadline
		final Deadline deadline = new FiniteDuration(5, TimeUnit.MINUTES).fromNow();

		// The number of checkpoints to complete before triggering the savepoint
		final int numberOfCompletedCheckpoints = 2;
		final int checkpointingInterval = 100;

		// Temporary directory for file state backend
		final File tmpDir = folder.newFolder();

		LOG.info("Created temporary directory: " + tmpDir + ".");

		TestingCluster flink = null;

		try {
			// Create a test actor system
			ActorSystem testActorSystem = AkkaUtils.createDefaultActorSystem();

			// Flink configuration
			final Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTaskManagers);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlotsPerTaskManager);

			final File checkpointDir = new File(tmpDir, "checkpoints");
			final File savepointDir = new File(tmpDir, "savepoints");

			if (!checkpointDir.mkdir() || !savepointDir.mkdirs()) {
				fail("Test setup failed: failed to create temporary directories.");
			}

			LOG.info("Created temporary checkpoint directory: " + checkpointDir + ".");
			LOG.info("Created temporary savepoint directory: " + savepointDir + ".");

			config.setString(ConfigConstants.STATE_BACKEND, "filesystem");
			config.setString(FsStateBackendFactory.CHECKPOINT_DIRECTORY_URI_CONF_KEY,
				checkpointDir.toURI().toString());
			config.setString(FsStateBackendFactory.MEMORY_THRESHOLD_CONF_KEY, "0");
			config.setString(ConfigConstants.SAVEPOINT_DIRECTORY_KEY,
				savepointDir.toURI().toString());

			LOG.info("Flink configuration: " + config + ".");

			// Start Flink
			flink = new TestingCluster(config);
			flink.start();

			// Retrieve the job manager
			ActorGateway jobManager = Await.result(
				flink.leaderGateway().future(),
				deadline.timeLeft());

			// Submit the job
			final JobGraph jobGraph = createJobGraph(parallelism, 0, 1000, checkpointingInterval);
			final JobID jobId = jobGraph.getJobID();

			// Wait for the source to be notified about the expected number
			// of completed checkpoints
			StatefulCounter.resetForTest();

			LOG.info("Submitting job " + jobGraph.getJobID() + " in detached mode.");

			flink.submitJobDetached(jobGraph);

			LOG.info("Waiting for " + numberOfCompletedCheckpoints + " checkpoint complete notifications.");

			// Wait...
			StatefulCounter.awaitCompletedCheckpoints(parallelism, numberOfCompletedCheckpoints, deadline.timeLeft().toMillis());

			LOG.info("Received all " + numberOfCompletedCheckpoints +
				" checkpoint complete notifications.");

			// ...and then trigger the savepoint
			LOG.info("Triggering a savepoint.");

			Future<Object> savepointPathFuture = jobManager.ask(
				new TriggerSavepoint(jobId, Option.<String>empty()), deadline.timeLeft());

			final String savepointPath = ((TriggerSavepointSuccess) Await
				.result(savepointPathFuture, deadline.timeLeft())).savepointPath();
			LOG.info("Retrieved savepoint path: " + savepointPath + ".");

			// Only one savepoint should exist
			File[] files = savepointDir.listFiles();
			if (files != null) {
				assertEquals("Savepoint not created in expected directory", 1, files.length);
			} else {
				fail("Savepoint not created in expected directory");
			}

			// Retrieve the savepoint from the testing job manager
			LOG.info("Requesting the savepoint.");
			Future<Object> savepointFuture = jobManager.ask(new RequestSavepoint(savepointPath), deadline.timeLeft());

			SavepointV1 savepoint = (SavepointV1) ((ResponseSavepoint) Await.result(
				savepointFuture, deadline.timeLeft())).savepoint();
			LOG.info("Retrieved savepoint: " + savepointPath + ".");

			// Shut down the Flink cluster (thereby canceling the job)
			LOG.info("Shutting down Flink cluster.");
			flink.shutdown();
			flink.awaitTermination();

			// - Verification START -------------------------------------------

			// Only one checkpoint of the savepoint should exist
			// We currently have the following directory layout: checkpointDir/jobId/chk-ID
			files = checkpointDir.listFiles();
			assertNotNull("Checkpoint directory empty", files);
			assertEquals("Checkpoints directory cleaned up, but needed for savepoint.", 1, files.length);
			assertEquals("No job-specific base directory", jobGraph.getJobID().toString(), files[0].getName());

			// Only one savepoint should exist
			files = savepointDir.listFiles();
			assertNotNull("Savepoint directory empty", files);
			assertEquals("No savepoint found in savepoint directory", 1, files.length);

			// - Verification END ---------------------------------------------

			// Restart the cluster
			LOG.info("Restarting Flink cluster.");
			flink.start();

			// Retrieve the job manager
			LOG.info("Retrieving JobManager.");
			jobManager = Await.result(flink.leaderGateway().future(), deadline.timeLeft());
			LOG.info("JobManager: " + jobManager + ".");

			// Reset for restore
			StatefulCounter.resetForTest();

			// Gather all task deployment descriptors
			final Throwable[] error = new Throwable[1];
			final TestingCluster finalFlink = flink;
			final Multimap<JobVertexID, TaskDeploymentDescriptor> tdds = HashMultimap.create();
			new JavaTestKit(testActorSystem) {{

				new Within(deadline.timeLeft()) {
					@Override
					protected void run() {
						try {
							// Register to all submit task messages for job
							for (ActorRef taskManager : finalFlink.getTaskManagersAsJava()) {
								taskManager.tell(new TestingTaskManagerMessages
									.RegisterSubmitTaskListener(jobId), getTestActor());
							}

							// Set the savepoint path
							jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath));

							LOG.info("Resubmitting job " + jobGraph.getJobID() + " with " +
								"savepoint path " + savepointPath + " in detached mode.");

							// Submit the job
							finalFlink.submitJobDetached(jobGraph);

							int numTasks = 0;
							for (JobVertex jobVertex : jobGraph.getVertices()) {
								numTasks += jobVertex.getParallelism();
							}

							// Gather the task deployment descriptors
							LOG.info("Gathering " + numTasks + " submitted " +
								"TaskDeploymentDescriptor instances.");

							for (int i = 0; i < numTasks; i++) {
								ResponseSubmitTaskListener resp = (ResponseSubmitTaskListener)
									expectMsgAnyClassOf(getRemainingTime(),
										ResponseSubmitTaskListener.class);

								TaskDeploymentDescriptor tdd = resp.tdd();

								LOG.info("Received: " + tdd.toString() + ".");

								TaskInformation taskInformation = tdd
									.getSerializedTaskInformation()
									.deserializeValue(getClass().getClassLoader());

								tdds.put(taskInformation.getJobVertexId(), tdd);
							}
						} catch (Throwable t) {
							error[0] = t;
						}
					}
				};
			}};

			// - Verification START -------------------------------------------

			String errMsg = "Error during gathering of TaskDeploymentDescriptors";
			assertNull(errMsg, error[0]);

			// Verify that all tasks, which are part of the savepoint
			// have a matching task deployment descriptor.
			for (TaskState taskState : savepoint.getTaskStates()) {
				Collection<TaskDeploymentDescriptor> taskTdds = tdds.get(taskState.getJobVertexID());

				errMsg = "Missing task for savepoint state for operator "
					+ taskState.getJobVertexID() + ".";
				assertTrue(errMsg, taskTdds.size() > 0);

				assertEquals(taskState.getNumberCollectedStates(), taskTdds.size());

				for (TaskDeploymentDescriptor tdd : taskTdds) {
					SubtaskState subtaskState = taskState.getState(tdd.getSubtaskIndex());

					assertNotNull(subtaskState);

					errMsg = "Initial operator state mismatch.";
					assertEquals(errMsg, subtaskState.getLegacyOperatorState(),
						tdd.getTaskStateHandles().getLegacyOperatorState());
				}
			}

			// Await state is restored
			StatefulCounter.awaitStateRestoredFromCheckpoint(deadline.timeLeft().toMillis());

			// Await some progress after restore
			StatefulCounter.awaitCompletedCheckpoints(parallelism, numberOfCompletedCheckpoints, deadline.timeLeft().toMillis());

			// - Verification END ---------------------------------------------

			LOG.info("Cancelling job " + jobId + ".");
			jobManager.tell(new CancelJob(jobId));

			LOG.info("Disposing savepoint " + savepointPath + ".");
			Future<Object> disposeFuture = jobManager.ask(new DisposeSavepoint(savepointPath), deadline.timeLeft());

			errMsg = "Failed to dispose savepoint " + savepointPath + ".";
			Object resp = Await.result(disposeFuture, deadline.timeLeft());
			assertTrue(errMsg, resp.getClass() == getDisposeSavepointSuccess().getClass());

			// - Verification START -------------------------------------------

			// The checkpoint files
			List<File> checkpointFiles = new ArrayList<>();

			for (TaskState stateForTaskGroup : savepoint.getTaskStates()) {
				for (SubtaskState subtaskState : stateForTaskGroup.getStates()) {
					ChainedStateHandle<StreamStateHandle> streamTaskState = subtaskState.getLegacyOperatorState();

					for (int i = 0; i < streamTaskState.getLength(); i++) {
						if (streamTaskState.get(i) != null) {
							FileStateHandle fileStateHandle = (FileStateHandle) streamTaskState.get(i);
							checkpointFiles.add(new File(fileStateHandle.getFilePath().toUri()));
						}
					}
				}
			}

			// The checkpoint of the savepoint should have been discarded
			for (File f : checkpointFiles) {
				errMsg = "Checkpoint file " + f + " not cleaned up properly.";
				assertFalse(errMsg, f.exists());
			}

			if (checkpointFiles.size() > 0) {
				File parent = checkpointFiles.get(0).getParentFile();
				errMsg = "Checkpoint parent directory " + parent + " not cleaned up properly.";
				assertFalse(errMsg, parent.exists());
			}

			// All savepoints should have been cleaned up
			errMsg = "Savepoints directory not cleaned up properly: " +
				Arrays.toString(savepointDir.listFiles()) + ".";
			assertEquals(errMsg, 0, savepointDir.listFiles().length);

			// - Verification END ---------------------------------------------
		} finally {
			if (flink != null) {
				flink.shutdown();
			}

			if (tmpDir != null) {
				FileUtils.deleteDirectory(tmpDir);
			}
		}
	}

	@Test
	public void testSubmitWithUnknownSavepointPath() throws Exception {
		// Config
		int numTaskManagers = 1;
		int numSlotsPerTaskManager = 1;
		int parallelism = numTaskManagers * numSlotsPerTaskManager;

		// Test deadline
		final Deadline deadline = new FiniteDuration(5, TimeUnit.MINUTES).fromNow();

		final File tmpDir = CommonTestUtils.createTempDirectory();
		final File savepointDir = new File(tmpDir, "savepoints");

		TestingCluster flink = null;

		try {
			// Flink configuration
			final Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTaskManagers);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlotsPerTaskManager);
			config.setString(ConfigConstants.SAVEPOINT_DIRECTORY_KEY,
				savepointDir.toURI().toString());

			LOG.info("Flink configuration: " + config + ".");

			// Start Flink
			flink = new TestingCluster(config);
			LOG.info("Starting Flink cluster.");
			flink.start();

			// Retrieve the job manager
			LOG.info("Retrieving JobManager.");
			ActorGateway jobManager = Await.result(
				flink.leaderGateway().future(),
				deadline.timeLeft());
			LOG.info("JobManager: " + jobManager + ".");

			// High value to ensure timeouts if restarted.
			int numberOfRetries = 1000;
			// Submit the job
			// Long delay to ensure that the test times out if the job
			// manager tries to restart the job.
			final JobGraph jobGraph = createJobGraph(parallelism, numberOfRetries, 3600000, 1000);

			// Set non-existing savepoint path
			jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath("unknown path"));
			assertEquals("unknown path", jobGraph.getSavepointRestoreSettings().getRestorePath());

			LOG.info("Submitting job " + jobGraph.getJobID() + " in detached mode.");

			try {
				flink.submitJobAndWait(jobGraph, false);
			} catch (Exception e) {
				assertEquals(JobExecutionException.class, e.getClass());
				assertEquals(IllegalArgumentException.class, e.getCause().getClass());
			}
		} finally {
			if (flink != null) {
				flink.shutdown();
			}
		}
	}

	// ------------------------------------------------------------------------
	// Test program
	// ------------------------------------------------------------------------

	/**
	 * Creates a streaming JobGraph from the StreamEnvironment.
	 */
	private JobGraph createJobGraph(
		int parallelism,
		int numberOfRetries,
		long restartDelay,
		int checkpointingInterval) {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.enableCheckpointing(checkpointingInterval);
		env.disableOperatorChaining();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(numberOfRetries, restartDelay));
		env.getConfig().disableSysoutLogging();

		DataStream<Integer> stream = env
			.addSource(new InfiniteTestSource())
			.shuffle()
			.map(new StatefulCounter());

		stream.addSink(new DiscardingSink<Integer>());

		return env.getStreamGraph().getJobGraph();
	}

	private static class InfiniteTestSource implements SourceFunction<Integer> {

		private static final long serialVersionUID = 1L;
		private volatile boolean running = true;

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (running) {
				ctx.collect(1);
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	private static class StatefulCounter
		extends RichMapFunction<Integer, Integer>
		implements Checkpointed<byte[]>, CheckpointListener {

		private static final Object checkpointLock = new Object();
		private static int numCompleteCalls;
		private static int numRestoreCalls;
		private static boolean restoredFromCheckpoint;

		private static final long serialVersionUID = 7317800376639115920L;
		private byte[] data;

		@Override
		public void open(Configuration parameters) throws Exception {
			if (data == null) {
				// We need this to be large, because we want to test with files
				Random rand = new Random(getRuntimeContext().getIndexOfThisSubtask());
				data = new byte[FsStateBackend.DEFAULT_FILE_STATE_THRESHOLD + 1];
				rand.nextBytes(data);
			}
		}

		@Override
		public Integer map(Integer value) throws Exception {
			for (int i = 0; i < data.length; i++) {
				data[i] += 1;
			}
			return value;
		}

		@Override
		public byte[] snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return data;
		}

		@Override
		public void restoreState(byte[] data) throws Exception {
			this.data = data;

			synchronized (checkpointLock) {
				if (++numRestoreCalls == getRuntimeContext().getNumberOfParallelSubtasks()) {
					restoredFromCheckpoint = true;
					checkpointLock.notifyAll();
				}
			}
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			synchronized (checkpointLock) {
				numCompleteCalls++;
				checkpointLock.notifyAll();
			}
		}

		// --------------------------------------------------------------------

		static void resetForTest() {
			synchronized (checkpointLock) {
				numCompleteCalls = 0;
				numRestoreCalls = 0;
				restoredFromCheckpoint = false;
			}
		}

		static void awaitCompletedCheckpoints(
				int parallelism,
				int expectedNumberOfCompletedCheckpoints,
				long timeoutMillis) throws InterruptedException, TimeoutException {

			long deadline = System.nanoTime() + timeoutMillis * 1_000_000;

			synchronized (checkpointLock) {
				// One completion notification per parallel subtask
				int expectedNumber = parallelism * expectedNumberOfCompletedCheckpoints;
				while (numCompleteCalls < expectedNumber && System.nanoTime() <= deadline) {
					checkpointLock.wait();
				}

				if (numCompleteCalls < expectedNumber) {
					throw new TimeoutException("Did not complete " + expectedNumberOfCompletedCheckpoints +
						" within timeout of " + timeoutMillis + " millis.");
				}
			}
		}

		static void awaitStateRestoredFromCheckpoint(long timeoutMillis) throws InterruptedException, TimeoutException {
			long deadline = System.nanoTime() + timeoutMillis * 1_000_000;

			synchronized (checkpointLock) {
				while (!restoredFromCheckpoint && System.currentTimeMillis() <= deadline) {
					checkpointLock.wait();
				}

				if (!restoredFromCheckpoint) {
					throw new TimeoutException("Did not restore from checkpoint within timeout of " + timeoutMillis + " millis.");
				}
			}
		}
	}

}
