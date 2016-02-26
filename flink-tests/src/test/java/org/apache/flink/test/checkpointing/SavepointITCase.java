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
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.SavepointStoreFactory;
import org.apache.flink.runtime.checkpoint.StateForTask;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.UnrecoverableException;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.JobManagerMessages.CancelJob;
import org.apache.flink.runtime.messages.JobManagerMessages.DisposeSavepoint;
import org.apache.flink.runtime.messages.JobManagerMessages.TriggerSavepoint;
import org.apache.flink.runtime.messages.JobManagerMessages.TriggerSavepointSuccess;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.filesystem.AbstractFileStateHandle;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.NotifyWhenJobRemoved;
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
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.apache.flink.streaming.runtime.tasks.StreamTaskStateList;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.messages.JobManagerMessages.CancellationSuccess;
import static org.apache.flink.runtime.messages.JobManagerMessages.getDisposeSavepointSuccess;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration test for triggering and resuming from savepoints.
 */
public class SavepointITCase extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(SavepointITCase.class);

	/**
	 * Tests that it is possible to submit a job, trigger a savepoint, and
	 * later restart the job on a new cluster. The savepoint is written to
	 * a file.
	 *
	 * <ol>
	 * <li>Submit job, wait for some checkpoints to complete</li>
	 * <li>Trigger savepoint and verify that savepoint has been created</li>
	 * <li>Shut down the cluster, re-submit the job from the savepoint, and
	 * verify that the initial state has been reset</li>
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
		final int numberOfCompletedCheckpoints = 10;

		// Temporary directory for file state backend
		final File tmpDir = CommonTestUtils.createTempDirectory();

		LOG.info("Created temporary directory: " + tmpDir + ".");

		ForkableFlinkMiniCluster flink = null;

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
			config.setString(SavepointStoreFactory.SAVEPOINT_BACKEND_KEY, "filesystem");
			config.setString(SavepointStoreFactory.SAVEPOINT_DIRECTORY_KEY,
					savepointDir.toURI().toString());

			LOG.info("Flink configuration: " + config + ".");

			// Start Flink
			flink = new ForkableFlinkMiniCluster(config);
			LOG.info("Starting Flink cluster.");
			flink.start();

			// Retrieve the job manager
			LOG.info("Retrieving JobManager.");
			ActorGateway jobManager = Await.result(
					flink.leaderGateway().future(),
					deadline.timeLeft());
			LOG.info("JobManager: " + jobManager + ".");

			// Submit the job
			final JobGraph jobGraph = createJobGraph(parallelism, 0, 1000, 1000);
			final JobID jobId = jobGraph.getJobID();

			// Wait for the source to be notified about the expected number
			// of completed checkpoints
			InfiniteTestSource.CheckpointCompleteLatch = new CountDownLatch(
					numberOfCompletedCheckpoints);

			LOG.info("Submitting job " + jobGraph.getJobID() + " in detached mode.");

			flink.submitJobDetached(jobGraph);

			LOG.info("Waiting for " + numberOfCompletedCheckpoints +
					" checkpoint complete notifications.");

			// Wait...
			InfiniteTestSource.CheckpointCompleteLatch.await();

			LOG.info("Received all " + numberOfCompletedCheckpoints +
					" checkpoint complete notifications.");

			// ...and then trigger the savepoint
			LOG.info("Triggering a savepoint.");

			Future<Object> savepointPathFuture = jobManager.ask(
					new TriggerSavepoint(jobId), deadline.timeLeft());

			final String savepointPath = ((TriggerSavepointSuccess) Await
					.result(savepointPathFuture, deadline.timeLeft())).savepointPath();
			LOG.info("Retrieved savepoint path: " + savepointPath + ".");

			// Retrieve the savepoint from the testing job manager
			LOG.info("Requesting the savepoint.");
			Future<Object> savepointFuture = jobManager.ask(
					new RequestSavepoint(savepointPath),
					deadline.timeLeft());

			CompletedCheckpoint savepoint = ((ResponseSavepoint) Await.result(
					savepointFuture, deadline.timeLeft())).savepoint();
			LOG.info("Retrieved savepoint: " + savepoint + ".");

			// Shut down the Flink cluster (thereby canceling the job)
			LOG.info("Shutting down Flink cluster.");
			flink.shutdown();

			// - Verification START -------------------------------------------

			// Only one checkpoint of the savepoint should exist
			String errMsg = "Checkpoints directory not cleaned up properly.";
			File[] files = checkpointDir.listFiles();
			if (files != null) {
				assertEquals(errMsg, 1, files.length);
			}
			else {
				fail(errMsg);
			}

			// Only one savepoint should exist
			errMsg = "Savepoints directory cleaned up.";
			files = savepointDir.listFiles();
			if (files != null) {
				assertEquals(errMsg, 1, files.length);
			}
			else {
				fail(errMsg);
			}

			// - Verification END ---------------------------------------------

			// Restart the cluster
			LOG.info("Restarting Flink cluster.");
			flink.start();

			// Retrieve the job manager
			LOG.info("Retrieving JobManager.");
			jobManager = Await.result(
					flink.leaderGateway().future(),
					deadline.timeLeft());
			LOG.info("JobManager: " + jobManager + ".");

			final Throwable[] error = new Throwable[1];
			final ForkableFlinkMiniCluster finalFlink = flink;
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
							jobGraph.setSavepointPath(savepointPath);

							LOG.info("Resubmitting job " + jobGraph.getJobID() + " with " +
									"savepoint path " + savepointPath + " in detached mode.");

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

								tdds.put(tdd.getVertexID(), tdd);
							}
						}
						catch (Throwable t) {
							error[0] = t;
						}
					}
				};
			}};

			// - Verification START -------------------------------------------

			errMsg = "Error during gathering of TaskDeploymentDescriptors";
			assertNull(errMsg, error[0]);

			// Verify that all tasks, which are part of the savepoint
			// have a matching task deployment descriptor.
			for (StateForTask stateForTask : savepoint.getStates()) {
				Collection<TaskDeploymentDescriptor> taskTdds = tdds.get(
						stateForTask.getOperatorId());

				errMsg = "Missing task for savepoint state for operator "
						+ stateForTask.getOperatorId() + ".";
				assertTrue(errMsg, taskTdds.size() > 0);

				boolean success = false;
				for (TaskDeploymentDescriptor tdd : taskTdds) {
					if (tdd.getIndexInSubtaskGroup() == stateForTask.getSubtask()) {
						success = true;

						errMsg = "Initial operator state mismatch.";
						assertEquals(errMsg, stateForTask.getState(), tdd.getOperatorState());
					}
				}

				errMsg = "No matching task deployment descriptor found.";
				assertTrue(errMsg, success);
			}

			// - Verification END ---------------------------------------------

			LOG.info("Cancelling job " + jobId + ".");
			jobManager.tell(new CancelJob(jobId));

			LOG.info("Disposing savepoint " + savepointPath + ".");
			Future<Object> disposeFuture = jobManager.ask(
					new DisposeSavepoint(savepointPath), deadline.timeLeft());

			errMsg = "Failed to dispose savepoint " + savepointPath + ".";
			Object resp = Await.result(disposeFuture, deadline.timeLeft());
			assertTrue(errMsg, resp.getClass() ==
					getDisposeSavepointSuccess().getClass());

			// - Verification START -------------------------------------------

			// The checkpoint files
			List<File> checkpointFiles = new ArrayList<>();

			for (StateForTask stateForTask : savepoint.getStates()) {
				StreamTaskStateList taskStateList = (StreamTaskStateList) stateForTask.getState()
						.deserializeValue(ClassLoader.getSystemClassLoader());

				for (StreamTaskState taskState : taskStateList.getState(
						ClassLoader.getSystemClassLoader())) {

					AbstractFileStateHandle fsState = (AbstractFileStateHandle) taskState.getFunctionState();
					checkpointFiles.add(new File(fsState.getFilePath().toUri()));
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
			assertNull(errMsg, savepointDir.listFiles());

			// - Verification END ---------------------------------------------
		}
		finally {
			if (flink != null) {
				flink.shutdown();
			}

			if (tmpDir != null) {
				FileUtils.deleteDirectory(tmpDir);
			}
		}
	}

	/**
	 * Tests that removed checkpoint files which are part of a savepoint throw
	 * a proper Exception on submission.
	 */
	@Test
	public void testCheckpointHasBeenRemoved() throws Exception {
		// Config
		int numTaskManagers = 2;
		int numSlotsPerTaskManager = 2;
		int parallelism = numTaskManagers * numSlotsPerTaskManager;

		// Test deadline
		final Deadline deadline = new FiniteDuration(5, TimeUnit.MINUTES).fromNow();

		// The number of checkpoints to complete before triggering the savepoint
		final int numberOfCompletedCheckpoints = 10;

		// Temporary directory for file state backend
		final File tmpDir = CommonTestUtils.createTempDirectory();

		LOG.info("Created temporary directory: " + tmpDir + ".");

		ForkableFlinkMiniCluster flink = null;

		try {
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
			config.setString(SavepointStoreFactory.SAVEPOINT_BACKEND_KEY, "filesystem");

			config.setString(FsStateBackendFactory.CHECKPOINT_DIRECTORY_URI_CONF_KEY,
					checkpointDir.toURI().toString());
			config.setString(SavepointStoreFactory.SAVEPOINT_DIRECTORY_KEY,
					savepointDir.toURI().toString());

			LOG.info("Flink configuration: " + config + ".");

			// Start Flink
			flink = new ForkableFlinkMiniCluster(config);
			LOG.info("Starting Flink cluster.");
			flink.start();

			// Retrieve the job manager
			LOG.info("Retrieving JobManager.");
			ActorGateway jobManager = Await.result(
					flink.leaderGateway().future(),
					deadline.timeLeft());
			LOG.info("JobManager: " + jobManager + ".");

			// Submit the job
			final JobGraph jobGraph = createJobGraph(parallelism, 0, 1000, 1000);
			final JobID jobId = jobGraph.getJobID();

			// Wait for the source to be notified about the expected number
			// of completed checkpoints
			InfiniteTestSource.CheckpointCompleteLatch = new CountDownLatch(
					numberOfCompletedCheckpoints);

			LOG.info("Submitting job " + jobGraph.getJobID() + " in detached mode.");

			flink.submitJobDetached(jobGraph);

			LOG.info("Waiting for " + numberOfCompletedCheckpoints +
					" checkpoint complete notifications.");

			// Wait...
			InfiniteTestSource.CheckpointCompleteLatch.await();

			LOG.info("Received all " + numberOfCompletedCheckpoints +
					" checkpoint complete notifications.");

			// ...and then trigger the savepoint
			LOG.info("Triggering a savepoint.");

			Future<Object> savepointPathFuture = jobManager.ask(
					new TriggerSavepoint(jobId), deadline.timeLeft());

			final String savepointPath = ((TriggerSavepointSuccess) Await
					.result(savepointPathFuture, deadline.timeLeft())).savepointPath();
			LOG.info("Retrieved savepoint path: " + savepointPath + ".");

			// Retrieve the savepoint from the testing job manager
			LOG.info("Requesting the savepoint.");
			Future<Object> savepointFuture = jobManager.ask(
					new RequestSavepoint(savepointPath),
					deadline.timeLeft());

			CompletedCheckpoint savepoint = ((ResponseSavepoint) Await.result(
					savepointFuture, deadline.timeLeft())).savepoint();
			LOG.info("Retrieved savepoint: " + savepoint + ".");

			// Shut down the Flink cluster (thereby canceling the job)
			LOG.info("Shutting down Flink cluster.");
			flink.shutdown();

			// Remove the checkpoint files
			FileUtils.deleteDirectory(checkpointDir);

			// Restart the cluster
			LOG.info("Restarting Flink cluster.");
			flink.start();

			// Set the savepoint path
			jobGraph.setSavepointPath(savepointPath);

			LOG.info("Resubmitting job " + jobGraph.getJobID() + " with " +
					"savepoint path " + savepointPath + " in detached mode.");

			try {
				flink.submitJobAndWait(jobGraph, false, deadline.timeLeft());
				fail("Did not throw expected Exception because of missing checkpoint files");
			}
			catch (Exception ignored) {
			}
		}
		finally {
			if (flink != null) {
				flink.shutdown();
			}

			if (tmpDir != null) {
				FileUtils.deleteDirectory(tmpDir);
			}
		}
	}

	/**
	 * Tests that a job manager backed savepoint is removed when the checkpoint
	 * coordinator is shut down, because the associated checkpoints files will
	 * linger around otherwise.
	 */
	@Test
	public void testCheckpointsRemovedWithJobManagerBackendOnShutdown() throws Exception {
		// Config
		int numTaskManagers = 2;
		int numSlotsPerTaskManager = 2;
		int parallelism = numTaskManagers * numSlotsPerTaskManager;

		// Test deadline
		final Deadline deadline = new FiniteDuration(5, TimeUnit.MINUTES).fromNow();

		// The number of checkpoints to complete before triggering the savepoint
		final int numberOfCompletedCheckpoints = 10;

		// Temporary directory for file state backend
		final File tmpDir = CommonTestUtils.createTempDirectory();

		LOG.info("Created temporary directory: " + tmpDir + ".");

		ForkableFlinkMiniCluster flink = null;
		List<File> checkpointFiles = new ArrayList<>();

		try {
			// Flink configuration
			final Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTaskManagers);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlotsPerTaskManager);

			final File checkpointDir = new File(tmpDir, "checkpoints");

			if (!checkpointDir.mkdir()) {
				fail("Test setup failed: failed to create temporary directories.");
			}

			LOG.info("Created temporary checkpoint directory: " + checkpointDir + ".");

			config.setString(SavepointStoreFactory.SAVEPOINT_BACKEND_KEY, "jobmanager");
			config.setString(ConfigConstants.STATE_BACKEND, "filesystem");
			config.setString(FsStateBackendFactory.CHECKPOINT_DIRECTORY_URI_CONF_KEY,
					checkpointDir.toURI().toString());

			LOG.info("Flink configuration: " + config + ".");

			// Start Flink
			flink = new ForkableFlinkMiniCluster(config);
			LOG.info("Starting Flink cluster.");
			flink.start();

			// Retrieve the job manager
			LOG.info("Retrieving JobManager.");
			ActorGateway jobManager = Await.result(
					flink.leaderGateway().future(),
					deadline.timeLeft());
			LOG.info("JobManager: " + jobManager + ".");

			// Submit the job
			final JobGraph jobGraph = createJobGraph(parallelism, 0, 1000, 1000);
			final JobID jobId = jobGraph.getJobID();

			// Wait for the source to be notified about the expected number
			// of completed checkpoints
			InfiniteTestSource.CheckpointCompleteLatch = new CountDownLatch(
					numberOfCompletedCheckpoints);

			LOG.info("Submitting job " + jobGraph.getJobID() + " in detached mode.");

			flink.submitJobDetached(jobGraph);

			LOG.info("Waiting for " + numberOfCompletedCheckpoints +
					" checkpoint complete notifications.");

			// Wait...
			InfiniteTestSource.CheckpointCompleteLatch.await();

			LOG.info("Received all " + numberOfCompletedCheckpoints +
					" checkpoint complete notifications.");

			// ...and then trigger the savepoint
			LOG.info("Triggering a savepoint.");

			Future<Object> savepointPathFuture = jobManager.ask(
					new TriggerSavepoint(jobId), deadline.timeLeft());

			final String savepointPath = ((TriggerSavepointSuccess) Await
					.result(savepointPathFuture, deadline.timeLeft())).savepointPath();
			LOG.info("Retrieved savepoint path: " + savepointPath + ".");

			// Retrieve the savepoint from the testing job manager
			LOG.info("Requesting the savepoint.");
			Future<Object> savepointFuture = jobManager.ask(
					new RequestSavepoint(savepointPath),
					deadline.timeLeft());

			CompletedCheckpoint savepoint = ((ResponseSavepoint) Await.result(
					savepointFuture, deadline.timeLeft())).savepoint();
			LOG.info("Retrieved savepoint: " + savepoint + ".");

			// Cancel the job
			LOG.info("Cancelling job " + jobId + ".");
			Future<Object> cancelRespFuture = jobManager.ask(
					new CancelJob(jobId), deadline.timeLeft());
			assertTrue(Await.result(cancelRespFuture, deadline.timeLeft())
					instanceof CancellationSuccess);

			LOG.info("Waiting for job " + jobId + " to be removed.");
			Future<Object> removedRespFuture = jobManager.ask(
					new NotifyWhenJobRemoved(jobId), deadline.timeLeft());
			assertTrue((Boolean) Await.result(removedRespFuture, deadline.timeLeft()));

			// Check that all checkpoint files have been removed
			for (StateForTask stateForTask : savepoint.getStates()) {
				StreamTaskStateList taskStateList = (StreamTaskStateList) stateForTask.getState()
						.deserializeValue(ClassLoader.getSystemClassLoader());

				for (StreamTaskState taskState : taskStateList.getState(
						ClassLoader.getSystemClassLoader())) {

					AbstractFileStateHandle fsState = (AbstractFileStateHandle) taskState.getFunctionState();
					checkpointFiles.add(new File(fsState.getFilePath().toUri()));
				}
			}
		}
		finally {
			if (flink != null) {
				flink.shutdown();
			}

			// At least one checkpoint file
			assertTrue(checkpointFiles.size() > 0);

			// The checkpoint associated with the savepoint should have been
			// discarded after shutdown
			for (File f : checkpointFiles) {
				String errMsg = "Checkpoint file " + f + " not cleaned up properly.";
				assertFalse(errMsg, f.exists());
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

		ForkableFlinkMiniCluster flink = null;

		try {
			// Flink configuration
			final Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTaskManagers);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlotsPerTaskManager);

			LOG.info("Flink configuration: " + config + ".");

			// Start Flink
			flink = new ForkableFlinkMiniCluster(config);
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
			jobGraph.setSavepointPath("unknown path");
			assertEquals("unknown path", jobGraph.getSnapshotSettings().getSavepointPath());

			LOG.info("Submitting job " + jobGraph.getJobID() + " in detached mode.");

			try {
				flink.submitJobAndWait(jobGraph, false);
			}
			catch (Exception e) {
				assertEquals(UnrecoverableException.class, e.getCause().getClass());
				assertEquals(IllegalArgumentException.class, e.getCause().getCause().getClass());
			}
		}
		finally {
			if (flink != null) {
				flink.shutdown();
			}
		}
	}

	/**
	 * Tests that a restore failure is retried with the savepoint state.
	 */
	@Test
	public void testRestoreFailure() throws Exception {
		// Config
		int numTaskManagers = 1;
		int numSlotsPerTaskManager = 1;
		int numExecutionRetries = 2;
		int retryDelay = 500;
		int checkpointingInterval = 100000000;

		// Test deadline
		final Deadline deadline = new FiniteDuration(3, TimeUnit.MINUTES).fromNow();

		ForkableFlinkMiniCluster flink = null;

		try {
			// The job
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(1);
			env.enableCheckpointing(checkpointingInterval);
			env.setNumberOfExecutionRetries(numExecutionRetries);
			env.getConfig().setExecutionRetryDelay(retryDelay);

			DataStream<Integer> stream = env
					.addSource(new RestoreStateCountingAndFailingSource());

			// Source configuration
			RestoreStateCountingAndFailingSource.failOnRestoreStateCall = false;
			RestoreStateCountingAndFailingSource.numRestoreStateCalls = 0;
			RestoreStateCountingAndFailingSource.checkpointCompleteLatch = new CountDownLatch(1);
			RestoreStateCountingAndFailingSource.emitted= 0;

			stream.addSink(new DiscardingSink<Integer>());

			JobGraph jobGraph = env.getStreamGraph().getJobGraph();

			// Flink configuration
			final Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTaskManagers);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlotsPerTaskManager);
			LOG.info("Flink configuration: " + config + ".");

			// Start Flink
			flink = new ForkableFlinkMiniCluster(config);
			LOG.info("Starting Flink cluster.");
			flink.start();

			// Retrieve the job manager
			LOG.info("Retrieving JobManager.");
			ActorGateway jobManager = flink.getLeaderGateway(deadline.timeLeft());
			LOG.info("JobManager: " + jobManager + ".");

			// Submit the job and wait for some checkpoints to complete
			flink.submitJobDetached(jobGraph);

			while (deadline.hasTimeLeft() && RestoreStateCountingAndFailingSource.emitted < 100) {
				Thread.sleep(100);
			}

			assertTrue("No progress", RestoreStateCountingAndFailingSource.emitted >= 100);

			// Trigger the savepoint
			Future<Object> savepointPathFuture = jobManager.ask(
					new TriggerSavepoint(jobGraph.getJobID()), deadline.timeLeft());

			final String savepointPath = ((TriggerSavepointSuccess) Await
					.result(savepointPathFuture, deadline.timeLeft())).savepointPath();
			LOG.info("Retrieved savepoint path: " + savepointPath + ".");

			// Completed checkpoint
			RestoreStateCountingAndFailingSource.checkpointCompleteLatch.await();

			// Cancel the job
			Future<?> cancelFuture = jobManager.ask(new CancelJob(
					jobGraph.getJobID()), deadline.timeLeft());
			Await.ready(cancelFuture, deadline.timeLeft());

			// Wait for the job to be removed
			Future<?> removedFuture = jobManager.ask(new NotifyWhenJobRemoved(
					jobGraph.getJobID()), deadline.timeLeft());
			Await.ready(removedFuture, deadline.timeLeft());

			// Set source to fail on restore calls and try to recover from savepoint
			RestoreStateCountingAndFailingSource.failOnRestoreStateCall = true;
			jobGraph.setSavepointPath(savepointPath);

			try {
				flink.submitJobAndWait(jobGraph, false, deadline.timeLeft());
				// If the savepoint state is not restored, we will wait here
				// until the deadline times out.
				fail("Did not throw expected Exception");
			} catch (Exception ignored) {
			} finally {
				// Expecting one restore for the initial submission from
				// savepoint and one for the execution retries
				assertEquals(1 + numExecutionRetries, RestoreStateCountingAndFailingSource.numRestoreStateCalls);
			}
		}
		finally {
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

	private static class InfiniteTestSource
			implements SourceFunction<Integer>, CheckpointListener {

		private static final long serialVersionUID = 1L;
		private volatile boolean running = true;

		// Test control
		private static CountDownLatch CheckpointCompleteLatch = new CountDownLatch(1);

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

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			CheckpointCompleteLatch.countDown();
		}
	}

	private static class StatefulCounter
			extends RichMapFunction<Integer, Integer>
			implements Checkpointed<byte[]> {

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
			LOG.info("snapshotState (" + checkpointId + "): " + Arrays.toString(data));
			return data;
		}

		@Override
		public void restoreState(byte[] data) throws Exception {
			LOG.info("restoreState: " + Arrays.toString(data));
			this.data = data;
		}
	}

	/**
	 * Test source that counts calls to restoreState and that can be configured
	 * to fail on restoreState calls.
	 */
	private static class RestoreStateCountingAndFailingSource
			implements SourceFunction<Integer>, Checkpointed, CheckpointListener {

		private static final long serialVersionUID = 1L;

		private static volatile int numRestoreStateCalls = 0;
		private static volatile boolean failOnRestoreStateCall = false;
		private static volatile CountDownLatch checkpointCompleteLatch = new CountDownLatch(1);
		private static volatile int emitted = 0;

		private volatile boolean running = true;

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			while (running) {
				ctx.collect(1);
				emitted++;
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		@Override
		public Serializable snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return 1;
		}

		@Override
		public void restoreState(Serializable state) throws Exception {
			numRestoreStateCalls++;

			if (failOnRestoreStateCall) {
				throw new RuntimeException("Restore test failure");
			}
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			checkpointCompleteLatch.countDown();
		}
	}

}
