/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

public class SavepointUtil {

	// list of JobGraphs to create savepoints for
	private static final ArrayList<Class<? extends SavepointTestJob>> savepointJobs = new ArrayList<>();
	static {
		savepointJobs.add(UserFunctionStateJob.class);
	}

	private static final Logger LOG = LoggerFactory.getLogger(SavepointUtil.class);
	private static final Deadline DEADLINE = new FiniteDuration(5, TimeUnit.MINUTES).fromNow();
	private static final String SAVEPOINT_BASE_DIR = "./flink-tests/src/test/resources/savepoints/";

	private static final int STATE_WAIT_FOR_JOB = 0;
	private static final int STATE_REQUEST_SAVEPOINT = 1;
	private static final int STATE_SAVEPOINT_DONE = 2;
	private static final int STATE_WAIT_FOR_TEST_JOB = 3;
	private static final int STATE_TEST_RESULT = 4;
	private static final int STATE_END = 5;

	private static volatile int state = STATE_WAIT_FOR_JOB;

	private static TestingCluster flink = null;
	private static ActorGateway jobManager = null;
	private static JobID jobId = null;
	private static File savepointDir = null;
	private static Exception testResult = null;

	public static void main(String[] args) throws Exception {

		// clean up
//		FileUtils.deleteDirectory(new File(SAVEPOINT_BASE_DIR));

		for (Class<? extends SavepointTestJob> testJob : savepointJobs) {
			SavepointTestJob job = testJob.newInstance();

//			runJobAndCreateSavepoint(job);

			runJobAndCompareState(job);

			triggerEndOfTest();
		}
	}

	public static synchronized void triggerSavepoint() {
		SavepointUtil.state = SavepointUtil.STATE_REQUEST_SAVEPOINT;
	}

	public static synchronized boolean allowStateChange() {
		return SavepointUtil.state < SavepointUtil.STATE_REQUEST_SAVEPOINT;
	}

	public static synchronized void triggerOrTestSavepoint(RichFunction function, Object expected, Object actual) throws Exception {
		if (SavepointUtil.state == SavepointUtil.STATE_WAIT_FOR_TEST_JOB) {
			if (expected.equals(actual)) {
				LOG.info("Test was successful.");
				SavepointUtil.testResult = null;
				SavepointUtil.state = SavepointUtil.STATE_TEST_RESULT;
			} else {
				LOG.info("Test failed.");
				SavepointUtil.testResult = new Exception("Comparison of state failed. Expected: " + expected + " but was: " + actual);
				SavepointUtil.state = SavepointUtil.STATE_TEST_RESULT;
			}
		} else if (SavepointUtil.state == SavepointUtil.STATE_WAIT_FOR_JOB) {
			final StateCondition condition = new StateCondition(function.getClass(), function.getRuntimeContext().getIndexOfThisSubtask());
			if (!testCounters.containsKey(condition)) {
				testCounters.put(condition, 0);
			}
			final Integer counter = testCounters.get(condition);
			testCounters.put(condition, counter + 1);
			// check if all counters are ready
			if (checkIfReadyForSavepoint()) {
				SavepointUtil.state = SavepointUtil.STATE_REQUEST_SAVEPOINT;
			}
		}
	}

	public static void triggerEndOfTest() throws Exception {
		LOG.info("Cancelling Flink.");
		if (flink != null) {
			flink.stop();
		}
		SavepointUtil.state = SavepointUtil.STATE_END;
	}

	public static void runJobAndCreateSavepoint(SavepointTestJob job) throws Exception {
		LOG.info("Waiting for job.");
		SavepointUtil.state = SavepointUtil.STATE_WAIT_FOR_JOB;

		final Thread t = new Thread(new SavepointPerformer());
		t.start();

		runJob(job);

		while(SavepointUtil.state != SavepointUtil.STATE_SAVEPOINT_DONE && DEADLINE.hasTimeLeft()) {
			Thread.sleep(100);
		}
	}

	public static void runJobAndCompareState(SavepointTestJob job) throws Exception {
		LOG.info("Waiting for test job.");
		SavepointUtil.state = SavepointUtil.STATE_WAIT_FOR_TEST_JOB;

		runJob(job);

		while(SavepointUtil.state != SavepointUtil.STATE_TEST_RESULT && DEADLINE.hasTimeLeft()) {
			Thread.sleep(100);
		}

		if (SavepointUtil.state != SavepointUtil.STATE_TEST_RESULT) {
			throw new Exception("No test result available.");
		}
		if (testResult != null) {
			throw testResult;
		}
	}

	public static void setTestResult(Exception e) {
		SavepointUtil.testResult = e;
		SavepointUtil.state = SavepointUtil.STATE_TEST_RESULT;
	}

	// --------------------------------------------------------------------------------------------

	private static void runJob(SavepointTestJob job) throws Exception {
		// Config
		int numTaskManagers = 2;
		int numSlotsPerTaskManager = 2;
		int parallelism = numTaskManagers * numSlotsPerTaskManager;
		String savepointPath = SAVEPOINT_BASE_DIR + job.getClass().getSimpleName();

		// Flink configuration
		final Configuration config = new Configuration();
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTaskManagers);
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlotsPerTaskManager);

		final File checkpointDir = File.createTempFile("checkpoints", Long.toString(System.nanoTime()));
		savepointDir = new File(savepointPath);
		savepointDir.mkdirs();

		if (!checkpointDir.exists() || !savepointDir.exists()) {
			throw new Exception("Test setup failed: failed to create (temporary) directories.");
		}

		LOG.info("Created temporary checkpoint directory: " + checkpointDir + ".");
		LOG.info("Created savepoint directory: " + savepointDir + ".");

		config.setString(ConfigConstants.STATE_BACKEND, "filesystem");
		config.setString(FsStateBackendFactory.CHECKPOINT_DIRECTORY_URI_CONF_KEY, checkpointDir.toURI().toString());
		config.setString(FsStateBackendFactory.MEMORY_THRESHOLD_CONF_KEY, "0");
		config.setString("state.savepoints.dir", savepointDir.toURI().toString());

		LOG.info("Flink configuration: " + config + ".");

		// Start Flink
		flink = new TestingCluster(config);
		flink.start();

		// Retrieve the job manager
		jobManager = Await.result(flink.leaderGateway().future(), DEADLINE.timeLeft());

		// Submit the job
		final JobGraph jobGraph = job.createJobGraph();
		if (SavepointUtil.state == SavepointUtil.STATE_WAIT_FOR_JOB) {
			savepointCondition = job.getSavepointCondition();
			testCounters.clear();
		} else if (SavepointUtil.state == SavepointUtil.STATE_WAIT_FOR_TEST_JOB) {
			final File[] dir = savepointDir.listFiles();
			if (dir.length == 0) {
				throw new RuntimeException("Savepoint of " + job.getClass().getSimpleName() + " does not exist.");
			}
			jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(dir[0].getAbsolutePath()));
		}
		jobId = jobGraph.getJobID();

		LOG.info("Submitting job " + jobGraph.getJobID() + " and waiting...");

		flink.submitJobDetached(jobGraph);
	}

	private static final HashMap<StateCondition, Integer> testCounters = new HashMap<>();
	private static SavepointCondition[] savepointCondition = null;

	private static boolean checkIfReadyForSavepoint() {
		for (SavepointCondition condition : savepointCondition) {
			final StateCondition stateCondition = new StateCondition(condition.clazz, condition.subtask);
			if (!testCounters.containsKey(stateCondition) || testCounters.get(stateCondition) != condition.invocation) {
				return false;
			}
		}
		return true;
	}

	private static void performSavepointAndShutdown() throws Exception {
		LOG.info("Triggering a savepoint.");

		// Flink 1.2
		final Future<Object> savepointPathFuture = jobManager.ask(new JobManagerMessages.TriggerSavepoint(jobId, Option.<String>empty()), DEADLINE.timeLeft());
		// Flink 1.1
//        final Future<Object> savepointPathFuture = jobManager.ask(new JobManagerMessages.TriggerSavepoint(jobId), DEADLINE.timeLeft());

		final String savepointPath = ((JobManagerMessages.TriggerSavepointSuccess) Await.result(savepointPathFuture, DEADLINE.timeLeft())).savepointPath();
		LOG.info("Saved savepoint: " + savepointPath);

		// Retrieve the savepoint from the testing job manager
		LOG.info("Requesting the savepoint.");
		Future<Object> savepointFuture = jobManager.ask(new TestingJobManagerMessages.RequestSavepoint(savepointPath), DEADLINE.timeLeft());

		Savepoint savepoint = ((TestingJobManagerMessages.ResponseSavepoint) Await.result(savepointFuture, DEADLINE.timeLeft())).savepoint();
		LOG.info("Retrieved savepoint: " + savepointPath + ".");

		LOG.info("Storing savepoint to file.");

		// Flink 1.2
		// it might be that the savepoint has already been written to file in Flink 1.2
		// this is just the command how to do it in 1.2
//        org.apache.flink.runtime.checkpoint.savepoint.SavepointStore.storeSavepoint(savepointDir.getAbsolutePath(), savepoint);
		// Flink 1.1
		// this writes it for FLink 1.1
//        Configuration config = new Configuration();
//        config.setString(org.apache.flink.runtime.checkpoint.savepoint.SavepointStoreFactory.SAVEPOINT_BACKEND_KEY, "filesystem");
//        config.setString(org.apache.flink.runtime.checkpoint.savepoint.SavepointStoreFactory.SAVEPOINT_DIRECTORY_KEY, "file://" + savepointDir.getAbsolutePath());
//        org.apache.flink.runtime.checkpoint.savepoint.SavepointStoreFactory.createFromConfig(config).storeSavepoint(savepoint);

		LOG.info("Cancelling Flink.");
		flink.stop();

		SavepointUtil.state = SavepointUtil.STATE_SAVEPOINT_DONE;
	}

	private static class StateCondition {
		private Class<?> clazz;
		private Integer subtask;

		StateCondition(Class<?> clazz, Integer subtask) {
			this.clazz = clazz;
			this.subtask = subtask;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			StateCondition that = (StateCondition) o;

			return clazz.equals(that.clazz) && subtask.equals(that.subtask);
		}

		@Override
		public int hashCode() {
			int result = clazz.hashCode();
			result = 31 * result + subtask.hashCode();
			return result;
		}
	}

	public static class SavepointCondition {
		Class<? extends RichFunction> clazz;
		int subtask;
		int invocation;

		SavepointCondition(Class<? extends RichFunction> clazz, int subtask, int invocation) {
			this.clazz = clazz;
			this.subtask = subtask;
			this.invocation = invocation;
		}
	}

	public interface SavepointTestJob {
		JobGraph createJobGraph();

		SavepointCondition[] getSavepointCondition();
	}

	private static class SavepointPerformer implements Runnable {

		@Override
		public void run() {
			try {
				while (SavepointUtil.state != SavepointUtil.STATE_END) {
					Thread.sleep(100);
					if (SavepointUtil.state == SavepointUtil.STATE_REQUEST_SAVEPOINT) {
						try {
							performSavepointAndShutdown();
						} catch (Exception e) {
							throw new RuntimeException("Performing savepoint failed.", e);
						}
					}
				}
			} catch (InterruptedException e) {
				// stop execution
			}
			LOG.info("SavepointPerformer Thread finished.");
		}
	}
}
