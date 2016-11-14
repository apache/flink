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

package org.apache.flink.test.classloading;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.JobManagerMessages.DisposeSavepoint;
import org.apache.flink.runtime.messages.JobManagerMessages.DisposeSavepointFailure;
import org.apache.flink.runtime.messages.JobManagerMessages.RunningJobsStatus;
import org.apache.flink.runtime.messages.JobManagerMessages.TriggerSavepoint;
import org.apache.flink.runtime.messages.JobManagerMessages.TriggerSavepointSuccess;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.WaitForAllVerticesToBeRunning;
import org.apache.flink.test.testdata.KMeansData;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class ClassLoaderITCase extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderITCase.class);

	private static final String INPUT_SPLITS_PROG_JAR_FILE = "customsplit-test-jar.jar";

	private static final String STREAMING_INPUT_SPLITS_PROG_JAR_FILE = "streaming-customsplit-test-jar.jar";

	private static final String STREAMING_PROG_JAR_FILE = "streamingclassloader-test-jar.jar";

	private static final String STREAMING_CHECKPOINTED_PROG_JAR_FILE = "streaming-checkpointed-classloader-test-jar.jar";

	private static final String KMEANS_JAR_PATH = "kmeans-test-jar.jar";

	private static final String USERCODETYPE_JAR_PATH = "usercodetype-test-jar.jar";

	private static final String CUSTOM_KV_STATE_JAR_PATH = "custom_kv_state-test-jar.jar";

	private static final String CHECKPOINTING_CUSTOM_KV_STATE_JAR_PATH = "checkpointing_custom_kv_state-test-jar.jar";

	public static final TemporaryFolder FOLDER = new TemporaryFolder();

	private static TestingCluster testCluster;

	private static int parallelism;

	@BeforeClass
	public static void setUp() throws Exception {
		FOLDER.create();

		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 2);
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 2);
		parallelism = 4;

		// we need to use the "filesystem" state backend to ensure FLINK-2543 is not happening again.
		config.setString(ConfigConstants.STATE_BACKEND, "filesystem");
		config.setString(FsStateBackendFactory.CHECKPOINT_DIRECTORY_URI_CONF_KEY,
				FOLDER.newFolder().getAbsoluteFile().toURI().toString());

		// Savepoint path
		config.setString(ConfigConstants.SAVEPOINT_DIRECTORY_KEY,
				FOLDER.newFolder().getAbsoluteFile().toURI().toString());

		testCluster = new TestingCluster(config, false);
		testCluster.start();
	}

	@AfterClass
	public static void tearDown() throws Exception {
		if (testCluster != null) {
			testCluster.shutdown();
		}

		FOLDER.delete();
	}

	@Test
	public void testJobsWithCustomClassLoader() {
		try {
			int port = testCluster.getLeaderRPCPort();

			PackagedProgram inputSplitTestProg = new PackagedProgram(
					new File(INPUT_SPLITS_PROG_JAR_FILE),
					new String[] { INPUT_SPLITS_PROG_JAR_FILE,
							"", // classpath
							"localhost",
							String.valueOf(port),
							"4" // parallelism
					});
			inputSplitTestProg.invokeInteractiveModeForExecution();

			PackagedProgram streamingInputSplitTestProg = new PackagedProgram(
					new File(STREAMING_INPUT_SPLITS_PROG_JAR_FILE),
					new String[] { STREAMING_INPUT_SPLITS_PROG_JAR_FILE,
							"localhost",
							String.valueOf(port),
							"4" // parallelism
					});
			streamingInputSplitTestProg.invokeInteractiveModeForExecution();

			String classpath = new File(INPUT_SPLITS_PROG_JAR_FILE).toURI().toURL().toString();
			PackagedProgram inputSplitTestProg2 = new PackagedProgram(new File(INPUT_SPLITS_PROG_JAR_FILE),
					new String[] { "",
							classpath, // classpath
							"localhost",
							String.valueOf(port),
							"4" // parallelism
					});
			inputSplitTestProg2.invokeInteractiveModeForExecution();

			// regular streaming job
			PackagedProgram streamingProg = new PackagedProgram(
					new File(STREAMING_PROG_JAR_FILE),
					new String[] {
							STREAMING_PROG_JAR_FILE,
							"localhost",
							String.valueOf(port)
					});
			streamingProg.invokeInteractiveModeForExecution();

			// checkpointed streaming job with custom classes for the checkpoint (FLINK-2543)
			// the test also ensures that user specific exceptions are serializable between JobManager <--> JobClient.
			try {
				PackagedProgram streamingCheckpointedProg = new PackagedProgram(
						new File(STREAMING_CHECKPOINTED_PROG_JAR_FILE),
						new String[] {
								STREAMING_CHECKPOINTED_PROG_JAR_FILE,
								"localhost",
								String.valueOf(port) });
				streamingCheckpointedProg.invokeInteractiveModeForExecution();
			} catch (Exception e) {
				// we can not access the SuccessException here when executing the tests with maven, because its not available in the jar.
				assertEquals("Program should terminate with a 'SuccessException'",
						"org.apache.flink.test.classloading.jar.CheckpointedStreamingProgram.SuccessException",
						e.getCause().getCause().getClass().getCanonicalName());
			}

			PackagedProgram kMeansProg = new PackagedProgram(
					new File(KMEANS_JAR_PATH),
					new String[] { KMEANS_JAR_PATH,
							"localhost",
							String.valueOf(port),
							"4", // parallelism
							KMeansData.DATAPOINTS,
							KMeansData.INITIAL_CENTERS,
							"25"
					});
			kMeansProg.invokeInteractiveModeForExecution();

			// test FLINK-3633
			final PackagedProgram userCodeTypeProg = new PackagedProgram(
					new File(USERCODETYPE_JAR_PATH),
					new String[] { USERCODETYPE_JAR_PATH,
							"localhost",
							String.valueOf(port),
					});

			userCodeTypeProg.invokeInteractiveModeForExecution();

			File checkpointDir = FOLDER.newFolder();
			File outputDir = FOLDER.newFolder();

			final PackagedProgram program = new PackagedProgram(
					new File(CHECKPOINTING_CUSTOM_KV_STATE_JAR_PATH),
					new String[] {
							CHECKPOINTING_CUSTOM_KV_STATE_JAR_PATH,
							"localhost",
							String.valueOf(port),
							checkpointDir.toURI().toString(),
							outputDir.toURI().toString()
					});

			program.invokeInteractiveModeForExecution();

		} catch (Exception e) {
			if (!(e.getCause().getCause() instanceof SuccessException)) {
				fail(e.getMessage());
			}
		}
	}

	/**
	 * Tests disposal of a savepoint, which contains custom user code KvState.
	 */
	@Test
	public void testDisposeSavepointWithCustomKvState() throws Exception {
		Deadline deadline = new FiniteDuration(100, TimeUnit.SECONDS).fromNow();

		int port = testCluster.getLeaderRPCPort();

		File checkpointDir = FOLDER.newFolder();
		File outputDir = FOLDER.newFolder();

		final PackagedProgram program = new PackagedProgram(
				new File(CUSTOM_KV_STATE_JAR_PATH),
				new String[] {
						CUSTOM_KV_STATE_JAR_PATH,
						"localhost",
						String.valueOf(port),
						String.valueOf(parallelism),
						checkpointDir.toURI().toString(),
						"5000",
						outputDir.toURI().toString()
				});

		// Execute detached
		Thread invokeThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					program.invokeInteractiveModeForExecution();
				} catch (ProgramInvocationException ignored) {
					ignored.printStackTrace();
				}
			}
		});

		LOG.info("Starting program invoke thread");
		invokeThread.start();

		// The job ID
		JobID jobId = null;

		ActorGateway jm = testCluster.getLeaderGateway(deadline.timeLeft());

		LOG.info("Waiting for job status running.");

		// Wait for running job
		while (jobId == null && deadline.hasTimeLeft()) {
			Future<Object> jobsFuture = jm.ask(JobManagerMessages.getRequestRunningJobsStatus(), deadline.timeLeft());
			RunningJobsStatus runningJobs = (RunningJobsStatus) Await.result(jobsFuture, deadline.timeLeft());

			for (JobStatusMessage runningJob : runningJobs.getStatusMessages()) {
				jobId = runningJob.getJobId();
				LOG.info("Job running. ID: " + jobId);
				break;
			}

			// Retry if job is not available yet
			if (jobId == null) {
				Thread.sleep(100);
			}
		}

		LOG.info("Wait for all tasks to be running.");
		Future<Object> allRunning = jm.ask(new WaitForAllVerticesToBeRunning(jobId), deadline.timeLeft());
		Await.ready(allRunning, deadline.timeLeft());
		LOG.info("All tasks are running.");

		// Trigger savepoint
		String savepointPath = null;
		for (int i = 0; i < 20; i++) {
			LOG.info("Triggering savepoint (" + (i+1) + "/20).");
			Future<Object> savepointFuture = jm.ask(new TriggerSavepoint(jobId, Option.<String>empty()), deadline.timeLeft());

			Object savepointResponse = Await.result(savepointFuture, deadline.timeLeft());

			if (savepointResponse.getClass() == TriggerSavepointSuccess.class) {
				savepointPath = ((TriggerSavepointSuccess) savepointResponse).savepointPath();
				LOG.info("Triggered savepoint. Path: " + savepointPath);
			} else if (savepointResponse.getClass() == JobManagerMessages.TriggerSavepointFailure.class) {
				Throwable cause = ((JobManagerMessages.TriggerSavepointFailure) savepointResponse).cause();
				LOG.info("Failed to trigger savepoint. Retrying...", cause);
				// This can fail if the operators are not opened yet
				Thread.sleep(500);
			} else {
				throw new IllegalStateException("Unexpected response to TriggerSavepoint");
			}
		}

		assertNotNull("Failed to trigger savepoint", savepointPath);

		// Upload JAR
		LOG.info("Uploading JAR " + CUSTOM_KV_STATE_JAR_PATH + " for savepoint disposal.");
		List<BlobKey> blobKeys = BlobClient.uploadJarFiles(jm, deadline.timeLeft(), testCluster.userConfiguration(),
				Collections.singletonList(new Path(CUSTOM_KV_STATE_JAR_PATH)));

		// Dispose savepoint
		LOG.info("Disposing savepoint at " + savepointPath);
		Future<Object> disposeFuture = jm.ask(new DisposeSavepoint(savepointPath), deadline.timeLeft());
		Object disposeResponse = Await.result(disposeFuture, deadline.timeLeft());

		if (disposeResponse.getClass() == JobManagerMessages.getDisposeSavepointSuccess().getClass()) {
			// Success :-)
			LOG.info("Disposed savepoint at " + savepointPath);
		} else if (disposeResponse instanceof DisposeSavepointFailure) {
			throw new IllegalStateException("Failed to dispose savepoint " + disposeResponse);
		} else {
			throw new IllegalStateException("Unexpected response to DisposeSavepoint");
		}
	}
}
