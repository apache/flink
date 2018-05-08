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
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.test.testdata.KMeansData;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.test.util.TestEnvironment;
import org.apache.flink.testutils.category.New;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * Test job classloader.
 */
@Category(New.class)
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


	private static final TemporaryFolder FOLDER = new TemporaryFolder();

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	private static MiniCluster testCluster;

	private static final int parallelism = 4;

	@BeforeClass
	public static void setUp() throws Exception {
		FOLDER.create();

		Configuration config = new Configuration();

		// we need to use the "filesystem" state backend to ensure FLINK-2543 is not happening again.
		config.setString(CheckpointingOptions.STATE_BACKEND, "filesystem");
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY,
				FOLDER.newFolder().getAbsoluteFile().toURI().toString());

		// Savepoint path
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY,
				FOLDER.newFolder().getAbsoluteFile().toURI().toString());

		// required as we otherwise run out of memory
		config.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, 80);

		testCluster = new MiniCluster(
			new MiniClusterConfiguration.Builder()
				.setNumTaskManagers(2)
				.setNumSlotsPerTaskManager(2)
				.setConfiguration(config)
			.build()
		);
		testCluster.start();
	}

	@AfterClass
	public static void tearDownClass() throws Exception {
		if (testCluster != null) {
			testCluster.close();
		}
		FOLDER.delete();
	}

	@After
	public void tearDown() throws Exception {
		TestStreamEnvironment.unsetAsContext();
		TestEnvironment.unsetAsContext();
	}

	@Test
	public void testCustomSplitJobWithCustomClassLoaderJar() throws IOException, ProgramInvocationException {

		PackagedProgram inputSplitTestProg = new PackagedProgram(new File(INPUT_SPLITS_PROG_JAR_FILE));

		TestEnvironment.setAsContext(
			testCluster,
			parallelism,
			Collections.singleton(new Path(INPUT_SPLITS_PROG_JAR_FILE)),
			Collections.<URL>emptyList());

		inputSplitTestProg.invokeInteractiveModeForExecution();
	}

	@Test
	public void testStreamingCustomSplitJobWithCustomClassLoader() throws IOException, ProgramInvocationException {
		PackagedProgram streamingInputSplitTestProg = new PackagedProgram(new File(STREAMING_INPUT_SPLITS_PROG_JAR_FILE));

		TestStreamEnvironment.setAsContext(
			testCluster,
			parallelism,
			Collections.singleton(new Path(STREAMING_INPUT_SPLITS_PROG_JAR_FILE)),
			Collections.<URL>emptyList());

		streamingInputSplitTestProg.invokeInteractiveModeForExecution();
	}

	@Test
	public void testCustomSplitJobWithCustomClassLoaderPath() throws IOException, ProgramInvocationException {
		URL classpath = new File(INPUT_SPLITS_PROG_JAR_FILE).toURI().toURL();
		PackagedProgram inputSplitTestProg2 = new PackagedProgram(new File(INPUT_SPLITS_PROG_JAR_FILE));

		TestEnvironment.setAsContext(
			testCluster,
			parallelism,
			Collections.<Path>emptyList(),
			Collections.singleton(classpath));

		inputSplitTestProg2.invokeInteractiveModeForExecution();
	}

	@Test
	public void testStreamingClassloaderJobWithCustomClassLoader() throws IOException, ProgramInvocationException {
		// regular streaming job
		PackagedProgram streamingProg = new PackagedProgram(new File(STREAMING_PROG_JAR_FILE));

		TestStreamEnvironment.setAsContext(
			testCluster,
			parallelism,
			Collections.singleton(new Path(STREAMING_PROG_JAR_FILE)),
			Collections.<URL>emptyList());

		streamingProg.invokeInteractiveModeForExecution();
	}

	@Test
	public void testCheckpointedStreamingClassloaderJobWithCustomClassLoader() throws IOException, ProgramInvocationException {
		// checkpointed streaming job with custom classes for the checkpoint (FLINK-2543)
		// the test also ensures that user specific exceptions are serializable between JobManager <--> JobClient.
		PackagedProgram streamingCheckpointedProg = new PackagedProgram(new File(STREAMING_CHECKPOINTED_PROG_JAR_FILE));

		TestStreamEnvironment.setAsContext(
			testCluster,
			parallelism,
			Collections.singleton(new Path(STREAMING_CHECKPOINTED_PROG_JAR_FILE)),
			Collections.<URL>emptyList());

		try {
			streamingCheckpointedProg.invokeInteractiveModeForExecution();
		} catch (Exception e) {
			// Program should terminate with a 'SuccessException':
			// the exception class is contained in the user-jar, but is not present on the maven classpath
			// the deserialization of the exception should thus fail here
			try {
				Optional<Throwable> exception = ExceptionUtils.findThrowable(e,
					candidate -> candidate.getClass().getCanonicalName().equals("org.apache.flink.test.classloading.jar.CheckpointedStreamingProgram.SuccessException"));

				// if we reach this point we either failed due to another exception,
				// or the deserialization of the user-exception did not fail
				if (!exception.isPresent()) {
					throw e;
				} else {
					Assert.fail("Deserialization of user exception should have failed.");
				}
			} catch (NoClassDefFoundError expected) {
				// expected
			}
		}
	}

	@Test
	public void testKMeansJobWithCustomClassLoader() throws IOException, ProgramInvocationException {
		PackagedProgram kMeansProg = new PackagedProgram(
			new File(KMEANS_JAR_PATH),
			new String[] {
				KMeansData.DATAPOINTS,
				KMeansData.INITIAL_CENTERS,
				"25"
			});

		TestEnvironment.setAsContext(
			testCluster,
			parallelism,
			Collections.singleton(new Path(KMEANS_JAR_PATH)),
			Collections.<URL>emptyList());

		kMeansProg.invokeInteractiveModeForExecution();
	}

	@Test
	public void testUserCodeTypeJobWithCustomClassLoader() throws IOException, ProgramInvocationException {
		PackagedProgram userCodeTypeProg = new PackagedProgram(new File(USERCODETYPE_JAR_PATH));

		TestEnvironment.setAsContext(
			testCluster,
			parallelism,
			Collections.singleton(new Path(USERCODETYPE_JAR_PATH)),
			Collections.<URL>emptyList());

		userCodeTypeProg.invokeInteractiveModeForExecution();
	}

	@Test
	public void testCheckpointingCustomKvStateJobWithCustomClassLoader() throws IOException, ProgramInvocationException {
		File checkpointDir = FOLDER.newFolder();
		File outputDir = FOLDER.newFolder();

		final PackagedProgram program = new PackagedProgram(
			new File(CHECKPOINTING_CUSTOM_KV_STATE_JAR_PATH),
			new String[] {
				checkpointDir.toURI().toString(),
				outputDir.toURI().toString()
			});

		TestStreamEnvironment.setAsContext(
			testCluster,
			parallelism,
			Collections.singleton(new Path(CHECKPOINTING_CUSTOM_KV_STATE_JAR_PATH)),
			Collections.<URL>emptyList());

		expectedException.expectCause(
			Matchers.<Throwable>hasProperty("cause", isA(SuccessException.class)));

		program.invokeInteractiveModeForExecution();
	}

	/**
	 * Tests disposal of a savepoint, which contains custom user code KvState.
	 */
	@Test
	public void testDisposeSavepointWithCustomKvState() throws Exception {
		ClusterClient<?> clusterClient = new MiniClusterClient(new Configuration(), testCluster);

		Deadline deadline = new FiniteDuration(100, TimeUnit.SECONDS).fromNow();

		File checkpointDir = FOLDER.newFolder();
		File outputDir = FOLDER.newFolder();

		final PackagedProgram program = new PackagedProgram(
				new File(CUSTOM_KV_STATE_JAR_PATH),
				new String[] {
						String.valueOf(parallelism),
						checkpointDir.toURI().toString(),
						"5000",
						outputDir.toURI().toString()
				});

		TestStreamEnvironment.setAsContext(
			testCluster,
			parallelism,
			Collections.singleton(new Path(CUSTOM_KV_STATE_JAR_PATH)),
			Collections.<URL>emptyList()
		);

		// Execute detached
		Thread invokeThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					program.invokeInteractiveModeForExecution();
				} catch (ProgramInvocationException ignored) {
					if (ignored.getCause() == null ||
						!(ignored.getCause() instanceof JobCancellationException)) {
						ignored.printStackTrace();
					}
				}
			}
		});

		LOG.info("Starting program invoke thread");
		invokeThread.start();

		// The job ID
		JobID jobId = null;

		LOG.info("Waiting for job status running.");

		// Wait for running job
		while (jobId == null && deadline.hasTimeLeft()) {

			Collection<JobStatusMessage> jobs = clusterClient.listJobs().get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
			for (JobStatusMessage job : jobs) {
				if (job.getJobState() == JobStatus.RUNNING) {
					jobId = job.getJobId();
					LOG.info("Job running. ID: " + jobId);
					break;
				}
			}

			// Retry if job is not available yet
			if (jobId == null) {
				Thread.sleep(100L);
			}
		}

		// Trigger savepoint
		String savepointPath = null;
		for (int i = 0; i < 20; i++) {
			LOG.info("Triggering savepoint (" + (i + 1) + "/20).");
			try {
				savepointPath = clusterClient.triggerSavepoint(jobId, null)
					.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);
			} catch (Exception cause) {
				LOG.info("Failed to trigger savepoint. Retrying...", cause);
				// This can fail if the operators are not opened yet
				Thread.sleep(500);
			}
		}

		assertNotNull("Failed to trigger savepoint", savepointPath);

		clusterClient.disposeSavepoint(savepointPath).get();

		clusterClient.cancel(jobId);

		// make sure, the execution is finished to not influence other test methods
		invokeThread.join(deadline.timeLeft().toMillis());
		assertFalse("Program invoke thread still running", invokeThread.isAlive());
	}
}
