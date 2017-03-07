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
import java.net.URI;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.TestBaseUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import static junit.framework.Assert.fail;

public class SavepointMigrationTestBase extends TestBaseUtils {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	private static final Logger LOG = LoggerFactory.getLogger(SavepointMigrationTestBase.class);
	private static final Deadline DEADLINE = new FiniteDuration(5, TimeUnit.MINUTES).fromNow();
	protected static final int DEFAULT_PARALLELISM = 4;
	protected LocalFlinkMiniCluster cluster = null;

	protected static String getResourceFilename(String filename) {
		ClassLoader cl = SavepointMigrationTestBase.class.getClassLoader();
		URL resource = cl.getResource(filename);
		if (resource == null) {
			throw new NullPointerException("Missing snapshot resource.");
		}
		return resource.getFile();
	}

	@Before
	public void setup() throws Exception {

		// Flink configuration
		final Configuration config = new Configuration();

		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, DEFAULT_PARALLELISM);

		final File checkpointDir = tempFolder.newFolder("checkpoints").getAbsoluteFile();
		final File savepointDir = tempFolder.newFolder("savepoints").getAbsoluteFile();

		if (!checkpointDir.exists() || !savepointDir.exists()) {
			throw new Exception("Test setup failed: failed to create (temporary) directories.");
		}

		LOG.info("Created temporary checkpoint directory: " + checkpointDir + ".");
		LOG.info("Created savepoint directory: " + savepointDir + ".");

		config.setString(CoreOptions.STATE_BACKEND, "memory");
		config.setString(FsStateBackendFactory.CHECKPOINT_DIRECTORY_URI_CONF_KEY, checkpointDir.toURI().toString());
		config.setString(FsStateBackendFactory.MEMORY_THRESHOLD_CONF_KEY, "0");
		config.setString("state.savepoints.dir", savepointDir.toURI().toString());

		cluster = TestBaseUtils.startCluster(config, false);
	}

	@After
	public void teardown() throws Exception {
		stopCluster(cluster, TestBaseUtils.DEFAULT_TIMEOUT);
	}

	@SafeVarargs
	protected final void executeAndSavepoint(
			StreamExecutionEnvironment env,
			String savepointPath,
			Tuple2<String, Integer>... expectedAccumulators) throws Exception {

		// Retrieve the job manager
		ActorGateway jobManager = Await.result(cluster.leaderGateway().future(), DEADLINE.timeLeft());

		// Submit the job
		JobGraph jobGraph = env.getStreamGraph().getJobGraph();


		JobSubmissionResult jobSubmissionResult = cluster.submitJobDetached(jobGraph);

		LOG.info("Submitted job {} and waiting...", jobSubmissionResult.getJobID());

		StandaloneClusterClient clusterClient = new StandaloneClusterClient(cluster.configuration());

		boolean done = false;
		while (DEADLINE.hasTimeLeft()) {
			Thread.sleep(100);
			Map<String, Object> accumulators = clusterClient.getAccumulators(jobSubmissionResult.getJobID());

			boolean allDone = true;
			for (Tuple2<String, Integer> acc : expectedAccumulators) {
				Integer numFinished = (Integer) accumulators.get(acc.f0);
				if (numFinished == null) {
					allDone = false;
					break;
				}
				if (!numFinished.equals(acc.f1)) {
					allDone = false;
					break;
				}
			}
			if (allDone) {
				done = true;
				break;
			}
		}

		if (!done) {
			fail("Did not see the expected accumulator results within time limit.");
		}

		LOG.info("Triggering savepoint.");
		// Flink 1.2
		final Future<Object> savepointResultFuture =
				jobManager.ask(new JobManagerMessages.TriggerSavepoint(jobSubmissionResult.getJobID(), Option.<String>empty()), DEADLINE.timeLeft());

		// Flink 1.1
//		final Future<Object> savepointResultFuture =
//				jobManager.ask(new JobManagerMessages.TriggerSavepoint(jobSubmissionResult.getJobID()), DEADLINE.timeLeft());


		Object savepointResult = Await.result(savepointResultFuture, DEADLINE.timeLeft());

		if (savepointResult instanceof JobManagerMessages.TriggerSavepointFailure) {
			fail("Error drawing savepoint: " + ((JobManagerMessages.TriggerSavepointFailure) savepointResult).cause());
		}

		// jobmanager will store savepoint in heap, we have to retrieve it
		final String jobmanagerSavepointPath = ((JobManagerMessages.TriggerSavepointSuccess) savepointResult).savepointPath();
		LOG.info("Saved savepoint: " + jobmanagerSavepointPath);

		// Flink 1.2
		FileUtils.moveFile(new File(new URI(jobmanagerSavepointPath).getPath()), new File(savepointPath));

		// Flink 1.1
		// Retrieve the savepoint from the testing job manager
//		LOG.info("Requesting the savepoint.");
//		Future<Object> savepointFuture = jobManager.ask(new TestingJobManagerMessages.RequestSavepoint(jobmanagerSavepointPath), DEADLINE.timeLeft());
//
//		Savepoint savepoint = ((TestingJobManagerMessages.ResponseSavepoint) Await.result(savepointFuture, DEADLINE.timeLeft())).savepoint();
//		LOG.info("Retrieved savepoint: " + jobmanagerSavepointPath + ".");
//
//		LOG.info("Storing savepoint to file.");
//		Configuration config = new Configuration();
//		config.setString(org.apache.flink.runtime.checkpoint.savepoint.SavepointStoreFactory.SAVEPOINT_BACKEND_KEY, "filesystem");
//		config.setString(org.apache.flink.runtime.checkpoint.savepoint.SavepointStoreFactory.SAVEPOINT_DIRECTORY_KEY, "file:///Users/aljoscha/Downloads");
//		String path = org.apache.flink.runtime.checkpoint.savepoint.SavepointStoreFactory.createFromConfig(config).storeSavepoint(savepoint);
//
//		FileUtils.moveFile(new File(new URI(path).getPath()), new File(savepointPath));
	}

	@SafeVarargs
	protected final void restoreAndExecute(
			StreamExecutionEnvironment env,
			String savepointPath,
			Tuple2<String, Integer>... expectedAccumulators) throws Exception {

		// Retrieve the job manager
		Await.result(cluster.leaderGateway().future(), DEADLINE.timeLeft());

		// Submit the job
		JobGraph jobGraph = env.getStreamGraph().getJobGraph();

		jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath));

		JobSubmissionResult jobSubmissionResult = cluster.submitJobDetached(jobGraph);

		StandaloneClusterClient clusterClient = new StandaloneClusterClient(cluster.configuration());

		boolean done = false;
		while (DEADLINE.hasTimeLeft()) {
			Thread.sleep(100);
			Map<String, Object> accumulators = clusterClient.getAccumulators(jobSubmissionResult.getJobID());

			boolean allDone = true;
			for (Tuple2<String, Integer> acc : expectedAccumulators) {
				Integer numFinished = (Integer) accumulators.get(acc.f0);
				if (numFinished == null) {
					allDone = false;
					break;
				}
				if (!numFinished.equals(acc.f1)) {
					allDone = false;
					break;
				}
			}

			if (allDone) {
				done = true;
				break;
			}
		}

		if (!done) {
			fail("Did not see the expected accumulator results within time limit.");
		}
	}
}
