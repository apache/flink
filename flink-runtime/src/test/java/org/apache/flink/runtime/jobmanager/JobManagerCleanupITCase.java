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

package org.apache.flink.runtime.jobmanager;

import akka.actor.ActorSystem;
import akka.testkit.JavaTestKit;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.testingUtils.TestingCluster;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.FailingBlockingInvokable;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.runtime.testingUtils.TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Small test to check that the {@link org.apache.flink.runtime.blob.BlobServer} cleanup is executed
 * after job termination.
 */
public class JobManagerCleanupITCase extends TestLogger {

	@Rule
	public TemporaryFolder tmpFolder = new TemporaryFolder();

	private static ActorSystem system;

	@BeforeClass
	public static void setup() {
		system = AkkaUtils.createLocalActorSystem(new Configuration());
	}

	@AfterClass
	public static void teardown() {
		JavaTestKit.shutdownActorSystem(system);
	}

	/**
	 * Specifies which test case to run in {@link #testBlobServerCleanup(TestCase)}.
	 */
	private enum TestCase {
		JOB_FINISHES_SUCESSFULLY,
		JOB_IS_CANCELLED,
		JOB_FAILS,
		JOB_SUBMISSION_FAILS
	}

	/**
	 * Test cleanup for a job that finishes ordinarily.
	 */
	@Test
	public void testBlobServerCleanupFinishedJob() throws IOException {
		testBlobServerCleanup(TestCase.JOB_FINISHES_SUCESSFULLY);
	}

	/**
	 * Test cleanup for a job which is cancelled after submission.
	 */
	@Test
	public void testBlobServerCleanupCancelledJob() throws IOException {
		testBlobServerCleanup(TestCase.JOB_IS_CANCELLED);
	}

	/**
	 * Test cleanup for a job that fails (first a task fails, then the job recovers, then the whole
	 * job fails due to a limited restart policy).
	 */
	@Test
	public void testBlobServerCleanupFailedJob() throws IOException {
		testBlobServerCleanup(TestCase.JOB_FAILS);
	}

	/**
	 * Test cleanup for a job that fails job submission (emulated by an additional BLOB not being
	 * present).
	 */
	@Test
	public void testBlobServerCleanupFailedSubmission() throws IOException {
		testBlobServerCleanup(TestCase.JOB_SUBMISSION_FAILS);
	}

	private void testBlobServerCleanup(final TestCase testCase) throws IOException {
		final int num_tasks = 2;
		final File blobBaseDir = tmpFolder.newFolder();

		new JavaTestKit(system) {{
			new Within(duration("30 seconds")) {
				@Override
				protected void run() {
					// Setup

					TestingCluster cluster = null;
					File tempBlob = null;

					try {
						Configuration config = new Configuration();
						config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 2);
						config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
						config.setString(AkkaOptions.ASK_TIMEOUT, DEFAULT_AKKA_ASK_TIMEOUT());
						config.setString(BlobServerOptions.STORAGE_DIRECTORY, blobBaseDir.getAbsolutePath());

						config.setString(ConfigConstants.RESTART_STRATEGY, "fixeddelay");
						config.setInteger(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
						config.setString(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY, "1 s");
						// BLOBs are deleted from BlobCache between 1s and 2s after last reference
						// -> the BlobCache may still have the BLOB or not (let's test both cases randomly)
						config.setLong(BlobServerOptions.CLEANUP_INTERVAL, 1L);

						cluster = new TestingCluster(config);
						cluster.start();

						final ActorGateway jobManagerGateway = cluster.getLeaderGateway(
							TestingUtils.TESTING_DURATION());

						// we can set the leader session ID to None because we don't use this gateway to send messages
						final ActorGateway testActorGateway = new AkkaActorGateway(getTestActor(),
							HighAvailabilityServices.DEFAULT_LEADER_ID);

						// Create a task

						JobVertex source = new JobVertex("Source");
						if (testCase == TestCase.JOB_FAILS || testCase == TestCase.JOB_IS_CANCELLED) {
							source.setInvokableClass(FailingBlockingInvokable.class);
						} else {
							source.setInvokableClass(NoOpInvokable.class);
						}
						source.setParallelism(num_tasks);

						JobGraph jobGraph = new JobGraph("BlobCleanupTest", source);
						final JobID jid = jobGraph.getJobID();

						// request the blob port from the job manager
						Future<Object> future = jobManagerGateway
							.ask(JobManagerMessages.getRequestBlobManagerPort(), remaining());
						int blobPort = (Integer) Await.result(future, remaining());

						// upload a blob
						tempBlob = File.createTempFile("Required", ".jar");
						List<PermanentBlobKey> keys =
							BlobClient.uploadFiles(new InetSocketAddress("localhost", blobPort),
								config, jid,
								Collections.singletonList(new Path(tempBlob.getAbsolutePath())));
						assertEquals(1, keys.size());
						jobGraph.addUserJarBlobKey(keys.get(0));

						if (testCase == TestCase.JOB_SUBMISSION_FAILS) {
							// add an invalid key so that the submission fails
							jobGraph.addUserJarBlobKey(new PermanentBlobKey());
						}

						// Submit the job and wait for all vertices to be running
						jobManagerGateway.tell(
							new JobManagerMessages.SubmitJob(
								jobGraph,
								// NOTE: to not receive two different (arbitrarily ordered) messages
								//       upon cancellation, only listen for the job submission
								//       message when cancelling the job
								testCase == TestCase.JOB_IS_CANCELLED ?
									ListeningBehaviour.DETACHED :
									ListeningBehaviour.EXECUTION_RESULT
							),
							testActorGateway);
						if (testCase == TestCase.JOB_SUBMISSION_FAILS) {
							expectMsgClass(JobManagerMessages.JobResultFailure.class);
						} else {
							expectMsgEquals(new JobManagerMessages.JobSubmitSuccess(jid));

							if (testCase == TestCase.JOB_FAILS) {
								// fail a task so that the job is going to be recovered (we actually do not
								// need the blocking part of the invokable and can start throwing right away)
								FailingBlockingInvokable.unblock();

								// job will get restarted, BlobCache may re-download the BLOB if already deleted
								// then the tasks will fail again and the restart strategy will finalise the job

								expectMsgClass(JobManagerMessages.JobResultFailure.class);
							} else if (testCase == TestCase.JOB_IS_CANCELLED) {
								jobManagerGateway.tell(
									new JobManagerMessages.CancelJob(jid),
									testActorGateway);

								expectMsgEquals(new JobManagerMessages.CancellationSuccess(jid, null));
							} else {
								expectMsgClass(JobManagerMessages.JobResultSuccess.class);
							}
						}

						// both BlobServer and BlobCache should eventually delete all files

						File[] blobDirs = blobBaseDir.listFiles(new FilenameFilter() {
							@Override
							public boolean accept(File dir, String name) {
								return name.startsWith("blobStore-");
							}
						});
						assertNotNull(blobDirs);
						for (File blobDir : blobDirs) {
							waitForEmptyBlobDir(blobDir, remaining());
						}

					} catch (Exception e) {
						e.printStackTrace();
						fail(e.getMessage());
					} finally {
						if (cluster != null) {
							cluster.stop();
						}
						if (tempBlob != null) {
							assertTrue(tempBlob.delete());
						}
					}
				}
			};
		}};

		// after everything has been shut down, the storage directory itself should be empty
		assertArrayEquals(new File[] {}, blobBaseDir.listFiles());
	}

	/**
	 * Waits until the given {@link org.apache.flink.runtime.blob.BlobService} storage directory
	 * does not contain any job-related folders any more.
	 *
	 * @param blobDir
	 * 		directory of a {@link org.apache.flink.runtime.blob.BlobServer} or {@link
	 * 		org.apache.flink.runtime.blob.BlobCacheService}
	 * @param remaining
	 * 		remaining time for this test
	 *
	 * @see org.apache.flink.runtime.blob.BlobUtils
	 */
	private static void waitForEmptyBlobDir(File blobDir, FiniteDuration remaining)
		throws InterruptedException {
		long deadline = System.currentTimeMillis() + remaining.toMillis();
		String[] blobDirContents;
		do {
			blobDirContents = blobDir.list(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return name.startsWith("job_");
				}
			});
			if (blobDirContents == null || blobDirContents.length == 0) {
				return;
			}
			Thread.sleep(100);
		} while (System.currentTimeMillis() < deadline);

		fail("Timeout while waiting for " + blobDir.getAbsolutePath() + " to become empty. Current contents: " + Arrays.toString(blobDirContents));
	}
}
