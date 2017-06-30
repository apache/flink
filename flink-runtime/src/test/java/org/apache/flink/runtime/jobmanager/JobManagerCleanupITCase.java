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
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.execution.ExecutionState;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.InetSocketAddress;

import static org.apache.flink.runtime.testingUtils.TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/**
 * Small test to check that the {@link org.apache.flink.runtime.blob.BlobServer} cleanup is executed
 * after job termination.
 */
public class JobManagerCleanupITCase {

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
	 * Test cleanup for a job that finishes ordinarily.
	 */
	@Test
	public void testBlobServerCleanupFinishedJob() throws IOException {
		testBlobServerCleanup(ExecutionState.FINISHED);
	}

	/**
	 * Test cleanup for a job which is cancelled after submission.
	 */
	@Test
	public void testBlobServerCleanupCancelledJob() throws IOException {
		testBlobServerCleanup(ExecutionState.CANCELED);
	}

	/**
	 * Test cleanup for a job that fails (first a task fails, then the job recovers, then the whole
	 * job fails due to a limited restart policy).
	 */
	@Test
	public void testBlobServerCleanupFailedJob() throws IOException {
		testBlobServerCleanup(ExecutionState.FAILED);
	}

	private void testBlobServerCleanup(final ExecutionState finalState) throws IOException {
		final int num_tasks = 2;
		final File blobBaseDir = tmpFolder.newFolder();

		new JavaTestKit(system) {{
			new Within(duration("15 seconds")) {
				@Override
				protected void run() {
					// Setup

					TestingCluster cluster = null;
					BlobClient bc = null;

					try {
						Configuration config = new Configuration();
						config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 2);
						config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
						config.setString(AkkaOptions.ASK_TIMEOUT, DEFAULT_AKKA_ASK_TIMEOUT());
						config.setString(BlobServerOptions.STORAGE_DIRECTORY, blobBaseDir.getAbsolutePath());

						config.setString(ConfigConstants.RESTART_STRATEGY, "fixeddelay");
						config.setInteger(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
						config.setString(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY, "1 s");
						// BLOBs are deleted from BlobCache between 1s and 2s after last reference
						// -> the BlobCache may still have the BLOB or not (let's test both cases randomly)
						config.setLong(ConfigConstants.LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL, 1L);

						cluster = new TestingCluster(config);
						cluster.start();

						final ActorGateway jobManagerGateway = cluster.getLeaderGateway(
							TestingUtils.TESTING_DURATION());

						// we can set the leader session ID to None because we don't use this gateway to send messages
						final ActorGateway testActorGateway = new AkkaActorGateway(getTestActor(),
							HighAvailabilityServices.DEFAULT_LEADER_ID);

						// Create a task

						JobVertex source = new JobVertex("Source");
						if (finalState == ExecutionState.FAILED || finalState == ExecutionState.CANCELED) {
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
						BlobKey key1;
						bc = new BlobClient(new InetSocketAddress("localhost", blobPort), config);
						try {
							key1 = bc.put(jid, new byte[10]);
						} finally {
							bc.close();
						}
						jobGraph.addBlob(key1);

						// Submit the job and wait for all vertices to be running
						jobManagerGateway.tell(
							new JobManagerMessages.SubmitJob(
								jobGraph,
								ListeningBehaviour.EXECUTION_RESULT),
							testActorGateway);
						expectMsgClass(JobManagerMessages.JobSubmitSuccess.class);


						if (finalState == ExecutionState.FAILED) {
							// fail a task so that the job is going to be recovered (we actually do not
							// need the blocking part of the invokable and can start throwing right away)
							FailingBlockingInvokable.unblock();

							// job will get restarted, BlobCache may re-download the BLOB if already deleted
							// then the tasks will fail again and the restart strategy will finalise the job

							expectMsgClass(JobManagerMessages.JobResultFailure.class);
						} else if (finalState == ExecutionState.CANCELED) {
							jobManagerGateway.tell(
								new JobManagerMessages.CancelJob(jid),
								testActorGateway);
							expectMsgClass(JobManagerMessages.CancellationResponse.class);

							// job will be cancelled and everything should be cleaned up

							expectMsgClass(JobManagerMessages.JobResultFailure.class);
						} else {
							expectMsgClass(JobManagerMessages.JobResultSuccess.class);
						}

						// both BlobServer and BlobCache should eventually delete all files

						File[] blobDirs = blobBaseDir.listFiles(new FilenameFilter() {
							@Override
							public boolean accept(File dir, String name) {
								return dir.getName().startsWith("blobStore-");
							}
						});
						assertNotNull(blobDirs);
						for (File blobDir : blobDirs) {
							waitForEmptyBlobDir(blobDir);
						}

					} catch (Exception e) {
						e.printStackTrace();
						fail(e.getMessage());
					} finally {
						if (bc != null) {
							try {
								bc.close();
							} catch (IOException ignored) {
							}
						}
						if (cluster != null) {
							cluster.shutdown();
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
	 * 		org.apache.flink.runtime.blob.BlobCache}
	 *
	 * @see org.apache.flink.runtime.blob.BlobUtils
	 */
	private static void waitForEmptyBlobDir(File blobDir) {
		while (true) {
			String[] blobDirContents = blobDir.list(new FilenameFilter() {
				@Override
				public boolean accept(File dir, String name) {
					return dir.getName().startsWith("job_");
				}
			});
			if (blobDirContents == null || blobDirContents.length == 0) {
				break;
			}
		}
	}
}
