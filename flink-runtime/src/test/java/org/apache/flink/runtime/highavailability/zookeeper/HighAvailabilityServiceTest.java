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

package org.apache.flink.runtime.highavailability.zookeeper;

import akka.actor.ActorRef;
import org.apache.curator.test.TestingServer;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.blob.FileSystemBlobStore;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobInfo;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.TestLogger;

import org.apache.zookeeper.KeeperException;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.junit.Assert;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class HighAvailabilityServiceTest extends TestLogger {
	private TestingServer testingServer;
	private HighAvailabilityServices zkHaService;
	private SubmittedJobGraphStore submittedJobGraphStore;

	@Rule
	public ExpectedException thrown = ExpectedException.none();
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception {
		testingServer = new TestingServer();

		Configuration configuration = new Configuration();
		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, testingServer.getConnectString());
		configuration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
		configuration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, temporaryFolder.getRoot().getPath());
		zkHaService = new ZooKeeperHaServices(
						ZooKeeperUtils.startCuratorFramework(configuration),
						Executors.directExecutor(),
						configuration,
						new FileSystemBlobStore(FileSystem.getLocalFileSystem(), "local"));

		submittedJobGraphStore = zkHaService.getSubmittedJobGraphStore();
		submittedJobGraphStore.start(new SubmittedJobGraphStore.SubmittedJobGraphListener() {
			@Override
			public void onAddedJobGraph(JobID jobId) {

			}

			@Override
			public void onRemovedJobGraph(JobID jobId) {

			}
		});
	}

	@After
	public void after() throws Exception {
		testingServer.stop();
		testingServer = null;

		submittedJobGraphStore.stop();
		submittedJobGraphStore = null;

		zkHaService.closeAndCleanupAllData();
		zkHaService = null;
	}

	/**
	 * Tests for that the function of cleanupData(JobID) in SubmittedJobGraph
	 */
	@Test
	public void testCleanSubmittedJobGraphStore() throws Throwable {
		SubmittedJobGraph jobGraph1 = new SubmittedJobGraph(
						new JobGraph("testSubmittedJob1"),
						new JobInfo(ActorRef.noSender(), ListeningBehaviour.DETACHED, 0, Integer.MAX_VALUE));
		SubmittedJobGraph jobGraph2 = new SubmittedJobGraph(
						new JobGraph("testSubmittedJob2"),
						new JobInfo(ActorRef.noSender(), ListeningBehaviour.DETACHED, 0, Integer.MAX_VALUE));
		submittedJobGraphStore.putJobGraph(jobGraph1);
		submittedJobGraphStore.putJobGraph(jobGraph2);

		zkHaService.cleanupData(jobGraph1.getJobId());

		SubmittedJobGraph recoverJobGraph2 = submittedJobGraphStore.recoverJobGraph(jobGraph2.getJobId());
		Assert.assertEquals(recoverJobGraph2.getJobId(), jobGraph2.getJobId());
		thrown.expect(new ExceptionCauseMatcher());
		thrown.expectMessage("Could not retrieve the submitted job graph state handle for /" +
			jobGraph1.getJobId().toString() + "from the submitted job graph store");
		submittedJobGraphStore.recoverJobGraph(jobGraph1.getJobId());
	}

	private class ExceptionCauseMatcher extends TypeSafeMatcher<Exception> {

		@Override
		protected boolean matchesSafely(Exception exception) {
			return exception.getCause().getCause() instanceof KeeperException.NoNodeException;
		}

		@Override
		public void describeTo(Description description) {
		}
	}
}
