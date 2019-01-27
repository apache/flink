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

package org.apache.flink.runtime.highavailability.filesystem;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests for the {@link FileSystemHaServices}.
 */
public class FileSystemHaServicesTest extends TestLogger {

	private final String jobManagerAddress = "jobManager";
	private final String dispatcherAddress = "dispatcher";
	private final String resourceManagerAddress = "resourceManager";
	private final String webMonitorAddress = "webMonitor";
	private String workingDir;

	private FileSystemHaServices filesystemHaServices;

	@Before
	public void setupTest() throws IOException {
		workingDir =
			System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + UUID.randomUUID().toString();

		Configuration configuration = new Configuration();
		configuration.setString(HighAvailabilityOptions.HA_MODE, "filesystem");
		configuration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, workingDir);
		BlobStoreService blobStoreService = BlobUtils.createBlobStoreFromConfig(configuration);

		filesystemHaServices = new FileSystemHaServices(
			resourceManagerAddress,
			dispatcherAddress,
			jobManagerAddress,
			webMonitorAddress,
			workingDir,
			blobStoreService);
	}

	@After
	public void teardownTest() throws Exception {
		if (filesystemHaServices != null) {
			filesystemHaServices.closeAndCleanupAllData();
			filesystemHaServices = null;
		}

		FileUtils.deleteDirectory(new File(workingDir));
	}

	/**
	 * Tests that the standalone leader election services return a fixed address and leader session
	 * id.
	 */
	@Test
	public void testLeaderElection() throws Exception {
		JobID jobId = new JobID();
		LeaderContender jmLeaderContender = mock(LeaderContender.class);
		LeaderContender rmLeaderContender = mock(LeaderContender.class);

		LeaderElectionService jmLeaderElectionService = filesystemHaServices.getJobManagerLeaderElectionService(jobId);
		LeaderElectionService rmLeaderElectionService = filesystemHaServices.getResourceManagerLeaderElectionService();

		jmLeaderElectionService.start(jmLeaderContender);
		rmLeaderElectionService.start(rmLeaderContender);

		verify(jmLeaderContender).grantLeadership(eq(HighAvailabilityServices.DEFAULT_LEADER_ID));
		verify(rmLeaderContender).grantLeadership(eq(HighAvailabilityServices.DEFAULT_LEADER_ID));
	}

	/**
	 * Tests that the standalone leader retrieval services return the specified address and the
	 * fixed leader session id.
	 */
	@Test
	public void testJobManagerLeaderRetrieval() throws Exception {
		JobID jobId1 = new JobID();
		JobID jobId2 = new JobID();
		LeaderRetrievalListener jmListener1 = mock(LeaderRetrievalListener.class);
		LeaderRetrievalListener jmListener2 = mock(LeaderRetrievalListener.class);
		LeaderRetrievalListener rmListener = mock(LeaderRetrievalListener.class);

		LeaderRetrievalService jmLeaderRetrievalService1 = filesystemHaServices.getJobManagerLeaderRetriever(jobId1);
		LeaderRetrievalService jmLeaderRetrievalService2 = filesystemHaServices.getJobManagerLeaderRetriever(jobId2);
		LeaderRetrievalService rmLeaderRetrievalService = filesystemHaServices.getResourceManagerLeaderRetriever();

		jmLeaderRetrievalService1.start(jmListener1);
		jmLeaderRetrievalService2.start(jmListener2);
		rmLeaderRetrievalService.start(rmListener);

		verify(jmListener1).notifyLeaderAddress(eq(jobManagerAddress), eq(HighAvailabilityServices.DEFAULT_LEADER_ID));
		verify(jmListener2).notifyLeaderAddress(eq(jobManagerAddress), eq(HighAvailabilityServices.DEFAULT_LEADER_ID));
		verify(rmListener).notifyLeaderAddress(eq(resourceManagerAddress), eq(HighAvailabilityServices.DEFAULT_LEADER_ID));
	}

	/**
	 * Tests that the standalone leader retrieval services return the given address and the
	 * fixed leader session id.
	 */
	@Test
	public void testJobManagerLeaderRetrievalFlip6() throws Exception {
		JobID jobId1 = new JobID();
		JobID jobId2 = new JobID();
		final String jobManagerAddress1 = "foobar";
		final String jobManagerAddress2 = "barfoo";
		LeaderRetrievalListener jmListener1 = mock(LeaderRetrievalListener.class);
		LeaderRetrievalListener jmListener2 = mock(LeaderRetrievalListener.class);

		LeaderRetrievalService jmLeaderRetrievalService1 = filesystemHaServices.getJobManagerLeaderRetriever(jobId1, jobManagerAddress1);
		LeaderRetrievalService jmLeaderRetrievalService2 = filesystemHaServices.getJobManagerLeaderRetriever(jobId2, jobManagerAddress2);

		jmLeaderRetrievalService1.start(jmListener1);
		jmLeaderRetrievalService2.start(jmListener2);

		verify(jmListener1).notifyLeaderAddress(eq(jobManagerAddress1), eq(HighAvailabilityServices.DEFAULT_LEADER_ID));
		verify(jmListener2).notifyLeaderAddress(eq(jobManagerAddress2), eq(HighAvailabilityServices.DEFAULT_LEADER_ID));
	}
}
