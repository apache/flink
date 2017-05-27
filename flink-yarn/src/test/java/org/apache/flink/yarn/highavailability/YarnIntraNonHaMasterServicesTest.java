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

package org.apache.flink.yarn.highavailability;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.util.Random;
import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for YarnIntraNonHaMasterServices.
 */
public class YarnIntraNonHaMasterServicesTest extends TestLogger {

	private static final Random RND = new Random();

	@ClassRule
	public static final TemporaryFolder TEMP_DIR = new TemporaryFolder();

	private static MiniDFSCluster hdfsCluster;

	private static Path hdfsRootPath;

	private org.apache.hadoop.conf.Configuration hadoopConfig;

	// ------------------------------------------------------------------------
	//  Test setup and shutdown
	// ------------------------------------------------------------------------

	@BeforeClass
	public static void createHDFS() throws Exception {
		Assume.assumeTrue(!OperatingSystem.isWindows());

		final File tempDir = TEMP_DIR.newFolder();

		org.apache.hadoop.conf.Configuration hdConf = new org.apache.hadoop.conf.Configuration();
		hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.getAbsolutePath());

		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
		hdfsCluster = builder.build();
		hdfsRootPath = new Path(hdfsCluster.getURI());
	}

	@AfterClass
	public static void destroyHDFS() {
		if (hdfsCluster != null) {
			hdfsCluster.shutdown();
		}
		hdfsCluster = null;
		hdfsRootPath = null;
	}

	@Before
	public void initConfig() {
		hadoopConfig = new org.apache.hadoop.conf.Configuration();
		hadoopConfig.set(org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY, hdfsRootPath.toString());
	}

	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------

	@Test
	public void testRepeatedClose() throws Exception {
		final Configuration flinkConfig = new Configuration();

		final YarnHighAvailabilityServices services = new YarnIntraNonHaMasterServices(flinkConfig, hadoopConfig);
		services.closeAndCleanupAllData();

		// this should not throw an exception
		services.close();
	}

	@Test
	public void testClosingReportsToLeader() throws Exception {
		final Configuration flinkConfig = new Configuration();

		try (YarnHighAvailabilityServices services = new YarnIntraNonHaMasterServices(flinkConfig, hadoopConfig)) {
			final LeaderElectionService elector = services.getResourceManagerLeaderElectionService();
			final LeaderRetrievalService retrieval = services.getResourceManagerLeaderRetriever();
			final LeaderContender contender = mockContender(elector);
			final LeaderRetrievalListener listener = mock(LeaderRetrievalListener.class);

			elector.start(contender);
			retrieval.start(listener);

			// wait until the contender has become the leader
			verify(listener, timeout(1000L).times(1)).notifyLeaderAddress(anyString(), any(UUID.class));

			// now we can close the election service
			services.close();

			verify(contender, timeout(1000L).times(1)).handleError(any(Exception.class));
		}
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private static LeaderContender mockContender(final LeaderElectionService service) {
		String address = StringUtils.getRandomString(RND, 5, 10, 'a', 'z');
		return mockContender(service, address);
	}

	private static LeaderContender mockContender(final LeaderElectionService service, final String address) {
		LeaderContender mockContender = mock(LeaderContender.class);

		when(mockContender.getAddress()).thenReturn(address);

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				final UUID uuid = (UUID) invocation.getArguments()[0];
				service.confirmLeaderSessionID(uuid);
				return null;
			}
		}).when(mockContender).grantLeadership(any(UUID.class));

		return mockContender;
	}
}
