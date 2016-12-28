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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.util.TestLogger;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.mockito.Mockito.*;


public class YarnPreConfiguredMasterHaServicesTest extends TestLogger {

	@ClassRule
	public static final TemporaryFolder TEMP_DIR = new TemporaryFolder();

	private static MiniDFSCluster HDFS_CLUSTER;

	private static Path HDFS_ROOT_PATH;

	private org.apache.hadoop.conf.Configuration hadoopConfig;

	// ------------------------------------------------------------------------
	//  Test setup and shutdown
	// ------------------------------------------------------------------------

	@BeforeClass
	public static void createHDFS() throws Exception {
		final File tempDir = TEMP_DIR.newFolder();

		org.apache.hadoop.conf.Configuration hdConf = new org.apache.hadoop.conf.Configuration();
		hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.getAbsolutePath());

		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
		HDFS_CLUSTER = builder.build();
		HDFS_ROOT_PATH = new Path(HDFS_CLUSTER.getURI());
	}

	@AfterClass
	public static void destroyHDFS() {
		if (HDFS_CLUSTER != null) {
			HDFS_CLUSTER.shutdown();
		}
		HDFS_CLUSTER = null;
		HDFS_ROOT_PATH = null;
	}

	@Before
	public void initConfig() {
		hadoopConfig = new org.apache.hadoop.conf.Configuration();
		hadoopConfig.set(org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY, HDFS_ROOT_PATH.toString());
	}

	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------

	@Test
	public void testConstantResourceManagerName() throws Exception {
		final Configuration flinkConfig = new Configuration();
		flinkConfig.setString(YarnConfigOptions.APP_MASTER_RPC_ADDRESS, "localhost");
		flinkConfig.setInteger(YarnConfigOptions.APP_MASTER_RPC_PORT, 1427);

		YarnHighAvailabilityServices services1 = new YarnPreConfiguredMasterNonHaServices(flinkConfig, hadoopConfig);
		YarnHighAvailabilityServices services2 = new YarnPreConfiguredMasterNonHaServices(flinkConfig, hadoopConfig);

		try {
			String rmName1 = services1.getResourceManagerEndpointName();
			String rmName2 = services2.getResourceManagerEndpointName();

			assertNotNull(rmName1);
			assertNotNull(rmName2);
			assertEquals(rmName1, rmName2);
		}
		finally {
			services1.closeAndCleanupAllData();
			services2.closeAndCleanupAllData();
		}
	}

	@Test
	public void testMissingRmConfiguration() throws Exception {
		final Configuration flinkConfig = new Configuration();

		// missing resource manager address
		try {
			new YarnPreConfiguredMasterNonHaServices(flinkConfig, hadoopConfig);
			fail();
		} catch (IllegalConfigurationException e) {
			// expected
		}

		flinkConfig.setString(YarnConfigOptions.APP_MASTER_RPC_ADDRESS, "localhost");

		// missing resource manager port
		try {
			new YarnPreConfiguredMasterNonHaServices(flinkConfig, hadoopConfig);
			fail();
		} catch (IllegalConfigurationException e) {
			// expected
		}

		flinkConfig.setInteger(YarnConfigOptions.APP_MASTER_RPC_PORT, 1427);

		// now everything is good ;-)
		new YarnPreConfiguredMasterNonHaServices(flinkConfig, hadoopConfig).closeAndCleanupAllData();
	}

	@Test
	public void testCloseAndCleanup() throws Exception {
		final Configuration flinkConfig = new Configuration();
		flinkConfig.setString(YarnConfigOptions.APP_MASTER_RPC_ADDRESS, "localhost");
		flinkConfig.setInteger(YarnConfigOptions.APP_MASTER_RPC_PORT, 1427);

		// create the services
		YarnHighAvailabilityServices services = new YarnPreConfiguredMasterNonHaServices(flinkConfig, hadoopConfig);
		services.closeAndCleanupAllData();

		final FileSystem fileSystem = HDFS_ROOT_PATH.getFileSystem();
		final Path workDir = new Path(HDFS_CLUSTER.getFileSystem().getWorkingDirectory().toString());
		
		try {
			fileSystem.getFileStatus(new Path(workDir, YarnHighAvailabilityServices.FLINK_RECOVERY_DATA_DIR));
			fail("Flink recovery data directory still exists");
		}
		catch (FileNotFoundException e) {
			// expected, because the directory should have been cleaned up
		}

		assertTrue(services.isClosed());

		// doing another cleanup when the services are closed should fail
		try {
			services.closeAndCleanupAllData();
			fail("should fail with an IllegalStateException");
		} catch (IllegalStateException e) {
			// expected
		}
	}

	@Test
	public void testCallsOnClosedServices() throws Exception {
		final Configuration flinkConfig = new Configuration();
		flinkConfig.setString(YarnConfigOptions.APP_MASTER_RPC_ADDRESS, "localhost");
		flinkConfig.setInteger(YarnConfigOptions.APP_MASTER_RPC_PORT, 1427);

		YarnHighAvailabilityServices services = new YarnPreConfiguredMasterNonHaServices(flinkConfig, hadoopConfig);

		// this method is not supported
		try {
			services.getSubmittedJobGraphStore();
			fail();
		} catch (UnsupportedOperationException ignored) {}


		services.close();

		// all these methods should fail now

		try {
			services.createBlobStore();
			fail();
		} catch (IllegalStateException ignored) {}

		try {
			services.getCheckpointRecoveryFactory();
			fail();
		} catch (IllegalStateException ignored) {}

		try {
			services.getJobManagerLeaderElectionService(new JobID());
			fail();
		} catch (IllegalStateException ignored) {}

		try {
			services.getJobManagerLeaderRetriever(new JobID());
			fail();
		} catch (IllegalStateException ignored) {}

		try {
			services.getRunningJobsRegistry();
			fail();
		} catch (IllegalStateException ignored) {}

		try {
			services.getResourceManagerLeaderElectionService();
			fail();
		} catch (IllegalStateException ignored) {}

		try {
			services.getResourceManagerLeaderRetriever();
			fail();
		} catch (IllegalStateException ignored) {}
	}
}
