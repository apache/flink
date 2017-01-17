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

package org.apache.flink.hdfstests;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.FsNegativeRunningJobsRegistry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FsNegativeRunningJobsRegistryTest {

	@ClassRule
	public static final TemporaryFolder TEMP_DIR = new TemporaryFolder();

	private static MiniDFSCluster HDFS_CLUSTER;

	private static Path HDFS_ROOT_PATH;

	// ------------------------------------------------------------------------
	//  startup / shutdown
	// ------------------------------------------------------------------------

	@BeforeClass
	public static void createHDFS() throws Exception {
		final File tempDir = TEMP_DIR.newFolder();

		Configuration hdConf = new Configuration();
		hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.getAbsolutePath());

		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
		HDFS_CLUSTER = builder.build();

		HDFS_ROOT_PATH = new Path("hdfs://" + HDFS_CLUSTER.getURI().getHost() + ":"
				+ HDFS_CLUSTER.getNameNodePort() + "/");
	}

	@AfterClass
	public static void destroyHDFS() {
		if (HDFS_CLUSTER != null) {
			HDFS_CLUSTER.shutdown();
		}
		HDFS_CLUSTER = null;
		HDFS_ROOT_PATH = null;
	}

	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------
	
	@Test
	public void testCreateAndSetFinished() throws Exception {
		final Path workDir = new Path(HDFS_ROOT_PATH, "test-work-dir");
		final JobID jid = new JobID();

		FsNegativeRunningJobsRegistry registry = new FsNegativeRunningJobsRegistry(workDir);

		// initially, without any call, the job is considered running
		assertTrue(registry.isJobRunning(jid));

		// repeated setting should not affect the status
		registry.setJobRunning(jid);
		assertTrue(registry.isJobRunning(jid));

		// set the job to finished and validate
		registry.setJobFinished(jid);
		assertFalse(registry.isJobRunning(jid));

		// another registry should pick this up
		FsNegativeRunningJobsRegistry otherRegistry = new FsNegativeRunningJobsRegistry(workDir);
		assertFalse(otherRegistry.isJobRunning(jid));
	}

	@Test
	public void testSetFinishedAndRunning() throws Exception {
		final Path workDir = new Path(HDFS_ROOT_PATH, "änother_wörk_directörü");
		final JobID jid = new JobID();

		FsNegativeRunningJobsRegistry registry = new FsNegativeRunningJobsRegistry(workDir);

		// set the job to finished and validate
		registry.setJobFinished(jid);
		assertFalse(registry.isJobRunning(jid));

		// set the job to back to running and validate
		registry.setJobRunning(jid);
		assertTrue(registry.isJobRunning(jid));

		// another registry should pick this up
		FsNegativeRunningJobsRegistry otherRegistry = new FsNegativeRunningJobsRegistry(workDir);
		assertTrue(otherRegistry.isJobRunning(jid));
	}
}
