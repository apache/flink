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
import org.apache.flink.runtime.highavailability.RunningJobsRegistry.JobSchedulingStatus;
import org.apache.flink.util.OperatingSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link FsNegativeRunningJobsRegistry} on HDFS.
 */
public class FsNegativeRunningJobsRegistryTest {

	@ClassRule
	public static final TemporaryFolder TEMP_DIR = new TemporaryFolder();

	private static MiniDFSCluster hdfsCluster;

	private static Path hdfsRootPath;

	// ------------------------------------------------------------------------
	//  startup / shutdown
	// ------------------------------------------------------------------------

	@BeforeClass
	public static void createHDFS() throws Exception {
		Assume.assumeTrue("HDFS cluster cannot be start on Windows without extensions.", !OperatingSystem.isWindows());

		final File tempDir = TEMP_DIR.newFolder();

		Configuration hdConf = new Configuration();
		hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tempDir.getAbsolutePath());

		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
		hdfsCluster = builder.build();

		hdfsRootPath = new Path("hdfs://" + hdfsCluster.getURI().getHost() + ":"
				+ hdfsCluster.getNameNodePort() + "/");
	}

	@AfterClass
	public static void destroyHDFS() {
		if (hdfsCluster != null) {
			hdfsCluster.shutdown();
		}
		hdfsCluster = null;
		hdfsRootPath = null;
	}

	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------

	@Test
	public void testCreateAndSetFinished() throws Exception {
		final Path workDir = new Path(hdfsRootPath, "test-work-dir");
		final JobID jid = new JobID();

		FsNegativeRunningJobsRegistry registry = new FsNegativeRunningJobsRegistry(workDir);

		// another registry should pick this up
		FsNegativeRunningJobsRegistry otherRegistry = new FsNegativeRunningJobsRegistry(workDir);

		// initially, without any call, the job is pending
		assertEquals(JobSchedulingStatus.PENDING, registry.getJobSchedulingStatus(jid));
		assertEquals(JobSchedulingStatus.PENDING, otherRegistry.getJobSchedulingStatus(jid));

		// after set running, the job is running
		registry.setJobRunning(jid);
		assertEquals(JobSchedulingStatus.RUNNING, registry.getJobSchedulingStatus(jid));
		assertEquals(JobSchedulingStatus.RUNNING, otherRegistry.getJobSchedulingStatus(jid));

		// set the job to finished and validate
		registry.setJobFinished(jid);
		assertEquals(JobSchedulingStatus.DONE, registry.getJobSchedulingStatus(jid));
		assertEquals(JobSchedulingStatus.DONE, otherRegistry.getJobSchedulingStatus(jid));
	}

	@Test
	public void testSetFinishedAndRunning() throws Exception {
		final Path workDir = new Path(hdfsRootPath, "änother_wörk_directörü");
		final JobID jid = new JobID();

		FsNegativeRunningJobsRegistry registry = new FsNegativeRunningJobsRegistry(workDir);

		// set the job to finished and validate
		registry.setJobFinished(jid);
		assertEquals(JobSchedulingStatus.DONE, registry.getJobSchedulingStatus(jid));

		// set the job to running does not overwrite the finished status
		registry.setJobRunning(jid);
		assertEquals(JobSchedulingStatus.DONE, registry.getJobSchedulingStatus(jid));

		// another registry should pick this up
		FsNegativeRunningJobsRegistry otherRegistry = new FsNegativeRunningJobsRegistry(workDir);
		assertEquals(JobSchedulingStatus.DONE, otherRegistry.getJobSchedulingStatus(jid));

		// clear the running and finished marker, it will be pending
		otherRegistry.clearJob(jid);
		assertEquals(JobSchedulingStatus.PENDING, registry.getJobSchedulingStatus(jid));
		assertEquals(JobSchedulingStatus.PENDING, otherRegistry.getJobSchedulingStatus(jid));
	}
}
