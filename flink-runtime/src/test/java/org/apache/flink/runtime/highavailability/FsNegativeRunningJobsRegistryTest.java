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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry.JobSchedulingStatus;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FsNegativeRunningJobsRegistryTest extends TestLogger {

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testCreateAndSetFinished() throws Exception {
		final File folder = tempFolder.newFolder();
		final String uri = folder.toURI().toString();

		final JobID jid = new JobID();

		FsNegativeRunningJobsRegistry registry = new FsNegativeRunningJobsRegistry(new Path(uri));

		// initially, without any call, the job is pending
		assertFalse(registry.isJobRunning(jid));
		assertEquals(registry.getJobSchedulingStatus(jid), JobSchedulingStatus.PENDING);

		// after set running, the job is running
		registry.setJobRunning(jid);
		assertTrue(registry.isJobRunning(jid));
		assertEquals(registry.getJobSchedulingStatus(jid), JobSchedulingStatus.RUNNING);

		// set the job to finished and validate
		registry.setJobFinished(jid);
		assertEquals(registry.getJobSchedulingStatus(jid), JobSchedulingStatus.DONE);

		// another registry should pick this up
		FsNegativeRunningJobsRegistry otherRegistry = new FsNegativeRunningJobsRegistry(new Path(uri));
		assertTrue(otherRegistry.isJobRunning(jid));
		assertEquals(otherRegistry.getJobSchedulingStatus(jid), JobSchedulingStatus.DONE);
	}

	@Test
	public void testSetFinishedAndRunning() throws Exception {
		final File folder = tempFolder.newFolder();
		final String uri = folder.toURI().toString();

		final JobID jid = new JobID();

		FsNegativeRunningJobsRegistry registry = new FsNegativeRunningJobsRegistry(new Path(uri));

		// set the job to finished and validate
		registry.setJobFinished(jid);
		assertFalse(registry.isJobRunning(jid));
		assertEquals(registry.getJobSchedulingStatus(jid), JobSchedulingStatus.DONE);

		// set the job to running does not overwrite the finished status
		registry.setJobRunning(jid);
		assertTrue(registry.isJobRunning(jid));
		assertEquals(registry.getJobSchedulingStatus(jid), JobSchedulingStatus.DONE);

		// another registry should pick this up
		FsNegativeRunningJobsRegistry otherRegistry = new FsNegativeRunningJobsRegistry(new Path(uri));
		assertTrue(otherRegistry.isJobRunning(jid));
		assertEquals(registry.getJobSchedulingStatus(jid), JobSchedulingStatus.DONE);

		// clear the running and finished marker, it will be pending
		otherRegistry.clearJob(jid);
		assertFalse(otherRegistry.isJobRunning(jid));
		assertEquals(registry.getJobSchedulingStatus(jid), JobSchedulingStatus.PENDING);
	}
}
