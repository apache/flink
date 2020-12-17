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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link JobManagerRunnerResult}.
 */
public class JobManagerRunnerResultTest extends TestLogger {

	private final ArchivedExecutionGraph archivedExecutionGraph = new ArchivedExecutionGraphBuilder().build();
	private final FlinkException testException = new FlinkException("test exception");

	@Test
	public void testSuccessfulJobManagerResult() {
		final JobManagerRunnerResult jobManagerRunnerResult = JobManagerRunnerResult.forSuccess(
			archivedExecutionGraph);

		assertTrue(jobManagerRunnerResult.isSuccess());
		assertFalse(jobManagerRunnerResult.isJobNotFinished());
		assertFalse(jobManagerRunnerResult.isInitializationFailure());
	}

	@Test
	public void testJobNotFinishedJobManagerResult() {
		final JobManagerRunnerResult jobManagerRunnerResult = JobManagerRunnerResult.forJobNotFinished();

		assertTrue(jobManagerRunnerResult.isJobNotFinished());
		assertFalse(jobManagerRunnerResult.isSuccess());
		assertFalse(jobManagerRunnerResult.isInitializationFailure());
	}

	@Test
	public void testInitializationFailureJobManagerResult() {
		final JobManagerRunnerResult jobManagerRunnerResult = JobManagerRunnerResult.forInitializationFailure(testException);

		assertTrue(jobManagerRunnerResult.isInitializationFailure());
		assertFalse(jobManagerRunnerResult.isSuccess());
		assertFalse(jobManagerRunnerResult.isJobNotFinished());
	}

	@Test
	public void testGetArchivedExecutionGraphFromSuccessfulJobManagerResult() {
		final JobManagerRunnerResult jobManagerRunnerResult = JobManagerRunnerResult.forSuccess(
			archivedExecutionGraph);

		assertThat(jobManagerRunnerResult.getArchivedExecutionGraph(), is(archivedExecutionGraph));
	}

	@Test(expected = IllegalStateException.class)
	public void testGetArchivedExecutionGraphFromJobNotFinishedFails() {
		final JobManagerRunnerResult jobManagerRunnerResult = JobManagerRunnerResult.forJobNotFinished();

		jobManagerRunnerResult.getArchivedExecutionGraph();
	}

	@Test(expected = IllegalStateException.class)
	public void testGetArchivedExecutionGraphFromInitializationFailureFails() {
		final JobManagerRunnerResult jobManagerRunnerResult = JobManagerRunnerResult.forInitializationFailure(
			testException);

		jobManagerRunnerResult.getArchivedExecutionGraph();
	}

	@Test
	public void testGetInitializationFailureFromFailedJobManagerResult() {
		final JobManagerRunnerResult jobManagerRunnerResult = JobManagerRunnerResult.forInitializationFailure(
			testException);

		assertThat(jobManagerRunnerResult.getInitializationFailure(), is(testException));
	}

	@Test(expected = IllegalStateException.class)
	public void testGetInitializationFailureFromJobNotFinished() {
		final JobManagerRunnerResult jobManagerRunnerResult = JobManagerRunnerResult.forJobNotFinished();

		jobManagerRunnerResult.getInitializationFailure();
	}

	@Test(expected = IllegalStateException.class)
	public void testGetInitializationFailureFromSuccessfulJobManagerResult() {
		final JobManagerRunnerResult jobManagerRunnerResult = JobManagerRunnerResult.forSuccess(archivedExecutionGraph);

		jobManagerRunnerResult.getInitializationFailure();
	}
}
