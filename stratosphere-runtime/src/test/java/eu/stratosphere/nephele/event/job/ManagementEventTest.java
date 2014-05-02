/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.event.job;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobgraph.JobStatus;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;
import eu.stratosphere.nephele.util.ManagementTestUtils;

/**
 * This test checks the proper serialization and deserialization of job events.
 * 
 */
public class ManagementEventTest {

	/**
	 * The time stamp used during the tests.
	 */
	private static final long TIMESTAMP = 123456789L;

	/**
	 * The name of the job used during the tests.
	 */
	private static final String JOBNAME = "Test Job Name";

	/**
	 * Tests serialization/deserialization for {@link ExecutionStateChangeEvent}.
	 */
	@Test
	public void testExecutionStateChangeEvent() {

		final ExecutionStateChangeEvent orig = new ExecutionStateChangeEvent(TIMESTAMP, new ManagementVertexID(),
			ExecutionState.READY);

		final ExecutionStateChangeEvent copy = (ExecutionStateChangeEvent) ManagementTestUtils.createCopy(orig);

		assertEquals(orig.getTimestamp(), copy.getTimestamp());
		assertEquals(orig.getVertexID(), copy.getVertexID());
		assertEquals(orig.getNewExecutionState(), copy.getNewExecutionState());
		assertEquals(orig.hashCode(), copy.hashCode());
		assertTrue(orig.equals(copy));
	}

	/**
	 * Tests serialization/deserialization for {@link RecentJobEvent}.
	 */
	@Test
	public void testRecentJobEvent() {

		final RecentJobEvent orig = new RecentJobEvent(new JobID(), JOBNAME, JobStatus.SCHEDULED, true, TIMESTAMP,
			TIMESTAMP);

		final RecentJobEvent copy = (RecentJobEvent) ManagementTestUtils.createCopy(orig);

		assertEquals(orig.getJobID(), copy.getJobID());
		assertEquals(orig.getJobName(), copy.getJobName());
		assertEquals(orig.getJobStatus(), copy.getJobStatus());
		assertEquals(orig.isProfilingAvailable(), copy.isProfilingAvailable());
		assertEquals(orig.getTimestamp(), copy.getTimestamp());
		assertEquals(orig.getSubmissionTimestamp(), copy.getSubmissionTimestamp());
		assertEquals(orig.hashCode(), copy.hashCode());
		assertTrue(orig.equals(copy));
	}

	/**
	 * Tests serialization/deserialization for {@link VertexAssignmentEvent}.
	 */
	@Test
	public void testVertexAssignmentEvent() {

		final VertexAssignmentEvent orig = new VertexAssignmentEvent(TIMESTAMP, new ManagementVertexID(), "test",
			"standard");
		final VertexAssignmentEvent copy = (VertexAssignmentEvent) ManagementTestUtils.createCopy(orig);

		assertEquals(orig.getVertexID(), copy.getVertexID());
		assertEquals(orig.getTimestamp(), copy.getTimestamp());
		assertEquals(orig.getInstanceName(), copy.getInstanceName());
		assertEquals(orig.getInstanceType(), copy.getInstanceType());
		assertEquals(orig.hashCode(), copy.hashCode());
		assertTrue(orig.equals(copy));
	}
}
