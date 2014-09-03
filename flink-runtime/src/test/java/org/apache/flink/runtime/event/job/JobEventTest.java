/**
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


package org.apache.flink.runtime.event.job;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.flink.runtime.event.job.JobEvent;
import org.apache.flink.runtime.event.job.VertexEvent;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.junit.Test;

/**
 * This class contains tests concerning the serialization/deserialization of job events which have been derived from
 * {@link org.apache.flink.runtime.event.job.AbstractEvent}.
 * 
 */
public class JobEventTest {

	/**
	 * This test checks the correct serialization/deserialization of a {@link JobEvent}.
	 */
	@Test
	public void testJobEvent() {

		try {

			final JobEvent orig = new JobEvent(1234567L, JobStatus.FINISHED, null);
			final JobEvent copy = (JobEvent) CommonTestUtils.createCopy(orig);

			assertEquals(orig.getTimestamp(), copy.getTimestamp());
			assertEquals(orig.getCurrentJobStatus(), copy.getCurrentJobStatus());
			assertEquals(orig.hashCode(), copy.hashCode());
			assertTrue(orig.equals(copy));

		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}
	}

	/**
	 * This test checks the correct serialization/deserialization of a {@link VertexEvent}.
	 */
	@Test
	public void testVertexEvent() {

		try {

			final VertexEvent orig = new VertexEvent(23423423L, new JobVertexID(), "Test Vertex", 2, 0,
				ExecutionState.READY, "Test Description");
			final VertexEvent copy = (VertexEvent) CommonTestUtils.createCopy(orig);

			assertEquals(orig.getTimestamp(), copy.getTimestamp());
			assertEquals(orig.getJobVertexID(), copy.getJobVertexID());
			assertEquals(orig.getJobVertexName(), copy.getJobVertexName());
			assertEquals(orig.getTotalNumberOfSubtasks(), copy.getTotalNumberOfSubtasks());
			assertEquals(orig.getIndexOfSubtask(), copy.getIndexOfSubtask());
			assertEquals(orig.getCurrentExecutionState(), copy.getCurrentExecutionState());
			assertEquals(orig.getDescription(), copy.getDescription());
			assertEquals(orig.hashCode(), copy.hashCode());
			assertTrue(orig.equals(copy));

		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}
	}
}
