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

package org.apache.flink.runtime.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.flink.runtime.client.AbstractJobResult.ReturnCode;
import org.apache.flink.runtime.event.job.AbstractEvent;
import org.apache.flink.runtime.event.job.JobEvent;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.SerializableArrayList;
import org.junit.Test;

/**
 * This class contains test concerning all classes which are derived from {@link AbstractJobResult}.
 */
public class JobResultTest {

	/**
	 * A sample description which is used during the tests.
	 */
	private static final String SAMPLE_DESCRIPTION = "The sample description";

	/**
	 * This test checks the serialization/deserialization of {@link JobCancelResult} objects.
	 */
	@Test
	public void testJobCancelResult() {

		final JobCancelResult orig = new JobCancelResult(ReturnCode.SUCCESS, SAMPLE_DESCRIPTION);

		try {

			final JobCancelResult copy = (JobCancelResult) CommonTestUtils.createCopyWritable(orig);
			assertEquals(orig.getReturnCode(), copy.getReturnCode());
			assertEquals(orig.getDescription(), copy.getDescription());
			assertEquals(orig.hashCode(), copy.hashCode());
			assertTrue(orig.equals(copy));

		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}
	}

	/**
	 * This test checks the serialization/deserialization of {@link JobProgressResult} objects.
	 */
	@Test
	public void testJobProgressResult() {

		final SerializableArrayList<AbstractEvent> events = new SerializableArrayList<AbstractEvent>();
		events.add(new JobEvent(123456L, JobStatus.FAILED, SAMPLE_DESCRIPTION));
		final JobProgressResult orig = new JobProgressResult(ReturnCode.ERROR, null, events);

		try {

			final JobProgressResult copy = (JobProgressResult) CommonTestUtils.createCopyWritable(orig);
			assertEquals(orig.getReturnCode(), copy.getReturnCode());
			assertEquals(orig.getDescription(), copy.getDescription());
			assertEquals(orig.hashCode(), copy.hashCode());
			assertTrue(orig.equals(copy));

		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}
	}

	/**
	 * This test checks the serialization/deserialization of {@link JobSubmissionResult} objects.
	 */
	@Test
	public void testJobSubmissionResult() {

		final JobSubmissionResult orig = new JobSubmissionResult(ReturnCode.SUCCESS, SAMPLE_DESCRIPTION);

		try {

			final JobSubmissionResult copy = (JobSubmissionResult) CommonTestUtils.createCopyWritable(orig);
			assertEquals(orig.getReturnCode(), copy.getReturnCode());
			assertEquals(orig.getDescription(), copy.getDescription());
			assertEquals(orig.hashCode(), copy.hashCode());
			assertTrue(orig.equals(copy));

		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}
	}
}
