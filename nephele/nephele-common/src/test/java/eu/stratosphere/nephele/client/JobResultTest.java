/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import org.junit.Test;

import eu.stratosphere.nephele.client.AbstractJobResult.ReturnCode;
import eu.stratosphere.nephele.event.job.AbstractEvent;
import eu.stratosphere.nephele.event.job.JobEvent;
import eu.stratosphere.nephele.jobgraph.JobStatus;
import eu.stratosphere.nephele.util.CommonTestUtils;

/**
 * This class contains test concerning all classes which are derived from {@link AbstractJobResult}.
 * 
 * @author warneke
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

		final JobCancelResult copy = (JobCancelResult) CommonTestUtils.createCopy(orig);
		assertEquals(orig.getReturnCode(), copy.getReturnCode());
		assertEquals(orig.getDescription(), copy.getDescription());
		assertEquals(orig.hashCode(), copy.hashCode());
		assertTrue(orig.equals(copy));
	}

	/**
	 * This test checks the serialization/deserialization of {@link JobProgressResult} objects.
	 */
	@Test
	public void testJobProgressResult() {

		final ArrayList<AbstractEvent> events = new ArrayList<AbstractEvent>();
		events.add(new JobEvent(123456L, JobStatus.FAILED, SAMPLE_DESCRIPTION));
		final JobProgressResult orig = new JobProgressResult(ReturnCode.ERROR, null, events);

		final JobProgressResult copy = (JobProgressResult) CommonTestUtils.createCopy(orig);
		assertEquals(orig.getReturnCode(), copy.getReturnCode());
		assertEquals(orig.getDescription(), copy.getDescription());
		assertEquals(orig.hashCode(), copy.hashCode());
		assertTrue(orig.equals(copy));
	}

	/**
	 * This test checks the serialization/deserialization of {@link JobSubmissionResult} objects.
	 */
	@Test
	public void testJobSubmissionResult() {

		final JobSubmissionResult orig = new JobSubmissionResult(ReturnCode.SUCCESS, SAMPLE_DESCRIPTION);

		final JobSubmissionResult copy = (JobSubmissionResult) CommonTestUtils.createCopy(orig);
		assertEquals(orig.getReturnCode(), copy.getReturnCode());
		assertEquals(orig.getDescription(), copy.getDescription());
		assertEquals(orig.hashCode(), copy.hashCode());
		assertTrue(orig.equals(copy));
	}
}
