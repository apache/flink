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

package org.apache.flink.runtime.messages;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobIdsWithStatusOverview;
import org.apache.flink.runtime.messages.webmonitor.JobsOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.messages.webmonitor.RequestJobDetails;
import org.apache.flink.runtime.messages.webmonitor.RequestJobsOverview;
import org.apache.flink.runtime.messages.webmonitor.RequestJobsWithIDsOverview;
import org.apache.flink.runtime.messages.webmonitor.RequestStatusOverview;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.fail;

public class WebMonitorMessagesTest {
	
	@Test
	public void testStatusMessages() {
		try {
			final Random rnd = new Random();
			
			GenericMessageTester.testMessageInstance(RequestJobsOverview.getInstance());
			GenericMessageTester.testMessageInstance(RequestJobsWithIDsOverview.getInstance());
			GenericMessageTester.testMessageInstance(RequestStatusOverview.getInstance());
			GenericMessageTester.testMessageInstance(RequestJobsOverview.getInstance());

			GenericMessageTester.testMessageInstance(GenericMessageTester.instantiateGeneric(RequestJobDetails.class, rnd));
			GenericMessageTester.testMessageInstance(GenericMessageTester.instantiateGeneric(ClusterOverview.class, rnd));
			GenericMessageTester.testMessageInstance(GenericMessageTester.instantiateGeneric(JobsOverview.class, rnd));
			
			GenericMessageTester.testMessageInstance(new JobIdsWithStatusOverview(Arrays.asList(
				new JobIdsWithStatusOverview.JobIdWithStatus(JobID.generate(), JobStatus.RUNNING),
				new JobIdsWithStatusOverview.JobIdWithStatus(JobID.generate(), JobStatus.CANCELED),
				new JobIdsWithStatusOverview.JobIdWithStatus(JobID.generate(), JobStatus.CREATED),
				new JobIdsWithStatusOverview.JobIdWithStatus(JobID.generate(), JobStatus.FAILED),
				new JobIdsWithStatusOverview.JobIdWithStatus(JobID.generate(), JobStatus.RESTARTING))));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testJobDetailsMessage() {
		try {
			final Random rnd = new Random();
			
			int[] numVerticesPerState = new int[ExecutionState.values().length];
			int numTotal = 0;

			for (int i = 0; i < numVerticesPerState.length; i++) {
				int count = rnd.nextInt(55);
				numVerticesPerState[i] = count;
				numTotal += count;
			}

			long time = rnd.nextLong();
			long endTime = rnd.nextBoolean() ? -1L : time + rnd.nextInt();
			long lastModified = endTime == -1 ? time + rnd.nextInt() : endTime;

			String name = GenericMessageTester.randomString(rnd);
			JobID jid = GenericMessageTester.randomJobId(rnd);
			JobStatus status = GenericMessageTester.randomJobStatus(rnd);
			
			JobDetails msg1 = new JobDetails(jid, name, time, endTime, endTime - time, status, lastModified, numVerticesPerState, numTotal);
			JobDetails msg2 = new JobDetails(jid, name, time, endTime, endTime - time, status, lastModified, numVerticesPerState, numTotal);
			
			GenericMessageTester.testMessageInstances(msg1, msg2);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testMultipleJobDetails() {
		try {
			final Random rnd = new Random();
			GenericMessageTester.testMessageInstance(
					new MultipleJobsDetails(randomJobDetails(rnd)));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	private static List<JobID> randomIds(Random rnd) {
		final int num = rnd.nextInt(20);
		ArrayList<JobID> ids = new ArrayList<>(num);
		
		for (int i = 0; i < num; i++) {
			ids.add(new JobID(rnd.nextLong(), rnd.nextLong()));
		}
		
		return ids;
	}
	
	private Collection<JobDetails> randomJobDetails(Random rnd) {
		final JobDetails[] details = new JobDetails[rnd.nextInt(10)];
		for (int k = 0; k < details.length; k++) {
			int[] numVerticesPerState = new int[ExecutionState.values().length];
			int numTotal = 0;

			for (int i = 0; i < numVerticesPerState.length; i++) {
				int count = rnd.nextInt(55);
				numVerticesPerState[i] = count;
				numTotal += count;
			}

			long time = rnd.nextLong();
			long endTime = rnd.nextBoolean() ? -1L : time + rnd.nextInt();
			long lastModified = endTime == -1 ? time + rnd.nextInt() : endTime;

			String name = new GenericMessageTester.StringInstantiator().instantiate(rnd);
			JobID jid = new JobID();
			JobStatus status = JobStatus.values()[rnd.nextInt(JobStatus.values().length)];

			details[k] = new JobDetails(jid, name, time, endTime, endTime - time, status, lastModified, numVerticesPerState, numTotal);
		}
		return Arrays.asList(details);
	}
}
