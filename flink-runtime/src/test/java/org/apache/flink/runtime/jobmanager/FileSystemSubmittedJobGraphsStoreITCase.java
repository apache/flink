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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.akka.ListeningBehaviour;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore.SubmittedJobGraphListener;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Tests for basic {@link FileSystemSubmittedJobGraphsStoreITCase}.
 */
public class FileSystemSubmittedJobGraphsStoreITCase extends TestLogger {

	private String jobGraphPath;

	@Before
	public void setup() {
		jobGraphPath =
			System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + UUID.randomUUID().toString();
	}

	@After
	public void tearDown() throws Exception {
		FileUtils.deleteDirectory(new File(jobGraphPath));
	}

	@Test
	public void testPutAndRemoveJobGraph() throws Exception {
		FileSystemSubmittedJobGraphStore jobGraphs = new FileSystemSubmittedJobGraphStore(jobGraphPath);

		try {
			SubmittedJobGraphListener listener = mock(SubmittedJobGraphListener.class);

			jobGraphs.start(listener);

			SubmittedJobGraph jobGraph = createSubmittedJobGraph(new JobID(), 0);

			// Empty state
			assertEquals(0, jobGraphs.getJobIds().size());

			// Add initial
			jobGraphs.putJobGraph(jobGraph);

			// Verify initial job graph
			Collection<JobID> jobIds = jobGraphs.getJobIds();
			assertEquals(1, jobIds.size());

			JobID jobId = jobIds.iterator().next();

			verifyJobGraphs(jobGraph, jobGraphs.recoverJobGraph(jobId));

			// Update (same ID)
			jobGraph = createSubmittedJobGraph(jobGraph.getJobId(), 1);
			jobGraphs.putJobGraph(jobGraph);

			// Verify updated
			jobIds = jobGraphs.getJobIds();
			assertEquals(1, jobIds.size());

			jobId = jobIds.iterator().next();

			verifyJobGraphs(jobGraph, jobGraphs.recoverJobGraph(jobId));

			// Remove
			jobGraphs.removeJobGraph(jobGraph.getJobId());

			// Empty state
			assertEquals(0, jobGraphs.getJobIds().size());

			// Nothing should have been notified
			verify(listener, atMost(1)).onAddedJobGraph(any(JobID.class));
			verify(listener, never()).onRemovedJobGraph(any(JobID.class));

			// Don't fail if called again
			jobGraphs.removeJobGraph(jobGraph.getJobId());
		}
		finally {
			jobGraphs.stop();
		}
	}

	@Test
	public void testRecoverJobGraphs() throws Exception {
		FileSystemSubmittedJobGraphStore jobGraphs = new FileSystemSubmittedJobGraphStore(jobGraphPath);

		try {
			SubmittedJobGraphListener listener = mock(SubmittedJobGraphListener.class);

			jobGraphs.start(listener);

			HashMap<JobID, SubmittedJobGraph> expected = new HashMap<>();
			JobID[] jobIds = new JobID[] { new JobID(), new JobID(), new JobID() };

			expected.put(jobIds[0], createSubmittedJobGraph(jobIds[0], 0));
			expected.put(jobIds[1], createSubmittedJobGraph(jobIds[1], 1));
			expected.put(jobIds[2], createSubmittedJobGraph(jobIds[2], 2));

			// Add all
			for (SubmittedJobGraph jobGraph : expected.values()) {
				jobGraphs.putJobGraph(jobGraph);
			}

			Collection<JobID> actual = jobGraphs.getJobIds();

			assertEquals(expected.size(), actual.size());

			for (JobID jobId : actual) {
				SubmittedJobGraph jobGraph = jobGraphs.recoverJobGraph(jobId);
				assertTrue(expected.containsKey(jobGraph.getJobId()));

				verifyJobGraphs(expected.get(jobGraph.getJobId()), jobGraph);

				jobGraphs.removeJobGraph(jobGraph.getJobId());
			}

			// Empty state
			assertEquals(0, jobGraphs.getJobIds().size());

			// Nothing should have been notified
			verify(listener, atMost(expected.size())).onAddedJobGraph(any(JobID.class));
			verify(listener, never()).onRemovedJobGraph(any(JobID.class));
		}
		finally {
			jobGraphs.stop();
		}
	}

	// ---------------------------------------------------------------------------------------------

	private SubmittedJobGraph createSubmittedJobGraph(JobID jobId, long start) {
		final JobGraph jobGraph = new JobGraph(jobId, "Test JobGraph");

		final JobVertex jobVertex = new JobVertex("Test JobVertex");
		jobVertex.setParallelism(1);

		jobGraph.addVertex(jobVertex);

		final JobInfo jobInfo = new JobInfo(
				ActorRef.noSender(), ListeningBehaviour.DETACHED, start, Integer.MAX_VALUE);

		return new SubmittedJobGraph(jobGraph, jobInfo);
	}

	private void verifyJobGraphs(SubmittedJobGraph expected, SubmittedJobGraph actual)
			throws Exception {

		JobGraph expectedJobGraph = expected.getJobGraph();
		JobGraph actualJobGraph = actual.getJobGraph();

		assertEquals(expectedJobGraph.getName(), actualJobGraph.getName());
		assertEquals(expectedJobGraph.getJobID(), actualJobGraph.getJobID());

		JobInfo expectedJobInfo = expected.getJobInfo();
		JobInfo actualJobInfo = actual.getJobInfo();

		assertEquals(expectedJobInfo, actualJobInfo);
	}
}
