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
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore.SubmittedJobGraphListener;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.state.RetrievableStreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.runtime.zookeeper.ZooKeeperTestEnvironment;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import akka.actor.ActorRef;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Tests for basic {@link SubmittedJobGraphStore} contract.
 */
public class ZooKeeperSubmittedJobGraphsStoreITCase extends TestLogger {

	private final static ZooKeeperTestEnvironment ZooKeeper = new ZooKeeperTestEnvironment(1);

	private final static RetrievableStateStorageHelper<SubmittedJobGraph> localStateStorage = new RetrievableStateStorageHelper<SubmittedJobGraph>() {
		@Override
		public RetrievableStateHandle<SubmittedJobGraph> store(SubmittedJobGraph state) throws IOException {
			ByteStreamStateHandle byteStreamStateHandle = new ByteStreamStateHandle(
					String.valueOf(UUID.randomUUID()),
					InstantiationUtil.serializeObject(state));
			return new RetrievableStreamStateHandle<>(byteStreamStateHandle);
		}
	};


	@AfterClass
	public static void tearDown() throws Exception {
		if (ZooKeeper != null) {
			ZooKeeper.shutdown();
		}
	}

	@Before
	public void cleanUp() throws Exception {
		ZooKeeper.deleteAll();
	}

	@Test
	public void testPutAndRemoveJobGraph() throws Exception {
		ZooKeeperSubmittedJobGraphStore jobGraphs = new ZooKeeperSubmittedJobGraphStore(
			ZooKeeper.createClient(),
			"/testPutAndRemoveJobGraph",
			localStateStorage,
			Executors.directExecutor());

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
		ZooKeeperSubmittedJobGraphStore jobGraphs = new ZooKeeperSubmittedJobGraphStore(
				ZooKeeper.createClient(), "/testRecoverJobGraphs", localStateStorage, Executors.directExecutor());

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

	@Test
	public void testConcurrentAddJobGraph() throws Exception {
		ZooKeeperSubmittedJobGraphStore jobGraphs = null;
		ZooKeeperSubmittedJobGraphStore otherJobGraphs = null;

		try {
			jobGraphs = new ZooKeeperSubmittedJobGraphStore(
					ZooKeeper.createClient(), "/testConcurrentAddJobGraph", localStateStorage, Executors.directExecutor());

			otherJobGraphs = new ZooKeeperSubmittedJobGraphStore(
					ZooKeeper.createClient(), "/testConcurrentAddJobGraph", localStateStorage, Executors.directExecutor());


			SubmittedJobGraph jobGraph = createSubmittedJobGraph(new JobID(), 0);
			SubmittedJobGraph otherJobGraph = createSubmittedJobGraph(new JobID(), 0);

			SubmittedJobGraphListener listener = mock(SubmittedJobGraphListener.class);

			final JobID[] actualOtherJobId = new JobID[1];
			final CountDownLatch sync = new CountDownLatch(1);

			doAnswer(new Answer<Void>() {
				@Override
				public Void answer(InvocationOnMock invocation) throws Throwable {
					actualOtherJobId[0] = (JobID) invocation.getArguments()[0];
					sync.countDown();

					return null;
				}
			}).when(listener).onAddedJobGraph(any(JobID.class));

			// Test
			jobGraphs.start(listener);
			otherJobGraphs.start(null);

			jobGraphs.putJobGraph(jobGraph);

			// Everything is cool... not much happening ;)
			verify(listener, never()).onAddedJobGraph(any(JobID.class));
			verify(listener, never()).onRemovedJobGraph(any(JobID.class));

			// This bad boy adds the other job graph
			otherJobGraphs.putJobGraph(otherJobGraph);

			// Wait for the cache to call back
			sync.await();

			verify(listener, times(1)).onAddedJobGraph(any(JobID.class));
			verify(listener, never()).onRemovedJobGraph(any(JobID.class));

			assertEquals(otherJobGraph.getJobId(), actualOtherJobId[0]);
		}
		finally {
			if (jobGraphs != null) {
				jobGraphs.stop();
			}

			if (otherJobGraphs != null) {
				otherJobGraphs.stop();
			}
		}
	}

	@Test(expected = IllegalStateException.class)
	public void testUpdateJobGraphYouDidNotGetOrAdd() throws Exception {
		ZooKeeperSubmittedJobGraphStore jobGraphs = new ZooKeeperSubmittedJobGraphStore(
				ZooKeeper.createClient(), "/testUpdateJobGraphYouDidNotGetOrAdd", localStateStorage, Executors.directExecutor());

		ZooKeeperSubmittedJobGraphStore otherJobGraphs = new ZooKeeperSubmittedJobGraphStore(
				ZooKeeper.createClient(), "/testUpdateJobGraphYouDidNotGetOrAdd", localStateStorage, Executors.directExecutor());

		jobGraphs.start(null);
		otherJobGraphs.start(null);

		SubmittedJobGraph jobGraph = createSubmittedJobGraph(new JobID(), 0);

		jobGraphs.putJobGraph(jobGraph);

		otherJobGraphs.putJobGraph(jobGraph);
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

	protected void verifyJobGraphs(SubmittedJobGraph expected, SubmittedJobGraph actual)
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
