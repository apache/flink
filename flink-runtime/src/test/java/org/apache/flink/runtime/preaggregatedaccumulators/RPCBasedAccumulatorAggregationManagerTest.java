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

package org.apache.flink.runtime.preaggregatedaccumulators;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.taskexecutor.JobManagerConnection;
import org.apache.flink.runtime.taskexecutor.JobManagerTable;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.preaggregatedaccumulators.RPCBasedAccumulatorAggregationManager.AggregatedAccumulator;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests the actions of RPCBasedAccumulatorAggregationManager.
 */
public class RPCBasedAccumulatorAggregationManagerTest {

	@Test
	public void testRegistration() {
		final JobID jobId = new JobID();
		final JobVertexID jobVertexId = new JobVertexID();
		final int subtaskIndex = 0;
		final String name = "test";

		RPCBasedAccumulatorAggregationManager manager =
			new RPCBasedAccumulatorAggregationManager(createJobManagerTable(Collections.singletonList(jobId)));
		manager.registerPreAggregatedAccumulator(jobId, jobVertexId, subtaskIndex, name);

		Map<JobID, Map<String, AggregatedAccumulator>> perJobAccumulators = manager.getPerJobAggregatedAccumulators();

		assertTrue("There should be only one job exactly.",
			perJobAccumulators.size() == 1 && perJobAccumulators.containsKey(jobId));

		Map<String, AggregatedAccumulator> currentJobAccumulators = perJobAccumulators.get(jobId);
		assertTrue("There should be only one accumulator exactly.",
			currentJobAccumulators.size() == 1 && currentJobAccumulators.containsKey(name));

		AggregatedAccumulator aggregatedAccumulator = currentJobAccumulators.get(name);
		assertTrue("There should be only one registered task exactly.",
			aggregatedAccumulator.getRegisteredTasks().size() == 1 && aggregatedAccumulator.getRegisteredTasks().contains(subtaskIndex));
		assertEquals(jobVertexId, aggregatedAccumulator.getJobVertexId());

		final JobVertexID secondJobVertexId = new JobVertexID();
		final ExecutionAttemptID secondAttemptId = new ExecutionAttemptID();

		try {
			manager.registerPreAggregatedAccumulator(jobId, secondJobVertexId, subtaskIndex, name);
			fail("Should throw exception when tasks from another job vertex try to register on the same accumulator.");
		} catch (IllegalArgumentException e) {
			// Expected exception.
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCommission() {
		final JobID jobId = new JobID();
		final JobVertexID jobVertexId = new JobVertexID();

		final int firstSubtaskIndex = 0;
		final int secondSubtaskIndex = 1;

		final String name = "test";

		JobManagerTable jobManagerTable = createJobManagerTable(Collections.singletonList(jobId));
		RPCBasedAccumulatorAggregationManager manager = new RPCBasedAccumulatorAggregationManager(jobManagerTable);
		JobMasterGateway gateway = jobManagerTable.get(jobId).getJobManagerGateway();

		List<CommitAccumulator> commitAccumulators = new ArrayList<>();
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				commitAccumulators.addAll((List<? extends CommitAccumulator>) invocationOnMock.getArguments()[0]);
				return null;
			}
		}).when(gateway).commitPreAggregatedAccumulator(any(List.class));

		manager.registerPreAggregatedAccumulator(jobId, jobVertexId, firstSubtaskIndex, name);
		manager.registerPreAggregatedAccumulator(jobId, jobVertexId, secondSubtaskIndex, name);

		// Commit for the second task.
		manager.commitPreAggregatedAccumulator(jobId, jobVertexId, secondSubtaskIndex, name, new IntCounter(2));

		Map<JobID, Map<String, AggregatedAccumulator>> perJobAccumulators = manager.getPerJobAggregatedAccumulators();
		assertTrue("There should be only current job exactly.",
			perJobAccumulators.size() == 1 && perJobAccumulators.containsKey(jobId));
		Map<String, AggregatedAccumulator> currentJobAccumulators = perJobAccumulators.get(jobId);
		assertTrue("There should be only current accumulator exactly.",
			currentJobAccumulators.size() == 1 && currentJobAccumulators.containsKey(name));

		AggregatedAccumulator aggregatedAccumulator = currentJobAccumulators.get(name);
		assertTrue("There should be two registered task exactly.",
			aggregatedAccumulator.getRegisteredTasks().size() == 2
				&& aggregatedAccumulator.getRegisteredTasks().contains(firstSubtaskIndex)
				&& aggregatedAccumulator.getRegisteredTasks().contains(secondSubtaskIndex));
		assertTrue("There should be one committed task exactly.",
			aggregatedAccumulator.getCommittedTasks().size() == 1
				&& aggregatedAccumulator.getCommittedTasks().contains(secondSubtaskIndex));
		assertEquals(2, aggregatedAccumulator.getAggregatedValue().getLocalValue());

		// Check that JobMaster receives no committed accumulators.
		assertEquals(0, commitAccumulators.size());

		// Commit for an unregistered task.
		try {
			final int nonExistSubtaskIndex = 100;
			manager.commitPreAggregatedAccumulator(jobId, jobVertexId, nonExistSubtaskIndex, name, new IntCounter(3));
			fail("Commit for an unregistered task should throw exception");
		} catch (IllegalStateException e) {
			// Expected exception.
		}

		// Commit for the first task.
		manager.commitPreAggregatedAccumulator(jobId, jobVertexId, firstSubtaskIndex, name, new IntCounter(1));

		assertTrue("The current job should be removed.", perJobAccumulators.isEmpty());
		assertEquals(1, commitAccumulators.size());
		assertEquals(jobVertexId, commitAccumulators.get(0).getJobVertexId());
		assertEquals(name, commitAccumulators.get(0).getName());
		assertEquals(3, ((IntCounter) commitAccumulators.get(0).getAccumulator()).getLocalValue().intValue());
		assertEquals(new HashSet<>(Arrays.asList(firstSubtaskIndex, secondSubtaskIndex)), commitAccumulators.get(0).getCommittedTasks());
	}

	/**
	 * Verifies that when clearing registration, for an incomplete aggregated accumulator,
	 * the task will not be removed from the registered and committed list.
	 */
	@Test
	public void testClearRegistrationOnCommittedAccumulator() {
		final JobID jobId = new JobID();
		final JobVertexID jobVertexId = new JobVertexID();
		final int subtaskIndex = 0;
		final String name = "test";

		JobManagerTable jobManagerTable = createJobManagerTable(Collections.singletonList(jobId));
		RPCBasedAccumulatorAggregationManager manager = new RPCBasedAccumulatorAggregationManager(jobManagerTable);

		manager.registerPreAggregatedAccumulator(jobId, jobVertexId, subtaskIndex, name);

		// Other tasks who have also registered.
		manager.registerPreAggregatedAccumulator(jobId, jobVertexId, 1, name);
		manager.registerPreAggregatedAccumulator(jobId, jobVertexId, 2, name);

		// Commit the accumulator.
		manager.commitPreAggregatedAccumulator(jobId, jobVertexId, subtaskIndex, name, new IntCounter(1));

		manager.clearRegistrationForTask(jobId, jobVertexId, subtaskIndex);

		AggregatedAccumulator aggregatedAccumulator = manager.getPerJobAggregatedAccumulators().get(jobId).get(name);

		assertEquals(3, aggregatedAccumulator.getRegisteredTasks().size());
		assertTrue("The committed task should not be removed from the registered list on clear registration.",
			aggregatedAccumulator.getRegisteredTasks().contains(subtaskIndex));

		assertEquals(1, aggregatedAccumulator.getCommittedTasks().size());
		assertTrue("The committed task should not be removed from the committed list on clear registration.",
			aggregatedAccumulator.getCommittedTasks().contains(subtaskIndex));
	}

	/**
	 * Verifies that when clearing registration, the task should be remove from the accumulator's registered list
	 * if it has not committed yet. Furthermore, if it is the only registered task, the whole accumulator should be
	 * removed.
	 */
	@Test
	public void testUncommittedAccumulatorsWhenClearRegistration() {
		final JobID jobId = new JobID();
		final JobVertexID jobVertexId = new JobVertexID();
		final int subtaskIndex = 0;

		JobManagerTable jobManagerTable = createJobManagerTable(Collections.singletonList(jobId));
		RPCBasedAccumulatorAggregationManager manager = new RPCBasedAccumulatorAggregationManager(jobManagerTable);

		// The first accumulator has more than one tasks registered, when clearing registration
		// it should only remove the target task.
		final String unRemovedName = "unremoved";
		manager.registerPreAggregatedAccumulator(jobId, jobVertexId, subtaskIndex, unRemovedName);
		manager.registerPreAggregatedAccumulator(jobId, jobVertexId, 1, unRemovedName);

		// The second accumulator has only one task registered, when clearing registration
		// the whole accumulator should be removed.
		final String removedName = "removed";
		manager.registerPreAggregatedAccumulator(jobId, jobVertexId, subtaskIndex, removedName);

		Map<String, AggregatedAccumulator> currentJobAccumulators = manager.getPerJobAggregatedAccumulators().get(jobId);
		assertTrue("Both of the two accumulators should exist.",
			currentJobAccumulators.size() == 2 &&
				currentJobAccumulators.containsKey(unRemovedName) &&
				currentJobAccumulators.containsKey(removedName));

		manager.clearRegistrationForTask(jobId, jobVertexId, subtaskIndex);
		assertTrue("Only the un-removed accumulator should exist.",
			currentJobAccumulators.size() == 1 && currentJobAccumulators.containsKey(unRemovedName));

		AggregatedAccumulator unRemovedAccumulator = currentJobAccumulators.get(unRemovedName);
		assertFalse("The target target should be removed from the registered list since it has not committed. ",
			unRemovedAccumulator.getRegisteredTasks().contains(subtaskIndex));
	}

	/**
	 * Verifies that when clearing registration, if there also some other tasks who have registered the target accumulator
	 * and they all have already committed, then the accumulator should be committed to JobMaster after clearing.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testCommittedAccumulatorsWhenClearRegistration() {
		final JobID jobId = new JobID();
		final JobVertexID jobVertexId = new JobVertexID();
		final int subtaskIndex = 0;
		final String name = "test";

		JobManagerTable jobManagerTable = createJobManagerTable(Collections.singletonList(jobId));
		JobMasterGateway gateway = jobManagerTable.get(jobId).getJobManagerGateway();

		Map<String, Accumulator> globalCommittedAccumulators = new HashMap<>();
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				List<CommitAccumulator> commitAccumulators = (List<CommitAccumulator>) invocationOnMock.getArguments()[0];

				for (CommitAccumulator commitAccumulator : commitAccumulators) {
					assertFalse("The accumulator with the same name has been committed",
						globalCommittedAccumulators.containsKey(commitAccumulator.getName()));
					globalCommittedAccumulators.put(commitAccumulator.getName(), commitAccumulator.getAccumulator());
				}

				return null;
			}
		}).when(gateway).commitPreAggregatedAccumulator(any(List.class));

		RPCBasedAccumulatorAggregationManager manager = new RPCBasedAccumulatorAggregationManager(jobManagerTable);

		manager.registerPreAggregatedAccumulator(jobId, jobVertexId, subtaskIndex, name);

		// other tasks who have registered and committed.
		final int otherTaskIndex = 1;
		manager.registerPreAggregatedAccumulator(jobId, jobVertexId, otherTaskIndex, name);
		manager.commitPreAggregatedAccumulator(jobId, jobVertexId, otherTaskIndex, name, new IntCounter(3));

		manager.clearRegistrationForTask(jobId, jobVertexId, subtaskIndex);

		assertEquals("The target accumulator should be removed after committing and further cause that the job's map get removed.",
			0, manager.getPerJobAggregatedAccumulators().size());

		assertTrue("The target accumulator should be committed.",
			globalCommittedAccumulators.size() == 1 && globalCommittedAccumulators.containsKey(name));
		assertEquals(3, globalCommittedAccumulators.get(name).getLocalValue());
	}

	/**
	 * Verifies that when querying an incomplete accumulator, the local queries should be kept
	 * in the unfulfilled list till the query to the job master is fulfilled.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testQueryIncompleteAccumulators() {
		final JobID jobId = new JobID();
		final String name = "test";

		JobManagerTable jobManagerTable = createJobManagerTable(Collections.singletonList(jobId));
		JobMasterGateway gateway = jobManagerTable.get(jobId).getJobManagerGateway();

		CompletableFuture jobMasterQueryFuture = new CompletableFuture();
		when(gateway.queryPreAggregatedAccumulator(any(String.class))).thenReturn(jobMasterQueryFuture);

		RPCBasedAccumulatorAggregationManager manager = new RPCBasedAccumulatorAggregationManager(jobManagerTable);

		final int numQueries = 4;
		Accumulator[] queryResults = new Accumulator[numQueries];

		for (int i = 0; i < numQueries; ++i) {
			CompletableFuture<Accumulator<Integer, Integer>> localQueryFuture = manager.queryPreAggregatedAccumulator(jobId, name);
			assertFalse("The query should not finish since the job master has not completed.", localQueryFuture.isDone());

			final int myIndex = i;
			localQueryFuture.whenComplete(((accumulator, throwable) -> {
				queryResults[myIndex] = accumulator;
			}));
		}

		// There should be only one query sent to JobMaster.
		verify(gateway, times(1)).queryPreAggregatedAccumulator(eq(name));

		// All the local queries should be kept in the unfulfilled list.
		assertEquals(numQueries, manager.getPerJobUnfulfilledUserQueryFutures().get(jobId).get(name).size());

		jobMasterQueryFuture.complete(new IntCounter(5));

		for (int i = 0;  i < queryResults.length; ++i) {
			assertNotNull("The query should finish since the job master has completed.", queryResults[i]);
			assertEquals(5, queryResults[i].getLocalValue());
		}
	}

	/**
	 * Verifies that when querying an complete accumulator, the first query will copy the accumulator from the
	 * job master to the local cache, and the following queries will be fulfilled by the local cache directly.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testQueryCompleteAccumulators() {
		final JobID jobId = new JobID();
		final String name = "test";

		JobManagerTable jobManagerTable = createJobManagerTable(Collections.singletonList(jobId));
		JobMasterGateway gateway = jobManagerTable.get(jobId).getJobManagerGateway();

		CompletableFuture jobMasterQueryFuture = new CompletableFuture();
		jobMasterQueryFuture.complete(new IntCounter(5));
		when(gateway.queryPreAggregatedAccumulator(any(String.class))).thenReturn(jobMasterQueryFuture);

		RPCBasedAccumulatorAggregationManager manager = new RPCBasedAccumulatorAggregationManager(jobManagerTable);

		// The first query copies the accumulator from the JobMaster and store it in the cache.
		CompletableFuture<Accumulator<Integer, Integer>> localQueryFuture = manager.queryPreAggregatedAccumulator(jobId, name);
		assertTrue("The query should finish directly since the accumulator has completed aggregation.",
			localQueryFuture.isDone());

		// There should be only one query sent to JobMaster.
		verify(gateway, times(1)).queryPreAggregatedAccumulator(eq(name));
		assertTrue("The accumulator should be added to cache after the first queries.",
			manager.getPerJobCachedQueryResults().containsKey(jobId) &&
			manager.getPerJobCachedQueryResults().get(jobId).containsKey(name));
		Object entry = manager.getPerJobCachedQueryResults().get(jobId).get(name);
		assertTrue("The cached result should be equals to the target accumulator.",
			entry != null && ((Accumulator<Integer, Integer>) entry).getLocalValue() == 5);

		reset(gateway);

		// The following queries should be fulfilled directly by the cache.
		final int numQueries = 4;
		Accumulator[] queryResults = new Accumulator[numQueries];

		for (int i = 0; i < numQueries; ++i) {
			localQueryFuture = manager.queryPreAggregatedAccumulator(jobId, name);
			assertTrue("The query should finish directly since the accumulator has completed aggregation.",
				localQueryFuture.isDone());

			final int myIndex = i;
			localQueryFuture.whenComplete(((accumulator, throwable) -> {
				queryResults[myIndex] = accumulator;
			}));
		}

		for (int i = 0;  i < queryResults.length; ++i) {
			assertNotNull("The query should be finished since the accumulator has complete aggregation.", queryResults[i]);
			assertEquals(5, queryResults[i].getLocalValue());
		}

		// There should be no more queries send to the job master.
		verify(gateway, times(0)).queryPreAggregatedAccumulator(any(String.class));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testQueryToJobMasterFail() {
		final JobID jobId = new JobID();
		final String name = "test";

		JobManagerTable jobManagerTable = createJobManagerTable(Collections.singletonList(jobId));
		JobMasterGateway gateway = jobManagerTable.get(jobId).getJobManagerGateway();

		CompletableFuture jobMasterQueryFuture = new CompletableFuture();
		jobMasterQueryFuture.completeExceptionally(new RuntimeException("fail to query"));
		when(gateway.queryPreAggregatedAccumulator(any(String.class))).thenReturn(jobMasterQueryFuture);

		RPCBasedAccumulatorAggregationManager manager = new RPCBasedAccumulatorAggregationManager(jobManagerTable);

		// The first query records the exception it in the cache.
		CompletableFuture<Accumulator<Integer, Integer>> localQueryFuture = manager.queryPreAggregatedAccumulator(jobId, name);
		assertTrue("The query should finish directly since the accumulator has complete aggregation.",
			localQueryFuture.isDone());

		// There should be only one query sent to JobMaster.
		verify(gateway, times(1)).queryPreAggregatedAccumulator(eq(name));
		assertTrue("The accumulator should be added to cache after the first queries.",
			manager.getPerJobCachedQueryResults().containsKey(jobId) &&
			manager.getPerJobCachedQueryResults().get(jobId).containsKey(name));
		Object entry = manager.getPerJobCachedQueryResults().get(jobId).get(name);
		assertTrue("The cached result should be a RuntimeException.",
			entry != null && entry instanceof RuntimeException);

		reset(gateway);

		// The following queries should be fulfilled directly by the cache.
		final int numQueries = 4;
		Throwable[] exceptionsCaught = new Throwable[numQueries];

		for (int i = 0; i < numQueries; ++i) {
			localQueryFuture = manager.queryPreAggregatedAccumulator(jobId, name);
			assertTrue("The query should finish directly since the accumulator has complete aggregation.",
				localQueryFuture.isDone());

			final int myIndex = i;
			localQueryFuture.whenComplete(((accumulator, throwable) -> {
				if (accumulator != null) {
					fail("The query should fail");
				} else {
					exceptionsCaught[myIndex] = throwable;
				}
			}));
		}

		for (int i = 0;  i < exceptionsCaught.length; ++i) {
			assertNotNull("The query should be finished with exception.", exceptionsCaught[i]);
			assertTrue("RuntimeException should be caught.", exceptionsCaught[i] instanceof RuntimeException);
		}

		// There should be no more queries send to the job master.
		verify(gateway, times(0)).queryPreAggregatedAccumulator(any(String.class));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testClearForJob() {
		final JobID jobId = new JobID();
		final JobVertexID jobVertexId = new JobVertexID();
		final int subtaskIndex = 0;

		JobManagerTable jobManagerTable = createJobManagerTable(Collections.singletonList(jobId));
		JobMasterGateway gateway = jobManagerTable.get(jobId).getJobManagerGateway();

		CompletableFuture unfulfilledQuery = new CompletableFuture();
		when(gateway.queryPreAggregatedAccumulator(eq("unfulfilled"))).thenReturn(unfulfilledQuery);

		CompletableFuture fulfilledQuery = new CompletableFuture();
		fulfilledQuery.complete(new IntCounter(5));
		when(gateway.queryPreAggregatedAccumulator(eq("fulfilled"))).thenReturn(fulfilledQuery);

		RPCBasedAccumulatorAggregationManager manager = new RPCBasedAccumulatorAggregationManager(jobManagerTable);

		manager.registerPreAggregatedAccumulator(jobId, jobVertexId, subtaskIndex, "test");
		manager.queryPreAggregatedAccumulator(jobId, "unfulfilled");
		manager.queryPreAggregatedAccumulator(jobId, "fulfilled");

		assertEquals(1, manager.getPerJobAggregatedAccumulators().size());
		assertEquals(1, manager.getPerJobUnfulfilledUserQueryFutures().size());
		assertEquals(1, manager.getPerJobCachedQueryResults().size());

		// There may be cases that one job is cleared before any registration.
		manager.clearAccumulatorsForJob(new JobID());
		assertEquals(1, manager.getPerJobAggregatedAccumulators().size());
		assertEquals(1, manager.getPerJobUnfulfilledUserQueryFutures().size());
		assertEquals(1, manager.getPerJobCachedQueryResults().size());

		manager.clearAccumulatorsForJob(jobId);
		assertEquals(0, manager.getPerJobAggregatedAccumulators().size());
		assertEquals(0, manager.getPerJobUnfulfilledUserQueryFutures().size());
		assertEquals(0, manager.getPerJobCachedQueryResults().size());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testConcurrentRegistrationAndCommission() throws InterruptedException {
		// Two jobs and each job has two accumulators.
		final int numberOfJobs = 2;
		final int numberOfAccumulatorsPerJob = 2;
		final int numberOfTasksPerPerJob = 4;

		Map<JobID, PerJobContext> jobs = new HashMap<>();
		for (int i = 0; i < numberOfJobs; ++i) {
			PerJobContext perJobContext = new PerJobContext(numberOfTasksPerPerJob, numberOfAccumulatorsPerJob);
			jobs.put(perJobContext.getJobId(), perJobContext);
		}

		JobManagerTable jobManagerTable = createJobManagerTable(new ArrayList<>(jobs.keySet()));

		for (Map.Entry<JobID, PerJobContext> entry : jobs.entrySet()) {
			JobMasterGateway gateway = jobManagerTable.get(entry.getKey()).getJobManagerGateway();

			doAnswer(new Answer<Void>() {
				@Override
				public Void answer(InvocationOnMock invocationOnMock) {
					synchronized (entry.getValue().getJobMasterLock()) {
						List<CommitAccumulator> commitAccumulators = (List<CommitAccumulator>) invocationOnMock.getArguments()[0];

						for (CommitAccumulator commitAccumulator : commitAccumulators) {
							entry.getValue().getGlobalCommittedAccumulators().compute(commitAccumulator.getName(), (k, v) -> {
								if (v == null) {
									return commitAccumulator.getAccumulator().clone();
								} else {
									v.merge(commitAccumulator.getAccumulator());
									return v;
								}
							});
						}
					}

					return null;
				}
			}).when(gateway).commitPreAggregatedAccumulator(any(List.class));
		}

		RPCBasedAccumulatorAggregationManager manager = new RPCBasedAccumulatorAggregationManager(jobManagerTable);

		List<Thread> taskThreads = new ArrayList<>(numberOfJobs * numberOfTasksPerPerJob);
		for (final JobID jobId : jobs.keySet()) {
			for (int j = 0; j < numberOfTasksPerPerJob; ++j) {
				final int currentTaskIndex = j;

				taskThreads.add(new Thread(new Runnable() {
					@Override
					public void run() {
						Random random = new Random();

						List<String> accumulators = new ArrayList<>(jobs.get(jobId).getAccumulators());
						Collections.shuffle(accumulators);

						for (String name : accumulators) {
							manager.registerPreAggregatedAccumulator(jobId,
								jobs.get(jobId).getJobVertexId(),
								jobs.get(jobId).getTasks().get(currentTaskIndex),
								name);
						}

						for (String name : accumulators) {
							manager.commitPreAggregatedAccumulator(jobId,
								jobs.get(jobId).getJobVertexId(),
								jobs.get(jobId).getTasks().get(currentTaskIndex),
								name,
								new IntCounter(currentTaskIndex + 1));
						}
					}
				}));
			}
		}

		for (Thread thread : taskThreads) {
			thread.start();
		}

		for (Thread thread : taskThreads) {
			thread.join();
		}

		// Check the result, every accumulator should have a final result of numberOfTasks * (numberOfTask + 1) / 2.
		for (Map.Entry<JobID, PerJobContext> entry : jobs.entrySet()) {
			assertEquals(numberOfAccumulatorsPerJob, entry.getValue().getGlobalCommittedAccumulators().size());
			for (String name : entry.getValue().getAccumulators()) {
				assertTrue("Accumulator with the name " + name + " not received.",
					entry.getValue().getGlobalCommittedAccumulators().containsKey(name));
				assertEquals(numberOfTasksPerPerJob * (numberOfTasksPerPerJob + 1) / 2,
					entry.getValue().getGlobalCommittedAccumulators().get(name).getLocalValue());
			}
		}
	}

	private JobManagerTable createJobManagerTable(List<JobID> jobIds) {
		JobManagerTable jobManagerTable = new JobManagerTable();

		for (JobID jobId : jobIds) {
			JobManagerConnection connection = new JobManagerConnection(
				jobId,
				new ResourceID("123456"),
				mock(JobMasterGateway.class),
				mock(TaskManagerActions.class),
				mock(CheckpointResponder.class),
				mock(LibraryCacheManager.class),
				mock(ResultPartitionConsumableNotifier.class),
				mock(PartitionProducerStateChecker.class)
			);

			jobManagerTable.add(connection);
		}

		return jobManagerTable;
	}

	/**
	 * Wrapper class for the context of a single job.
	 */
	private static class PerJobContext {
		private Object jobMasterLock = new Object();
		private JobID jobId = new JobID();
		private JobVertexID jobVertexId = new JobVertexID();
		private List<Integer> tasks = new ArrayList<>();
		private List<String> accumulators = new ArrayList<>();

		private Map<String, Accumulator> globalCommittedAccumulators = new HashMap<>();

		PerJobContext(int numberOfTasks, int numberOfAccumulators) {
			for (int i = 0; i < numberOfTasks; ++i) {
				tasks.add(i);
			}

			for (int i = 0; i < numberOfAccumulators; ++i) {
				accumulators.add(jobId + "_" + i);
			}
		}

		Object getJobMasterLock() {
			return jobMasterLock;
		}

		JobID getJobId() {
			return jobId;
		}

		JobVertexID getJobVertexId() {
			return jobVertexId;
		}

		List<Integer> getTasks() {
			return tasks;
		}

		List<String> getAccumulators() {
			return accumulators;
		}

		Map<String, Accumulator> getGlobalCommittedAccumulators() {
			return globalCommittedAccumulators;
		}
	}
}
