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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.InfiniteDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.testtasks.NoOpInvokable;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Validates that reconciling out of various states.
 */
public class ExecutionGraphReconcilingTest {

	/**
	 * Reconciling from CREATED goes to RECONCILING directly.
	 */
	@Test
	public void testReconcilingOutOfCreated() throws Exception {
		final ExecutionGraph eg = createExecutionGraph();

		assertEquals(JobStatus.CREATED, eg.getState());

		// reconcile
		eg.reconcile();

		assertEquals(JobStatus.RECONCILING, eg.getState());
		for (ExecutionVertex ev : eg.getAllExecutionVertices()) {
			assertEquals(ExecutionState.RECONCILING, ev.getCurrentExecutionAttempt().getState());
		}
	}

	/**
	 * Reconciling from RUNNING state will throw an illegal state exception.
	 */
	@Test
	public void testReconcilingOutOfRunning() throws Exception {
		final ExecutionGraph eg = createExecutionGraph();

		eg.scheduleForExecution();
		ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

		assertEquals(JobStatus.RUNNING, eg.getState());

		boolean hasFailure = false;
		try {
			eg.reconcile();
		} catch (IllegalStateException e) {
			assertTrue(e.getMessage().contains("Job is not in expected CREATED state"));
			hasFailure = true;
		}
		assertTrue("We must see a failure because reconciling only works from CREATED state.", hasFailure);
	}

	/**
	 * Reconciling from FAILING state will throw an illegal state exception.
	 */
	@Test
	public void testReconcilingOutOfFailing() throws Exception {
		final ExecutionGraph eg = createExecutionGraph();

		eg.scheduleForExecution();
		ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

		eg.failGlobal(new Exception("fail global"));

		assertEquals(JobStatus.FAILING, eg.getState());

		boolean hasFailure = false;
		try {
			eg.reconcile();
		} catch (IllegalStateException e) {
			assertTrue(e.getMessage().contains("Job is not in expected CREATED state"));
			hasFailure = true;
		}
		assertTrue("We must see a failure because reconciling only works from CREATED state.", hasFailure);
	}

	/**
	 * Reconciling from FAILED state will throw an illegal state exception.
	 */
	@Test
	public void testReconcilingOutOfFailed() throws Exception {
		final ExecutionGraph eg = createExecutionGraph();

		eg.scheduleForExecution();
		ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

		eg.failGlobal(new Exception("test"));
		assertEquals(JobStatus.FAILING, eg.getState());

		ExecutionGraphTestUtils.completeCancellingForAllVertices(eg);
		assertEquals(JobStatus.FAILED, eg.getState());

		boolean hasFailure = false;
		try {
			eg.reconcile();
		} catch (IllegalStateException e) {
			assertTrue(e.getMessage().contains("Job is not in expected CREATED state"));
			hasFailure = true;
		}
		assertTrue("We must see a failure because reconciling only works from CREATED state.", hasFailure);
	}

	/**
	 * Reconciling from CANCELING state will throw an illegal state exception.
	 */
	@Test
	public void testReconcilingOutOfCanceling() throws Exception {
		final ExecutionGraph eg = createExecutionGraph();

		eg.scheduleForExecution();
		ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

		eg.cancel();
		assertEquals(JobStatus.CANCELLING, eg.getState());

		boolean hasFailure = false;
		try {
			eg.reconcile();
		} catch (IllegalStateException e) {
			assertTrue(e.getMessage().contains("Job is not in expected CREATED state"));
			hasFailure = true;
		}
		assertTrue("We must see a failure because reconciling only works from CREATED state.", hasFailure);
	}

	/**
	 * Reconciling from CANCELLED state will throw an illegal state exception.
	 */
	@Test
	public void testReconcilingOutOfCanceled() throws Exception {
		final ExecutionGraph eg = createExecutionGraph();

		eg.scheduleForExecution();
		ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

		eg.cancel();
		assertEquals(JobStatus.CANCELLING, eg.getState());

		ExecutionGraphTestUtils.completeCancellingForAllVertices(eg);
		assertEquals(JobStatus.CANCELED, eg.getState());

		boolean hasFailure = false;
		try {
			eg.reconcile();
		} catch (IllegalStateException e) {
			assertTrue(e.getMessage().contains("Job is not in expected CREATED state"));
			hasFailure = true;
		}
		assertTrue("We must see a failure because reconciling only works from CREATED state.", hasFailure);
	}

	/**
	 * Reconciling from RESTARTING state will throw an illegal state exception.
	 */
	@Test
	public void testReconcilingWhileRestarting() throws Exception {
		final ExecutionGraph eg = ExecutionGraphTestUtils.createSimpleTestGraph(new InfiniteDelayRestartStrategy(10));
		eg.scheduleForExecution();

		assertEquals(JobStatus.RUNNING, eg.getState());
		ExecutionGraphTestUtils.switchAllVerticesToRunning(eg);

		eg.failGlobal(new Exception("test"));
		assertEquals(JobStatus.FAILING, eg.getState());

		ExecutionGraphTestUtils.completeCancellingForAllVertices(eg);
		assertEquals(JobStatus.RESTARTING, eg.getState());

		boolean hasFailure = false;
		try {
			eg.reconcile();
		} catch (IllegalStateException e) {
			assertTrue(e.getMessage().contains("Job is not in expected CREATED state"));
			hasFailure = true;
		}
		assertTrue("We must see a failure because reconciling only works from CREATED state.", hasFailure);
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	private static ExecutionGraph createExecutionGraph() throws Exception {
		final JobID jobId = new JobID();

		final JobVertex vertex = new JobVertex("vertex");
		vertex.setInvokableClass(NoOpInvokable.class);

		return ExecutionGraphTestUtils.createSimpleTestGraph(
			jobId,
			new SimpleSlotProvider(jobId, 1),
			new FixedDelayRestartStrategy(0, 0),
			vertex);
	}
}
