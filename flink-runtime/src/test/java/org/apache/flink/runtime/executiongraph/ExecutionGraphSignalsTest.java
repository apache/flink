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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.StoppingException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.runtime.executiongraph.restart.InfiniteDelayRestartStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.SerializedValue;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.api.mockito.PowerMockito;

import scala.concurrent.duration.FiniteDuration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ExecutionGraph.class)
public class ExecutionGraphSignalsTest {
	private ExecutionJobVertex[] mockEJV = new ExecutionJobVertex[5];
	private int[] dop = new int[] { 5, 7, 2, 11, 4 };
	private ExecutionVertex[][] mockEV = new ExecutionVertex[mockEJV.length][];
	private ExecutionGraph eg;
	private Field f;

	@Before
	public void prepare() throws Exception {
		final JobID jobId = new JobID();
		final String jobName = "Test Job Sample Name";
		final Configuration cfg = new Configuration();


		assert (mockEJV.length == 5);
		JobVertex v1 = new JobVertex("vertex1");
		JobVertex v2 = new JobVertex("vertex2");
		JobVertex v3 = new JobVertex("vertex3");
		JobVertex v4 = new JobVertex("vertex4");
		JobVertex v5 = new JobVertex("vertex5");

		for(int i = 0; i < mockEJV.length; ++i) {
			mockEJV[i] = mock(ExecutionJobVertex.class);

			this.mockEV[i] = new ExecutionVertex[dop[i]];
			for (int j = 0; j < dop[i]; ++j) {
				this.mockEV[i][j] = mock(ExecutionVertex.class);
			}

			when(mockEJV[i].getProducedDataSets()).thenReturn(new IntermediateResult[0]);
			when(mockEJV[i].getTaskVertices()).thenReturn(this.mockEV[i]);
		}

		PowerMockito
			.whenNew(ExecutionJobVertex.class)
			.withArguments(any(ExecutionGraph.class), same(v1), any(Integer.class).intValue(),
				any(FiniteDuration.class), any(Long.class).longValue()).thenReturn(mockEJV[0]);
		PowerMockito
			.whenNew(ExecutionJobVertex.class)
			.withArguments(any(ExecutionGraph.class), same(v2), any(Integer.class).intValue(),
				any(FiniteDuration.class), any(Long.class).longValue()).thenReturn(mockEJV[1]);
		PowerMockito
			.whenNew(ExecutionJobVertex.class)
			.withArguments(any(ExecutionGraph.class), same(v3), any(Integer.class).intValue(),
				any(FiniteDuration.class), any(Long.class).longValue()).thenReturn(mockEJV[2]);
		PowerMockito
			.whenNew(ExecutionJobVertex.class)
			.withArguments(any(ExecutionGraph.class), same(v4), any(Integer.class).intValue(),
				any(FiniteDuration.class), any(Long.class).longValue()).thenReturn(mockEJV[3]);
		PowerMockito
			.whenNew(ExecutionJobVertex.class)
			.withArguments(any(ExecutionGraph.class), same(v5), any(Integer.class).intValue(),
				any(FiniteDuration.class), any(Long.class).longValue()).thenReturn(mockEJV[4]);

		v1.setParallelism(dop[0]);
		v2.setParallelism(dop[1]);
		v3.setParallelism(dop[2]);
		v4.setParallelism(dop[3]);
		v5.setParallelism(dop[4]);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL);
		mockNumberOfInputs(1,0);
		v4.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL);
		mockNumberOfInputs(3,1);
		v4.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL);
		mockNumberOfInputs(3,2);
		v5.connectNewDataSetAsInput(v4, DistributionPattern.ALL_TO_ALL);
		mockNumberOfInputs(4,3);
		v5.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL);
		mockNumberOfInputs(4,2);

		List<JobVertex> ordered = new ArrayList<JobVertex>(Arrays.asList(v1, v2, v3, v4, v5));

		eg = new ExecutionGraph(
			TestingUtils.defaultExecutionContext(),
			TestingUtils.defaultExecutionContext(),
			jobId,
			jobName,
			cfg,
			new SerializedValue<>(new ExecutionConfig()),
			AkkaUtils.getDefaultTimeout(),
			new NoRestartStrategy());
		eg.attachJobGraph(ordered);

		f = eg.getClass().getDeclaredField("state");
		f.setAccessible(true);
	}

	private void mockNumberOfInputs(int nodeIndex, int predecessorIndex) {
		for(int j = 0; j < dop[nodeIndex]; ++j) {
			when(mockEV[nodeIndex][j].getNumberOfInputs()).thenReturn(dop[predecessorIndex]);
		}
	}

	@Test
	public void testCancel() throws Exception {
		assertEquals(JobStatus.CREATED, eg.getState());
		eg.cancel();

		verifyCancel(1);

		f.set(eg, JobStatus.RUNNING);
		eg.cancel();

		verifyCancel(2);
		assertEquals(JobStatus.CANCELLING, eg.getState());

		eg.cancel();

		verifyCancel(2);
		assertEquals(JobStatus.CANCELLING, eg.getState());

		f.set(eg, JobStatus.CANCELED);
		eg.cancel();

		verifyCancel(2);
		assertEquals(JobStatus.CANCELED, eg.getState());

		f.set(eg, JobStatus.FAILED);
		eg.cancel();

		verifyCancel(2);
		assertEquals(JobStatus.FAILED, eg.getState());

		f.set(eg, JobStatus.FAILING);
		eg.cancel();

		verifyCancel(2);
		assertEquals(JobStatus.CANCELLING, eg.getState());

		f.set(eg, JobStatus.FINISHED);
		eg.cancel();

		verifyCancel(2);
		assertEquals(JobStatus.FINISHED, eg.getState());

		f.set(eg, JobStatus.RESTARTING);
		eg.cancel();

		verifyCancel(2);
		assertEquals(JobStatus.CANCELED, eg.getState());
	}

	private void verifyCancel(int times) {
		for (int i = 0; i < mockEJV.length; ++i) {
			verify(mockEJV[i], times(times)).cancel();
		}
	}

	/**
	 * Tests that suspend cancels the ExecutionJobVertices and transitions to SUSPENDED state.
	 * Tests also that one cannot leave the SUSPENDED state to enter a terminal state.
	 */
	@Test
	public void testSuspend() throws Exception {
		assertEquals(JobStatus.CREATED, eg.getState());
		Exception testException = new Exception("Test exception");

		eg.suspend(testException);

		verifyCancel(1);
		assertEquals(JobStatus.SUSPENDED, eg.getState());

		f.set(eg, JobStatus.RUNNING);

		eg.suspend(testException);

		verifyCancel(2);
		assertEquals(JobStatus.SUSPENDED, eg.getState());

		f.set(eg, JobStatus.FAILING);

		eg.suspend(testException);

		verifyCancel(3);
		assertEquals(JobStatus.SUSPENDED, eg.getState());

		f.set(eg, JobStatus.CANCELLING);

		eg.suspend(testException);

		verifyCancel(4);
		assertEquals(JobStatus.SUSPENDED, eg.getState());

		f.set(eg, JobStatus.FAILED);

		eg.suspend(testException);

		verifyCancel(4);
		assertEquals(JobStatus.FAILED, eg.getState());

		f.set(eg, JobStatus.FINISHED);

		eg.suspend(testException);

		verifyCancel(4);
		assertEquals(JobStatus.FINISHED, eg.getState());

		f.set(eg, JobStatus.CANCELED);

		eg.suspend(testException);

		verifyCancel(4);
		assertEquals(JobStatus.CANCELED, eg.getState());

		f.set(eg, JobStatus.SUSPENDED);

		eg.fail(testException);

		assertEquals(JobStatus.SUSPENDED, eg.getState());

		eg.cancel();

		assertEquals(JobStatus.SUSPENDED, eg.getState());
	}

	// test that all source tasks receive STOP signal
	// test that all non-source tasks do not receive STOP signal
	@Test
	public void testStop() throws Exception {
		Field f = eg.getClass().getDeclaredField("isStoppable");
		f.setAccessible(true);
		f.set(eg, true);

		eg.stop();

		for (int i : new int[]{0,2}) {
			for (int j = 0; j < mockEV[i].length; ++j) {
				verify(mockEV[i][j], times(1)).stop();
			}
		}

		for (int i : new int[]{1,3,4}) {
			for (int j = 0; j < mockEV[i].length; ++j) {
				verify(mockEV[i][j], times(0)).stop();
			}
		}
	}

	/**
	 * Test that failing in state restarting will retrigger the restarting logic. This means that
	 * it only goes into the state FAILED after the restart strategy says the job is no longer
	 * restartable.
	 */
	@Test
	public void testFailureWhileRestarting() throws IllegalAccessException, NoSuchFieldException, InterruptedException {
		Field restartStrategyField = eg.getClass().getDeclaredField("restartStrategy");
		restartStrategyField.setAccessible(true);

		restartStrategyField.set(eg, new InfiniteDelayRestartStrategy(1));

		f.set(eg, JobStatus.RESTARTING);

		eg.fail(new Exception("Test"));

		// we should restart since we have one restart attempt left
		assertEquals(JobStatus.RESTARTING, eg.getState());

		eg.fail(new Exception("Test"));

		// after depleting all our restart attempts we should go into Failed
		assertEquals(JobStatus.FAILED, eg.getState());
	}

	/**
	 * Tests that a {@link SuppressRestartsException} in state RESTARTING stops the restarting
	 * immediately and sets the execution graph's state to FAILED.
	 */
	@Test
	public void testSuppressRestartFailureWhileRestarting() throws IllegalAccessException, NoSuchFieldException {
		Field restartStrategyField = eg.getClass().getDeclaredField("restartStrategy");
		restartStrategyField.setAccessible(true);

		restartStrategyField.set(eg, new InfiniteDelayRestartStrategy());

		f.set(eg, JobStatus.RESTARTING);

		// suppress a possible restart
		eg.fail(new SuppressRestartsException(new Exception("Test")));

		assertEquals(JobStatus.FAILED, eg.getState());
	}

	/**
	 * Tests that we can suspend a job when in state RESTARTING.
	 */
	@Test
	public void testSuspendWhileRestarting() throws IllegalAccessException, NoSuchFieldException {
		Field restartStrategyField = eg.getClass().getDeclaredField("restartStrategy");
		restartStrategyField.setAccessible(true);

		restartStrategyField.set(eg, new InfiniteDelayRestartStrategy());

		f.set(eg, JobStatus.RESTARTING);

		final Exception exception = new Exception("Suspended");

		eg.suspend(exception);

		assertEquals(JobStatus.SUSPENDED, eg.getState());

		assertEquals(exception, eg.getFailureCause());
	}

	// STOP only supported if all sources are stoppable 
	@Test(expected = StoppingException.class)
	public void testStopBatching() throws StoppingException {
		eg.stop();
	}

	/**
	 * Tests that a failing scheduleOrUpdateConsumers call with a non-existing execution attempt
	 * id, will not fail the execution graph.
	 */
	@Test
	public void testFailingScheduleOrUpdateConsumers() throws IllegalAccessException {
		IntermediateResultPartitionID intermediateResultPartitionId = new IntermediateResultPartitionID();
		// The execution attempt id does not exist and thus the scheduleOrUpdateConsumers call
		// should fail
		ExecutionAttemptID producerId = new ExecutionAttemptID();
		ResultPartitionID resultPartitionId = new ResultPartitionID(intermediateResultPartitionId, producerId);

		f.set(eg, JobStatus.RUNNING);

		assertEquals(JobStatus.RUNNING, eg.getState());

		try {
			eg.scheduleOrUpdateConsumers(resultPartitionId);
			fail("Expected ExecutionGraphException.");
		} catch (ExecutionGraphException e) {
			// we've expected this exception to occur
		}

		assertEquals(JobStatus.RUNNING, eg.getState());
	}
}
