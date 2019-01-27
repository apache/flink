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
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.io.network.partition.BlockingShuffleType;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.executiongraph.ExecutionTest.createNoOpJobVertex;
import static org.apache.flink.runtime.executiongraph.ExecutionTest.createProgrammedSlotProvider;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for the resource calculation for the {@link ExecutionVertex}.
 */
public class ExecutionVertexResourceCalculationTest {

	@Test
	public void testResourceCalculationBeforeSlotAllocationInScheduled() throws Exception {
		// Prepares input gates and input channels.
		// 0. Prepares for various settings.
		final int numPipelineChannelsPerGate = 10;
		final int numPipelineGates = 2;
		final int numConsumersPerExternalResultPartition = 10;
		final int numExternalResultPartitions = 3;

		final int networkBuffersPerChannel = 100;
		final int networkBuffersPerSubpartition = 50;
		final int networkExtraBuffersPerGate = 20;
		final int taskManagerOutputMemoryMB = 170;

		final float cpuCores = 1.0f;
		final int heapMemoryInMB = 1024;
		final int directMemoryInMB = 200;
		final int nativeMemoryInMB = 100;
		final int managedMemoryInMB = 1000;

		// 1. Prepares for pipelined input edges.
		IntermediateResult mockPipelinedIntermediateResult = new IntermediateResult(
			new IntermediateDataSetID(),
			mock(ExecutionJobVertex.class),
			numPipelineChannelsPerGate,
			ResultPartitionType.PIPELINED);

		IntermediateResultPartition mockPipelinedIntermediateResultPartition =
			mock(IntermediateResultPartition.class);
		when(mockPipelinedIntermediateResultPartition.getIntermediateResult())
			.thenReturn(mockPipelinedIntermediateResult);

		ExecutionEdge mockExecutionEdge = mock(ExecutionEdge.class);
		when(mockExecutionEdge.getSource()).thenReturn(mockPipelinedIntermediateResultPartition);

		ExecutionEdge[][] inputEdges = new ExecutionEdge[numPipelineGates][numPipelineChannelsPerGate];
		for (int i = 0; i < numPipelineGates; i++) {
			ExecutionEdge[] edgesPerGate = new ExecutionEdge[numPipelineChannelsPerGate];
			for (int j = 0; j < numPipelineChannelsPerGate; j++) {
				edgesPerGate[j] = mockExecutionEdge;
			}
			inputEdges[i] = edgesPerGate;
		}

		// 2. Prepares for blocking output edges using external shuffle service.
		IntermediateResult mockBlockingIntermediateResult = new IntermediateResult(
			new IntermediateDataSetID(),
			mock(ExecutionJobVertex.class),
			numPipelineChannelsPerGate,
			ResultPartitionType.BLOCKING);

		IntermediateResultPartition mockBlockingIntermediateResultPartition =
			mock(IntermediateResultPartition.class);
		when(mockBlockingIntermediateResultPartition.getIntermediateResult())
			.thenReturn(mockBlockingIntermediateResult);
		List<List<ExecutionEdge>> consumersPerExternalResultPartition = new ArrayList<>();
		consumersPerExternalResultPartition.add(new ArrayList<>());
		for (int i = 0; i < numConsumersPerExternalResultPartition; i++) {
			consumersPerExternalResultPartition.get(0).add(mockExecutionEdge);
		}
		when(mockBlockingIntermediateResultPartition.getConsumers())
			.thenReturn(consumersPerExternalResultPartition);

		Map<IntermediateResultPartitionID, IntermediateResultPartition> producedPartitions =
			new HashMap<>();
		for (int i = 0; i < numExternalResultPartitions; i++) {
			producedPartitions.put(new IntermediateResultPartitionID(),
				mockBlockingIntermediateResultPartition);
		}

		// 3. Prepares other facilities for this unittest.
		final JobVertex jobVertex = spy(createNoOpJobVertex());
		when(jobVertex.getMinResources()).thenReturn(ResourceSpec.newBuilder()
			.setCpuCores(cpuCores)
			.setHeapMemoryInMB(heapMemoryInMB)
			.setDirectMemoryInMB(directMemoryInMB)
			.setNativeMemoryInMB(nativeMemoryInMB)
			.addExtendedResource(new CommonExtendedResource(
				ResourceSpec.MANAGED_MEMORY_NAME, managedMemoryInMB))
			.build());

		final JobVertexID jobVertexId = jobVertex.getID();

		final ExecutionTest.SingleSlotTestingSlotOwner slotOwner = new ExecutionTest.SingleSlotTestingSlotOwner();

		final SimpleSlot slot = new SimpleSlot(
			slotOwner,
			new LocalTaskManagerLocation(),
			0,
			new SimpleAckingTaskManagerGateway());

		final AtomicReference<SlotProfile> actualSlotProfile = new AtomicReference(null);

		final ProgrammedSlotProvider slotProvider = spy(new ProgrammedSlotProvider(1) {
			@Override
			public CompletableFuture<LogicalSlot> allocateSlot(
				SlotRequestId slotRequestId,
				ScheduledUnit task,
				boolean allowQueued,
				SlotProfile slotProfile,
				Time allocationTimeout) {

				actualSlotProfile.set(slotProfile);
				return super.allocateSlot(
					slotRequestId, task, allowQueued, slotProfile, allocationTimeout);
			}
		});

		slotProvider.addSlot(jobVertexId, 0, CompletableFuture.completedFuture(slot));

		Configuration jobManagerConfiguration = new Configuration();
		jobManagerConfiguration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL, networkBuffersPerChannel);
		jobManagerConfiguration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_SUBPARTITION, networkBuffersPerSubpartition);
		jobManagerConfiguration.setInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE, networkExtraBuffersPerGate);
		jobManagerConfiguration.setString(TaskManagerOptions.TASK_BLOCKING_SHUFFLE_TYPE,
			BlockingShuffleType.YARN.toString());
		jobManagerConfiguration.setInteger(TaskManagerOptions.TASK_MANAGER_OUTPUT_MEMORY_MB, taskManagerOutputMemoryMB);

		ExecutionGraph executionGraph = ExecutionGraphTestUtils.createSimpleTestGraph(
			new JobID(),
			jobManagerConfiguration,
			slotProvider,
			new NoRestartStrategy(),
			TestingUtils.defaultExecutor(),
			jobVertex);

		ExecutionJobVertex executionJobVertex = spy(executionGraph.getJobVertex(jobVertexId));

		final Execution execution = spy(executionJobVertex.getTaskVertices()[0].getCurrentExecutionAttempt());

		ExecutionVertex executionVertex = spy(execution.getVertex());
		when(executionVertex.getNumberOfInputs()).thenReturn(inputEdges.length);
		doAnswer(new Answer<ExecutionEdge[]>() {
			@Override
			public ExecutionEdge[] answer(InvocationOnMock invocation) {
				int index = invocation.getArgumentAt(0, int.class);
				return inputEdges[index];
			}
		}).when(executionVertex).getInputEdges(any(int.class));
		when(executionVertex.getProducedPartitions()).thenReturn(producedPartitions);

		when(execution.getVertex()).thenReturn(executionVertex);

		CompletableFuture<Execution> allocationFuture = execution.allocateAndAssignSlotForExecution(
			slotProvider,
			false,
			LocationPreferenceConstraint.ALL,
			TestingUtils.infiniteTime());

		assertTrue(allocationFuture.isDone());

		assertEquals(ExecutionState.SCHEDULED, execution.getState());

		assertEquals(slot, execution.getAssignedResource());

		// cancelling the execution should move it into state CANCELED
		execution.cancel();
		assertEquals(ExecutionState.CANCELED, execution.getState());

		assertEquals(slot, slotOwner.getReturnedSlotFuture().get());

		assertTrue(actualSlotProfile.get() != null);
		assertEquals((int) Math.ceil(1.0 * (networkBuffersPerChannel * numPipelineChannelsPerGate * numPipelineGates
				+ networkExtraBuffersPerGate * numPipelineGates) * 32 / 1024),
			actualSlotProfile.get().getResourceProfile().getNetworkMemoryInMB());
		assertEquals(managedMemoryInMB + taskManagerOutputMemoryMB * numExternalResultPartitions,
			actualSlotProfile.get().getResourceProfile().getManagedMemoryInMB());
	}

	@Test
	public void testNetworkMemoryCalculation() throws Exception {
		int[][] parameters = {{2, 8, 128, 2, 10, 10 * 128 + 2 * 2 + 2 * 10 + 8},
			{2, 8, 128, 2, 1, 2 * 128 + 2 * 2 + 2 * 10 + 8}};

		for (int[] parameter: parameters) {
			testNetworkMemoryCalculation(parameter);
		}
	}

	private void testNetworkMemoryCalculation(int[] parameters) throws Exception {
		final int NETWORK_BUFFERS_PER_CHANNEL = parameters[0];
		final int NETWORK_EXTRA_BUFFERS_PER_GATE = parameters[1];
		final int NETWORK_BUFFERS_PER_BLOCKING_CHANNEL = parameters[2];
		final int NETWORK_EXTRA_BUFFERS_PER_BLOCKING_GATE = parameters[3];
		final int YARN_SHUFFLE_SERVICE_MAX_REQUESTS_IN_FLIGHT = parameters[4];

		final JobVertex jobVertex1 = createNoOpJobVertex();
		final JobVertexID jobVertexId1 = jobVertex1.getID();

		final JobVertex jobVertex2 = createNoOpJobVertex();
		jobVertex2.setParallelism(10);

		final JobVertex jobVertex3 = createNoOpJobVertex();
		jobVertex3.setParallelism(10);

		final JobVertex jobVertex4 = createNoOpJobVertex();
		jobVertex4.setParallelism(10);

		final Configuration configuration = new Configuration();
		configuration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL, NETWORK_BUFFERS_PER_CHANNEL);
		configuration.setInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE, NETWORK_EXTRA_BUFFERS_PER_GATE);
		configuration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_EXTERNAL_BLOCKING_CHANNEL, NETWORK_BUFFERS_PER_BLOCKING_CHANNEL);
		configuration.setInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_EXTERNAL_BLOCKING_GATE, NETWORK_EXTRA_BUFFERS_PER_BLOCKING_GATE);
		configuration.setInteger(TaskManagerOptions.TASK_EXTERNAL_SHUFFLE_MAX_CONCURRENT_REQUESTS, YARN_SHUFFLE_SERVICE_MAX_REQUESTS_IN_FLIGHT);
		configuration.setString(TaskManagerOptions.TASK_BLOCKING_SHUFFLE_TYPE, BlockingShuffleType.YARN.toString());
		configuration.setInteger(TaskManagerOptions.MEMORY_SEGMENT_SIZE, 1024 * 1024);

		IntermediateDataSetID dataSetID1 = new IntermediateDataSetID();
		IntermediateDataSet dataSet1 = mock(IntermediateDataSet.class);
		when(dataSet1.getId()).thenReturn(dataSetID1);

		IntermediateDataSetID dataSetID2 = new IntermediateDataSetID();
		IntermediateDataSet dataSet2 = mock(IntermediateDataSet.class);
		when(dataSet2.getId()).thenReturn(dataSetID2);

		IntermediateDataSetID dataSetID3 = new IntermediateDataSetID();
		IntermediateDataSet dataSet3 = mock(IntermediateDataSet.class);
		when(dataSet3.getId()).thenReturn(dataSetID3);

		DistributionPattern distributionPattern = DistributionPattern.ALL_TO_ALL;

		jobVertex4.connectDataSetAsInput(jobVertex1, dataSet1.getId(), distributionPattern, ResultPartitionType.BLOCKING);

		jobVertex4.connectDataSetAsInput(jobVertex2, dataSet2.getId(), distributionPattern, ResultPartitionType.PIPELINED);

		jobVertex4.connectDataSetAsInput(jobVertex3, dataSet3.getId(), distributionPattern, ResultPartitionType.BLOCKING);

		final ExecutionTest.SingleSlotTestingSlotOwner slotOwner = new ExecutionTest.SingleSlotTestingSlotOwner();
		final ProgrammedSlotProvider slotProvider = createProgrammedSlotProvider(
			1,
			Collections.singleton(jobVertexId1),
			slotOwner);

		ExecutionGraph executionGraph = ExecutionGraphTestUtils.createSimpleTestGraph(
			new JobID(),
			configuration,
			slotProvider,
			new NoRestartStrategy(),
			TestingUtils.defaultExecutor(),
			new JobVertex[] {jobVertex1, jobVertex2, jobVertex3, jobVertex4});

		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertex4.getID());

		ExecutionVertex executionVertex = executionJobVertex.getTaskVertices()[0];

		assertEquals(parameters[5], executionVertex.calculateTaskNetworkMemory());
	}
}
