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

package org.apache.flink.table.resource.batch.schedule;

import org.apache.flink.runtime.event.ExecutionVertexStateChangedEvent;
import org.apache.flink.runtime.event.ResultPartitionConsumableEvent;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.schedule.VertexScheduler;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.table.plan.nodes.exec.BatchExecNode;
import org.apache.flink.table.plan.nodes.physical.batch.BatchPhysicalRel;
import org.apache.flink.table.resource.batch.BatchExecNodeStage;
import org.apache.flink.table.resource.batch.NodeRunningUnit;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for RunningUnitGraphManagerPlugin.
 */
public class RunningUnitGraphManagerPluginTest extends TestLogger {

	private List<JobVertex> jobVertexList;
	private List<NodeRunningUnit> nodeRunningUnitList;
	private List<BatchExecNodeStage> nodeStageList;
	private VertexScheduler scheduler;
	private JobGraph jobGraph;

	@Before
	public void setUp() {
		jobVertexList = new ArrayList<>();
		nodeRunningUnitList = new ArrayList<>();
		nodeStageList = new ArrayList<>();
		scheduler = mock(VertexScheduler.class);
		jobGraph = mock(JobGraph.class);
	}

	@Test
	public void testSchedule() throws Exception {

		createJobVertexIDs(6);

		Map<JobVertexID, ArrayList<Integer>> vertexToStreamNodeIds = new HashMap<>();
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(0).getID(), k->new ArrayList<>()).add(0);
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(1).getID(), k->new ArrayList<>()).add(1);
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(2).getID(), k->new ArrayList<>()).add(2);
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(3).getID(), k->new ArrayList<>()).addAll(Arrays.asList(3, 4));
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(4).getID(), k->new ArrayList<>()).add(6);
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(5).getID(), k->new ArrayList<>()).addAll(Arrays.asList(7, 8, 9));

		createNodeStages(13);
		nodeStageList.get(0).addTransformation(mockTransformation(0));
		nodeStageList.get(1).addTransformation(mockTransformation(1));
		nodeStageList.get(2).addTransformation(mockTransformation(2));
		nodeStageList.get(3).addTransformation(mockTransformation(3));
		nodeStageList.get(4).addTransformation(mockTransformation(3));
		nodeStageList.get(5).addTransformation(mockTransformation(3));
		nodeStageList.get(5).addDependStage(nodeStageList.get(3), BatchExecNodeStage.DependType.DATA_TRIGGER);
		nodeStageList.get(5).addDependStage(nodeStageList.get(4), BatchExecNodeStage.DependType.DATA_TRIGGER);
		nodeStageList.get(6).addTransformation(mockTransformation(4));
		nodeStageList.get(7).addTransformation(mockTransformation(6));
		nodeStageList.get(8).addTransformation(mockTransformation(7));
		nodeStageList.get(9).addTransformation(mockTransformation(7));
		nodeStageList.get(9).addDependStage(nodeStageList.get(8), BatchExecNodeStage.DependType.PRIORITY);
		nodeStageList.get(10).addTransformation(mockTransformation(8));
		nodeStageList.get(11).addTransformation(mockTransformation(8));
		nodeStageList.get(11).addDependStage(nodeStageList.get(10), BatchExecNodeStage.DependType.DATA_TRIGGER);
		nodeStageList.get(12).addTransformation(mockTransformation(9));

		createNodeRunningUnits(5);

		buildRunningUnits(Arrays.asList(0, 1, 3), Arrays.asList(2, 4), Arrays.asList(5, 6, 7, 8), Arrays.asList(9, 10), Arrays.asList(11, 12));

		RunningUnitGraphManagerPlugin plugin = new RunningUnitGraphManagerPlugin();
		plugin.open(scheduler, jobGraph, vertexToStreamNodeIds, nodeRunningUnitList);

		plugin.onSchedulingStarted();
		verifyScheduleJobVertex(0, 1, 3);
		triggerJobVertexStatusChanged(plugin, 0, 1);
		triggerTaskStatusChanged(plugin, 3, 0);
		verify(scheduler, times(6)).scheduleExecutionVertices(any());
		triggerTaskStatusChanged(plugin, 3, 1);
		verifyScheduleJobVertex(2);
		triggerJobVertexStatusChanged(plugin, 2);
		triggerPartitionConsumable(plugin, 3);
		verifyScheduleJobVertex(4, 5);
	}

	private void setNodeStage(int nodeStageIndex, Integer... transformationIDs) {
		when(nodeStageList.get(nodeStageIndex).getTransformationIDList()).thenReturn(Arrays.asList(transformationIDs));
	}

	@Test
	public void testScheduleChain() {
		createJobVertexIDs(5);

		Map<JobVertexID, ArrayList<Integer>> vertexToStreamNodeIds = new HashMap<>();
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(0).getID(), k->new ArrayList<>()).add(0);
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(1).getID(), k->new ArrayList<>()).addAll(Arrays.asList(1, 2));
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(2).getID(), k->new ArrayList<>()).add(3);
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(3).getID(), k->new ArrayList<>()).add(4);
		vertexToStreamNodeIds.computeIfAbsent(jobVertexList.get(4).getID(), k->new ArrayList<>()).add(5);

		createNodeStages(13);
		nodeStageList.get(0).addTransformation(mockTransformation(0));
		nodeStageList.get(1).addTransformation(mockTransformation(1));
		nodeStageList.get(2).addTransformation(mockTransformation(1));
		nodeStageList.get(2).addDependStage(nodeStageList.get(1), BatchExecNodeStage.DependType.DATA_TRIGGER);
		nodeStageList.get(3).addTransformation(mockTransformation(2));
		nodeStageList.get(4).addTransformation(mockTransformation(2));
		nodeStageList.get(4).addDependStage(nodeStageList.get(3), BatchExecNodeStage.DependType.PRIORITY);
		nodeStageList.get(5).addTransformation(mockTransformation(3));
		nodeStageList.get(6).addTransformation(mockTransformation(4));

		createNodeRunningUnits(5);

		buildRunningUnits(Arrays.asList(0, 1), Arrays.asList(2, 3), Arrays.asList(4, 5, 6));

		RunningUnitGraphManagerPlugin plugin = new RunningUnitGraphManagerPlugin();
		plugin.open(scheduler, jobGraph, vertexToStreamNodeIds, nodeRunningUnitList);

		plugin.onSchedulingStarted();
		verifyScheduleJobVertex(0, 1);
		triggerJobVertexStatusChanged(plugin, 0, 1);
		verifyScheduleJobVertex(2, 3);
		triggerJobVertexStatusChanged(plugin, 2, 3);
		verifyScheduleJobVertex(4);
	}

	private void buildRunningUnits(List<Integer>... nodeStageIndexGroups) {
		int runningUnitIndex = 0;
		for (List<Integer> group : nodeStageIndexGroups) {
			NodeRunningUnit runningUnit = new NodeRunningUnit();
			nodeRunningUnitList.add(runningUnitIndex, runningUnit);
			for (int nodeStageIndex : group) {
				runningUnit.addNodeStage(nodeStageList.get(nodeStageIndex));
				nodeStageList.get(nodeStageIndex).addRunningUnit(runningUnit);
			}
			runningUnitIndex++;
		}
	}

	private void triggerPartitionConsumable(RunningUnitGraphManagerPlugin plugin, int producerIndex) {
		IntermediateDataSetID id = new IntermediateDataSetID();
		when(jobGraph.getResultProducerID(id)).thenReturn(jobVertexList.get(producerIndex).getID());
		ResultPartitionConsumableEvent event = new ResultPartitionConsumableEvent(id, 0);
		plugin.onResultPartitionConsumable(event);
	}

	private void verifyScheduleJobVertex(int... jobVertexIndexes) {
		for (int jobVertexIndex : jobVertexIndexes) {
			verify(scheduler).scheduleExecutionVertices(Collections.singletonList(new ExecutionVertexID(jobVertexList.get(jobVertexIndex).getID(), 0)));
			verify(scheduler).scheduleExecutionVertices(Collections.singletonList(new ExecutionVertexID(jobVertexList.get(jobVertexIndex).getID(), 1)));
		}
	}

	private void triggerJobVertexStatusChanged(RunningUnitGraphManagerPlugin plugin, int... jobVertexIndexes) {
		for (int jobVertexIndex : jobVertexIndexes) {
			triggerTaskStatusChanged(plugin, jobVertexIndex, 0);
			triggerTaskStatusChanged(plugin, jobVertexIndex, 1);
		}
	}

	private void triggerTaskStatusChanged(RunningUnitGraphManagerPlugin plugin, int jobVertexIndex, int taskIndex) {
		ExecutionVertexStateChangedEvent event = new ExecutionVertexStateChangedEvent(new ExecutionVertexID(jobVertexList.get(jobVertexIndex).getID(), taskIndex), ExecutionState.DEPLOYING);
		plugin.onExecutionVertexStateChanged(event);
	}

	private void createNodeStages(int num) {
		for (int i = 0; i < num; i++) {
			BatchExecNode<?> batchExecNode = mock(BatchExecNode.class);
			BatchPhysicalRel batchRel = mock(BatchPhysicalRel.class);
			when(batchExecNode.getFlinkPhysicalRel()).thenReturn(batchRel);
			nodeStageList.add(new BatchExecNodeStage(batchExecNode, 0));
		}
	}

	private StreamTransformation mockTransformation(int transformationID) {
		StreamTransformation transformation = mock(StreamTransformation.class);
		when(transformation.getId()).thenReturn(transformationID);
		return transformation;
	}

	private void createNodeRunningUnits(int num) {
		for (int i = 0; i < num; i++) {
			nodeRunningUnitList.add(mock(NodeRunningUnit.class));
		}
	}

	private void createJobVertexIDs(int num) {
		for (int i = 0; i < num; i++) {
			JobVertexID jobVertexID = new JobVertexID();
			JobVertex jobVertex = new JobVertex(String.valueOf(i), jobVertexID);
			jobVertex.setParallelism(2);
			jobVertexList.add(jobVertex);
		}
		when(jobGraph.getVerticesAsArray()).thenReturn(jobVertexList.toArray(new JobVertex[jobVertexList.size()]));
	}
}
