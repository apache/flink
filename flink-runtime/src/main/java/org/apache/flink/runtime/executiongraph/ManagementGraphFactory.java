/**
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.io.network.channels.ChannelType;
import org.apache.flink.runtime.managementgraph.ManagementEdge;
import org.apache.flink.runtime.managementgraph.ManagementEdgeID;
import org.apache.flink.runtime.managementgraph.ManagementGate;
import org.apache.flink.runtime.managementgraph.ManagementGateID;
import org.apache.flink.runtime.managementgraph.ManagementGraph;
import org.apache.flink.runtime.managementgraph.ManagementGroupEdge;
import org.apache.flink.runtime.managementgraph.ManagementGroupVertex;
import org.apache.flink.runtime.managementgraph.ManagementStage;
import org.apache.flink.runtime.managementgraph.ManagementVertex;

public class ManagementGraphFactory {

	private ManagementGraphFactory() {
	}

	public static ManagementGraph fromExecutionGraph(ExecutionGraph executionGraph) {

		final ManagementGraph managementGraph = new ManagementGraph(executionGraph.getJobID());

		final Map<ExecutionStage, ManagementStage> stageMap = addExecutionStages(managementGraph, executionGraph);
		final Map<ExecutionGroupVertex, ManagementGroupVertex> groupMap = addGroupVertices(stageMap);
		addExecutionVertices(groupMap, executionGraph);

		return managementGraph;
	}

	private static Map<ExecutionStage, ManagementStage> addExecutionStages(ManagementGraph managementGraph,
			ExecutionGraph executionGraph) {

		final Map<ExecutionStage, ManagementStage> stageMap = new HashMap<ExecutionStage, ManagementStage>();

		for (int i = 0; i < executionGraph.getNumberOfStages(); i++) {

			final ExecutionStage executionStage = executionGraph.getStage(i);
			final ManagementStage managementStage = new ManagementStage(managementGraph, i);
			stageMap.put(executionStage, managementStage);
		}

		return stageMap;
	}

	private static Map<ExecutionGroupVertex, ManagementGroupVertex> addGroupVertices(
			Map<ExecutionStage, ManagementStage> stageMap) {

		final Map<ExecutionGroupVertex, ManagementGroupVertex> groupMap = new HashMap<ExecutionGroupVertex, ManagementGroupVertex>();

		// First, create all vertices
		Iterator<Map.Entry<ExecutionStage, ManagementStage>> iterator = stageMap.entrySet().iterator();
		while (iterator.hasNext()) {

			final Map.Entry<ExecutionStage, ManagementStage> entry = iterator.next();
			final ExecutionStage executionStage = entry.getKey();
			final ManagementStage parent = entry.getValue();

			for (int i = 0; i < executionStage.getNumberOfStageMembers(); i++) {

				final ExecutionGroupVertex groupVertex = executionStage.getStageMember(i);
				final ManagementGroupVertex managementGroupVertex = new ManagementGroupVertex(parent, groupVertex
					.getName());

				groupMap.put(groupVertex, managementGroupVertex);
			}
		}

		// Second, make sure all edges are created and connected properly
		iterator = stageMap.entrySet().iterator();
		while (iterator.hasNext()) {

			final Map.Entry<ExecutionStage, ManagementStage> entry = iterator.next();
			final ExecutionStage executionStage = entry.getKey();
			for (int i = 0; i < executionStage.getNumberOfStageMembers(); i++) {

				final ExecutionGroupVertex sourceVertex = executionStage.getStageMember(i);
				final ManagementGroupVertex sourceGroupVertex = groupMap.get(sourceVertex);

				for (int j = 0; j < sourceVertex.getNumberOfForwardLinks(); j++) {

					final ExecutionGroupEdge edge = sourceVertex.getForwardEdge(j);
					final ExecutionGroupVertex targetVertex = edge.getTargetVertex();
					final ManagementGroupVertex targetGroupVertex = groupMap.get(targetVertex);
					new ManagementGroupEdge(sourceGroupVertex, j, targetGroupVertex, edge.getIndexOfInputGate(), edge
						.getChannelType());
				}
			}
		}

		return groupMap;
	}

	private static void addExecutionVertices(Map<ExecutionGroupVertex, ManagementGroupVertex> groupMap,
			ExecutionGraph executionGraph) {

		ExecutionGraphIterator iterator = new ExecutionGraphIterator(executionGraph, true);
		final Map<ExecutionVertex, ManagementVertex> vertexMap = new HashMap<ExecutionVertex, ManagementVertex>();
		final Map<ExecutionGate, ManagementGate> gateMap = new HashMap<ExecutionGate, ManagementGate>();

		while (iterator.hasNext()) {

			final ExecutionVertex ev = iterator.next();
			final ManagementGroupVertex parent = groupMap.get(ev.getGroupVertex());

			final Instance instance = ev.getAllocatedResource().getInstance();
			final ManagementVertex managementVertex = new ManagementVertex(
						parent, 
						ev.getID().toManagementVertexID(),
						(instance.getInstanceConnectionInfo() != null) ? instance.getInstanceConnectionInfo().toString() : instance.toString(),
						ev.getIndexInVertexGroup()
					);
			managementVertex.setExecutionState(ev.getExecutionState());
			vertexMap.put(ev, managementVertex);

			for (int i = 0; i < ev.getNumberOfOutputGates(); i++) {
				final ExecutionGate outputGate = ev.getOutputGate(i);
				final ManagementGate managementGate = new ManagementGate(managementVertex, new ManagementGateID(), i,
					false);
				gateMap.put(outputGate, managementGate);
			}

			for (int i = 0; i < ev.getNumberOfInputGates(); i++) {
				final ExecutionGate inputGate = ev.getInputGate(i);
				final ManagementGate managementGate = new ManagementGate(managementVertex, new ManagementGateID(), i,
					true);
				gateMap.put(inputGate, managementGate);
			}
		}

		iterator = new ExecutionGraphIterator(executionGraph, true);

		// Setup connections
		while (iterator.hasNext()) {

			final ExecutionVertex source = iterator.next();

			for (int i = 0; i < source.getNumberOfOutputGates(); i++) {

				final ExecutionGate outputGate = source.getOutputGate(i);
				final ManagementGate manangementOutputGate = gateMap.get(outputGate);
				final ChannelType channelType = outputGate.getChannelType();

				for (int j = 0; j < outputGate.getNumberOfEdges(); j++) {

					final ExecutionEdge outputChannel = outputGate.getEdge(j);

					final ManagementGate managementInputGate = gateMap.get(outputChannel.getInputGate());

					final ManagementEdgeID sourceEdgeID = new ManagementEdgeID(outputChannel.getOutputChannelID());
					final ManagementEdgeID targetEdgeID = new ManagementEdgeID(outputChannel.getInputChannelID());
					new ManagementEdge(sourceEdgeID, targetEdgeID, manangementOutputGate, j, managementInputGate,
						outputChannel.getInputGateIndex(), channelType);
				}
			}
		}
	}
}
