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

package eu.stratosphere.nephele.executiongraph;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import eu.stratosphere.nephele.io.AbstractGate;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.AbstractOutputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.managementgraph.ManagementEdge;
import eu.stratosphere.nephele.managementgraph.ManagementEdgeID;
import eu.stratosphere.nephele.managementgraph.ManagementGate;
import eu.stratosphere.nephele.managementgraph.ManagementGateID;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementGroupEdge;
import eu.stratosphere.nephele.managementgraph.ManagementGroupVertex;
import eu.stratosphere.nephele.managementgraph.ManagementStage;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;
import eu.stratosphere.nephele.types.Record;

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
						.getChannelType(), edge.getCompressionLevel());
				}
			}
		}

		return groupMap;
	}

	private static void addExecutionVertices(Map<ExecutionGroupVertex, ManagementGroupVertex> groupMap,
			ExecutionGraph executionGraph) {

		ExecutionGraphIterator iterator = new ExecutionGraphIterator(executionGraph, true);
		final Map<ExecutionVertex, ManagementVertex> vertexMap = new HashMap<ExecutionVertex, ManagementVertex>();
		final Map<AbstractGate<? extends Record>, ManagementGate> gateMap = new HashMap<AbstractGate<? extends Record>, ManagementGate>();

		while (iterator.hasNext()) {

			final ExecutionVertex ev = iterator.next();
			final ManagementGroupVertex parent = groupMap.get(ev.getGroupVertex());

			final ManagementVertex managementVertex = new ManagementVertex(parent, ev.getID().toManagementVertexID(),
				(ev.getAllocatedResource().getInstance().getInstanceConnectionInfo() != null) ? ev
					.getAllocatedResource().getInstance().getInstanceConnectionInfo().toString() : ev
					.getAllocatedResource().getInstance().toString(), ev.getAllocatedResource().getInstance().getType()
					.toString(), ev.getCheckpointState().toString(), ev.getEnvironment().getIndexInSubtaskGroup());
			managementVertex.setExecutionState(ev.getExecutionState());
			vertexMap.put(ev, managementVertex);

			for (int i = 0; i < ev.getEnvironment().getNumberOfOutputGates(); i++) {
				final OutputGate<? extends Record> outputGate = ev.getEnvironment().getOutputGate(i);
				final ManagementGate managementGate = new ManagementGate(managementVertex, 
					new ManagementGateID(), i, false, outputGate.getType().toString());
				gateMap.put(outputGate, managementGate);
			}

			for (int i = 0; i < ev.getEnvironment().getNumberOfInputGates(); i++) {
				final InputGate<? extends Record> inputGate = ev.getEnvironment().getInputGate(i);
				final ManagementGate managementGate = new ManagementGate(managementVertex,
					new ManagementGateID(), i, true, "");
				gateMap.put(inputGate, managementGate);
			}
		}

		iterator = new ExecutionGraphIterator(executionGraph, true);

		// Setup connections
		while (iterator.hasNext()) {

			final ExecutionVertex source = iterator.next();

			for (int i = 0; i < source.getEnvironment().getNumberOfOutputGates(); i++) {

				final OutputGate<? extends Record> outputGate = (OutputGate<? extends Record>) source.getEnvironment()
					.getOutputGate(i);
				final ManagementGate manangementOutputGate = gateMap.get(outputGate);

				for (int j = 0; j < outputGate.getNumberOfOutputChannels(); j++) {

					final AbstractOutputChannel<? extends Record> outputChannel = outputGate.getOutputChannel(j);

					final ChannelID inputChannelID = outputChannel.getConnectedChannelID();

					final AbstractInputChannel<? extends Record> inputChannel = executionGraph
						.getInputChannelByID(inputChannelID);
					final ManagementGate managementInputGate = gateMap.get(inputChannel.getInputGate());
					
					final ManagementEdgeID managementEdgeID = new ManagementEdgeID(
						manangementOutputGate.getManagementGateID(),
						managementInputGate.getManagementGateID());
					new ManagementEdge(managementEdgeID, manangementOutputGate, j, managementInputGate,
						inputChannel.getChannelIndex(),
						inputChannel.getType(), inputChannel.getCompressionLevel());
				}
			}
		}
	}
}
