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

package eu.stratosphere.nephele.visualization.swt;

import java.util.Map;

import eu.stratosphere.nephele.managementgraph.ManagementGate;
import eu.stratosphere.nephele.managementgraph.ManagementGroupEdge;
import eu.stratosphere.nephele.managementgraph.ManagementGroupVertex;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;

public class GroupEdgeVisualizationData {

	private final static double IO_THRESHOLD = 50.0f;

	private final ManagementGroupEdge managementGroupEdge;

	public GroupEdgeVisualizationData(ManagementGroupEdge managementGroupEdge) {
		this.managementGroupEdge = managementGroupEdge;
	}

	private boolean ioBottleneck = false;;

	public boolean isAnySuccessorIOBottleneck(Map<ManagementGroupEdge, Boolean> successorIOBottleneckMap) {

		Boolean anySuccessorIsIOBottleneck = successorIOBottleneckMap.get(this.managementGroupEdge);
		if (anySuccessorIsIOBottleneck == null) {
			// The value is not yet calculated
			final ManagementGroupVertex groupVertex = this.managementGroupEdge.getTarget();
			for (int i = 0; i < groupVertex.getNumberOfForwardEdges(); i++) {
				final ManagementGroupEdge successorEdge = groupVertex.getForwardEdge(i);
				final GroupEdgeVisualizationData groupEdgeVisualizationData = (GroupEdgeVisualizationData) successorEdge
					.getAttachment();
				groupEdgeVisualizationData.updateIOBottleneckFlag(successorIOBottleneckMap);
				if (groupEdgeVisualizationData.isIOBottleneck()
					|| groupEdgeVisualizationData.isAnySuccessorIOBottleneck(successorIOBottleneckMap)) {
					successorIOBottleneckMap.put(this.managementGroupEdge, Boolean.valueOf(true));
					return true;
				}
			}

			successorIOBottleneckMap.put(this.managementGroupEdge, Boolean.valueOf(false));
			return false;
		}

		return anySuccessorIsIOBottleneck.booleanValue();
	}

	public void updateIOBottleneckFlag(Map<ManagementGroupEdge, Boolean> successorIOBottleneckMap) {

		if (isAnySuccessorIOBottleneck(successorIOBottleneckMap)) {
			this.ioBottleneck = false;
			return;
		}

		if (getAverageNumberOfCapacityExhaustions() < IO_THRESHOLD) {
			this.ioBottleneck = false;
			return;
		}

		this.ioBottleneck = true;
	}

	public double getAverageNumberOfCapacityExhaustions() {

		double numberOfCapacityExhaustions = 0.0f;

		final ManagementGroupVertex sourceVertex = this.managementGroupEdge.getSource();
		for (int i = 0; i < sourceVertex.getNumberOfGroupMembers(); i++) {
			final ManagementVertex vertex = sourceVertex.getGroupMember(i);
			final ManagementGate gate = vertex.getOutputGate(this.managementGroupEdge.getSourceIndex());
			final GateVisualizationData gateVisulizationData = (GateVisualizationData) gate.getAttachment();
			numberOfCapacityExhaustions += gateVisulizationData.getAverageChartData();
		}

		numberOfCapacityExhaustions /= (double) sourceVertex.getNumberOfGroupMembers();

		return numberOfCapacityExhaustions;
	}

	public boolean isIOBottleneck() {
		return this.ioBottleneck;
	}
}
