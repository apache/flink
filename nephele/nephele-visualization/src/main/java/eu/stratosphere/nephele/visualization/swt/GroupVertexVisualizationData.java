/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import eu.stratosphere.nephele.managementgraph.ManagementGroupVertex;

public class GroupVertexVisualizationData {

	private final static double CPU_THRESHOLD = 90.0f;

	final ManagementGroupVertex managementGroupVertex;

	private boolean cpuBottleneck = false;

	public GroupVertexVisualizationData(ManagementGroupVertex managementGroupVertex) {

		this.managementGroupVertex = managementGroupVertex;
	}

	public boolean isAnySuccessorCPUBottleneck(Map<ManagementGroupVertex, Boolean> successorCPUBottleneckMap) {

		Boolean anySuccessorIsCPUBottleneck = successorCPUBottleneckMap.get(this.managementGroupVertex);
		if (anySuccessorIsCPUBottleneck == null) {
			// The value is not yet calculated
			for (int i = 0; i < this.managementGroupVertex.getNumberOfForwardEdges(); i++) {
				final ManagementGroupVertex targetVertex = this.managementGroupVertex.getForwardEdge(i).getTarget();
				final GroupVertexVisualizationData groupVertexVisualizationData = (GroupVertexVisualizationData) targetVertex
					.getAttachment();
				groupVertexVisualizationData.updateCPUBottleneckFlag(successorCPUBottleneckMap);
				if (groupVertexVisualizationData.isCPUBottleneck()
					|| groupVertexVisualizationData.isAnySuccessorCPUBottleneck(successorCPUBottleneckMap)) {
					successorCPUBottleneckMap.put(this.managementGroupVertex, Boolean.valueOf(true));
					return true;
				}
			}

			successorCPUBottleneckMap.put(this.managementGroupVertex, Boolean.valueOf(false));
			return false;
		}

		return anySuccessorIsCPUBottleneck.booleanValue();
	}

	public void updateCPUBottleneckFlag(Map<ManagementGroupVertex, Boolean> successorCPUBottleneckMap) {

		if (isAnySuccessorCPUBottleneck(successorCPUBottleneckMap)) {
			this.cpuBottleneck = false;
			return;
		}

		if (getAverageUsrTime() < CPU_THRESHOLD) {
			this.cpuBottleneck = false;
			return;
		}

		this.cpuBottleneck = true;
	}

	public double getAverageUsrTime() {

		double usrTime = 0.0f;

		for (int i = 0; i < this.managementGroupVertex.getNumberOfGroupMembers(); i++) {
			final VertexVisualizationData vertexVisualizationData = (VertexVisualizationData) this.managementGroupVertex
				.getGroupMember(i).getAttachment();
			usrTime += vertexVisualizationData.getAverageUserTime();
		}

		usrTime /= (double) this.managementGroupVertex.getNumberOfGroupMembers();

		return usrTime;
	}

	public boolean isCPUBottleneck() {
		return this.cpuBottleneck;
	}
}
