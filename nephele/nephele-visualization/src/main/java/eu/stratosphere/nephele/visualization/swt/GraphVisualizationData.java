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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementGroupEdge;
import eu.stratosphere.nephele.managementgraph.ManagementGroupVertex;
import eu.stratosphere.nephele.topology.NetworkTopology;

public class GraphVisualizationData {

	private final JobID jobID;

	private final String jobName;

	private final ManagementGraph managementGraph;

	private final NetworkTopology networkTopology;

	private final boolean profilingEnabledForJob;

	public GraphVisualizationData(JobID jobID, String jobName, boolean profilingEnabledForJob,
			ManagementGraph managementGraph, NetworkTopology networkTopology) {

		this.jobID = jobID;
		this.jobName = jobName;
		this.profilingEnabledForJob = profilingEnabledForJob;
		this.managementGraph = managementGraph;
		this.networkTopology = networkTopology;
	}

	public String getJobName() {

		return this.jobName;
	}

	public JobID getJobID() {
		return this.jobID;
	}

	public ManagementGraph getManagementGraph() {
		return this.managementGraph;
	}

	public NetworkTopology getNetworkTopology() {
		return this.networkTopology;
	}

	public boolean isProfilingAvailableForJob() {
		return this.profilingEnabledForJob;
	}

	public void detectBottlenecks() {

		// Detect CPU bottlenecks
		final List<ManagementGroupVertex> reverseTopologicalSort = this.managementGraph
			.getGroupVerticesInReverseTopologicalOrder();
		final Map<ManagementGroupVertex, Boolean> successorCPUBottleneckMap = new HashMap<ManagementGroupVertex, Boolean>();
		Iterator<ManagementGroupVertex> it = reverseTopologicalSort.iterator();
		boolean atLeastOneCPUBottleneck = false;
		while (it.hasNext()) {
			final ManagementGroupVertex groupVertex = it.next();
			final GroupVertexVisualizationData groupVertexVisualizationData = (GroupVertexVisualizationData) groupVertex
				.getAttachment();
			groupVertexVisualizationData.updateCPUBottleneckFlag(successorCPUBottleneckMap);
			if (groupVertexVisualizationData.isCPUBottleneck()) {
				atLeastOneCPUBottleneck = true;
			}
		}

		// If there is a CPU bottleneck do not look for further I/O bottlenecks
		if (atLeastOneCPUBottleneck) {
			return;
		}

		// Detect IO bottlenecks
		it = reverseTopologicalSort.iterator();
		final Map<ManagementGroupEdge, Boolean> successorIOBottleneckMap = new HashMap<ManagementGroupEdge, Boolean>();
		while (it.hasNext()) {

			final ManagementGroupVertex groupVertex = it.next();
			for (int i = 0; i < groupVertex.getNumberOfForwardEdges(); i++) {
				final ManagementGroupEdge groupEdge = groupVertex.getForwardEdge(i);
				final GroupEdgeVisualizationData groupEdgeVisualizationData = (GroupEdgeVisualizationData) groupEdge
					.getAttachment();
				groupEdgeVisualizationData.updateIOBottleneckFlag(successorIOBottleneckMap);
			}

		}
	}
}
