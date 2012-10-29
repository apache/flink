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

package eu.stratosphere.nephele.rpc;

import java.util.List;

import eu.stratosphere.nephele.event.job.ExecutionStateChangeEvent;
import eu.stratosphere.nephele.event.job.RecentJobEvent;
import eu.stratosphere.nephele.event.job.VertexAssignmentEvent;
import eu.stratosphere.nephele.instance.HardwareDescription;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.instance.InstanceTypeDescription;
import eu.stratosphere.nephele.managementgraph.ManagementEdgeID;
import eu.stratosphere.nephele.managementgraph.ManagementGraph;
import eu.stratosphere.nephele.managementgraph.ManagementGroupVertexID;
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;
import eu.stratosphere.nephele.profiling.types.InstanceSummaryProfilingEvent;
import eu.stratosphere.nephele.profiling.types.SingleInstanceProfilingEvent;
import eu.stratosphere.nephele.profiling.types.ThreadProfilingEvent;
import eu.stratosphere.nephele.topology.NetworkNode;
import eu.stratosphere.nephele.topology.NetworkTopology;

/**
 * This utility class provides a list of types frequently used by the RPC protocols included in this package.
 * 
 * @author warneke
 */
public class ManagementTypeUtils {

	/**
	 * Private constructor to prevent instantiation.
	 */
	private ManagementTypeUtils() {
	}

	/**
	 * Returns a list of types frequently used by the RPC protocols of this package and its parent packages.
	 * 
	 * @return a list of types frequently used by the RPC protocols of this package
	 */
	public static List<Class<?>> getRPCTypesToRegister() {

		final List<Class<?>> types = CommonTypeUtils.getRPCTypesToRegister();

		types.add(ExecutionStateChangeEvent.class);
		types.add(HardwareDescription.class);
		types.add(InstanceSummaryProfilingEvent.class);
		types.add(InstanceType.class);
		types.add(InstanceTypeDescription.class);
		types.add(ManagementEdgeID.class);
		types.add(ManagementGraph.class);
		types.add(ManagementGroupVertexID.class);
		types.add(ManagementVertexID.class);
		types.add(NetworkNode.class);
		types.add(NetworkTopology.class);
		types.add(RecentJobEvent.class);
		types.add(SingleInstanceProfilingEvent.class);
		types.add(ThreadProfilingEvent.class);
		types.add(VertexAssignmentEvent.class);

		return types;
	}
}
