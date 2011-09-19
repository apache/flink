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

package eu.stratosphere.nephele.profiling.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import eu.stratosphere.nephele.executiongraph.ExecutionGraph;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionGroupVertexIterator;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.AllocatedResource;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.profiling.impl.types.InternalInstanceProfilingData;
import eu.stratosphere.nephele.profiling.types.InstanceSummaryProfilingEvent;

public class JobProfilingData {

	private final ExecutionGraph executionGraph;

	private final long profilingStart;

	private final Map<InstanceConnectionInfo, InternalInstanceProfilingData> collectedInstanceProfilingData = new HashMap<InstanceConnectionInfo, InternalInstanceProfilingData>();

	public JobProfilingData(ExecutionGraph executionGraph) {
		this.executionGraph = executionGraph;

		this.profilingStart = System.currentTimeMillis();
	}

	public long getProfilingStart() {
		return this.profilingStart;
	}

	public ExecutionGraph getExecutionGraph() {

		return this.executionGraph;
	}

	public boolean instanceAllocatedByJob(InternalInstanceProfilingData instanceProfilingData) {

		final ExecutionGroupVertexIterator it = new ExecutionGroupVertexIterator(this.executionGraph, true,
			this.executionGraph.getIndexOfCurrentExecutionStage());
		while (it.hasNext()) {

			final ExecutionGroupVertex groupVertex = it.next();
			for (int i = 0; i < groupVertex.getCurrentNumberOfGroupMembers(); i++) {
				final ExecutionVertex executionVertex = groupVertex.getGroupMember(i);
				if (instanceProfilingData.getInstanceConnectionInfo().equals(
					executionVertex.getAllocatedResource().getInstance().getInstanceConnectionInfo())) {
					this.collectedInstanceProfilingData.put(instanceProfilingData.getInstanceConnectionInfo(),
						instanceProfilingData);
					return true;
				}
			}
		}

		return false;
	}

	public InstanceSummaryProfilingEvent getInstanceSummaryProfilingData(long timestamp) {

		final Set<AbstractInstance> tempSet = new HashSet<AbstractInstance>();
		// First determine the number of allocated instances in the current stage
		final ExecutionGroupVertexIterator it = new ExecutionGroupVertexIterator(this.executionGraph, true,
			this.executionGraph.getIndexOfCurrentExecutionStage());
		while (it.hasNext()) {

			final ExecutionGroupVertex groupVertex = it.next();
			for (int i = 0; i < groupVertex.getCurrentNumberOfGroupMembers(); i++) {
				final ExecutionVertex executionVertex = groupVertex.getGroupMember(i);
				AllocatedResource allocatedResource = executionVertex.getAllocatedResource();
				tempSet.add(allocatedResource.getInstance());
			}
		}

		/*
		 * Now compare the size of the collected data set and the allocated instance set.
		 * If their sizes are equal we can issue an instance summary.
		 */
		if (tempSet.size() != this.collectedInstanceProfilingData.size()) {
			return null;
		}

		return constructInstanceSummary(timestamp);
	}

	private InstanceSummaryProfilingEvent constructInstanceSummary(long timestamp) {

		final int numberOfInstances = this.collectedInstanceProfilingData.size();
		final Iterator<InstanceConnectionInfo> instanceIterator = this.collectedInstanceProfilingData.keySet()
			.iterator();

		long freeMemorySum = 0;
		long totalMemorySum = 0;
		long bufferedMemorySum = 0;
		long cachedMemorySum = 0;
		long cachedSwapMemorySum = 0;

		int ioWaitCPUSum = 0;
		int idleCPUSum = 0;
		int profilingIntervalSum = 0;
		int systemCPUSum = 0;
		int hardIrqCPUSum = 0;
		int softIrqCPUSum = 0;

		int userCPUSum = 0;
		long receivedBytesSum = 0;
		long transmittedBytesSum = 0;

		// Sum up the individual values
		while (instanceIterator.hasNext()) {

			final InternalInstanceProfilingData profilingData = this.collectedInstanceProfilingData
				.get(instanceIterator.next());

			freeMemorySum += profilingData.getFreeMemory();
			ioWaitCPUSum += profilingData.getIOWaitCPU();
			idleCPUSum += profilingData.getIdleCPU();
			profilingIntervalSum += profilingData.getProfilingInterval();
			systemCPUSum += profilingData.getSystemCPU();
			hardIrqCPUSum += profilingData.getHardIrqCPU();
			softIrqCPUSum += profilingData.getSoftIrqCPU();
			totalMemorySum += profilingData.getTotalMemory();
			userCPUSum += profilingData.getUserCPU();
			receivedBytesSum += profilingData.getReceivedBytes();
			transmittedBytesSum += profilingData.getTransmittedBytes();
			bufferedMemorySum += profilingData.getBufferedMemory();
			cachedMemorySum += profilingData.getCachedMemory();
			cachedSwapMemorySum += profilingData.getCachedSwapMemory();
		}

		final InstanceSummaryProfilingEvent instanceSummary = new InstanceSummaryProfilingEvent(profilingIntervalSum
			/ numberOfInstances, ioWaitCPUSum / numberOfInstances, idleCPUSum / numberOfInstances, userCPUSum
			/ numberOfInstances, systemCPUSum / numberOfInstances, hardIrqCPUSum / numberOfInstances, softIrqCPUSum
			/ numberOfInstances, totalMemorySum / (long) numberOfInstances, freeMemorySum / (long) numberOfInstances,
			bufferedMemorySum / (long) numberOfInstances, cachedMemorySum / (long) numberOfInstances,
			cachedSwapMemorySum / (long) numberOfInstances, receivedBytesSum / (long) numberOfInstances,
			transmittedBytesSum / (long) numberOfInstances, this.executionGraph.getJobID(), timestamp,
			(timestamp - this.profilingStart));

		this.collectedInstanceProfilingData.clear();

		return instanceSummary;
	}
}
