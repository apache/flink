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

package org.apache.flink.runtime.profiling.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.instance.AllocatedSlot;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.profiling.impl.types.InternalInstanceProfilingData;
import org.apache.flink.runtime.profiling.types.InstanceSummaryProfilingEvent;


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

	public boolean addIfInstanceIsAllocatedByJob(InternalInstanceProfilingData instanceProfilingData) {

		for (ExecutionVertex executionVertex : this.executionGraph.getAllExecutionVertices()) {
			AllocatedSlot slot = executionVertex.getCurrentAssignedResource();
			if (slot != null && slot.getInstance().getInstanceConnectionInfo().equals(
					instanceProfilingData.getInstanceConnectionInfo()))
			{
				this.collectedInstanceProfilingData.put(instanceProfilingData.getInstanceConnectionInfo(), instanceProfilingData);
				return true;
			}
		}

		return false;
	}

	public InstanceSummaryProfilingEvent getInstanceSummaryProfilingData(long timestamp) {

		final Set<Instance> tempSet = new HashSet<Instance>();
		
		for (ExecutionVertex executionVertex : this.executionGraph.getAllExecutionVertices()) {
			AllocatedSlot slot = executionVertex.getCurrentAssignedResource();
			if (slot != null) {
				tempSet.add(slot.getInstance());
			}
		}

		// Now compare the size of the collected data set and the allocated instance set.
		// If their sizes are equal we can issue an instance summary.
		if (tempSet.size() != this.collectedInstanceProfilingData.size()) {
			return null;
		}

		return constructInstanceSummary(timestamp);
	}

	private InstanceSummaryProfilingEvent constructInstanceSummary(long timestamp) {

		final int numberOfInstances = this.collectedInstanceProfilingData.size();
		
		final Iterator<InstanceConnectionInfo> instanceIterator = this.collectedInstanceProfilingData.keySet().iterator();

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

			final InternalInstanceProfilingData profilingData = this.collectedInstanceProfilingData.get(instanceIterator.next());

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

		final InstanceSummaryProfilingEvent instanceSummary = new InstanceSummaryProfilingEvent(
				profilingIntervalSum / numberOfInstances,
				ioWaitCPUSum / numberOfInstances,
				idleCPUSum / numberOfInstances,
				userCPUSum / numberOfInstances,
				systemCPUSum / numberOfInstances,
				hardIrqCPUSum / numberOfInstances,
				softIrqCPUSum / numberOfInstances,
				totalMemorySum / numberOfInstances,
				freeMemorySum / numberOfInstances,
				bufferedMemorySum / numberOfInstances,
				cachedMemorySum / numberOfInstances,
				cachedSwapMemorySum / numberOfInstances,
				receivedBytesSum / numberOfInstances,
				transmittedBytesSum / numberOfInstances,
				this.executionGraph.getJobID(), timestamp, (timestamp - this.profilingStart));

		this.collectedInstanceProfilingData.clear();

		return instanceSummary;
	}
}
