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

package eu.stratosphere.nephele.profiling.types;

import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * Instance summary profiling events summarize the profiling events of all instances involved in computing a Nephele
 * job.
 * <p>
 * This class is not thread-safe.
 * 
 * @author warneke
 */
public final class InstanceSummaryProfilingEvent extends InstanceProfilingEvent {

	/**
	 * Constructs a new instance summary profiling event.
	 * 
	 * @param profilingInterval
	 *        the interval of time this profiling event covers in milliseconds
	 * @param ioWaitCPU
	 *        the percentage of time the CPU(s) spent in state IOWAIT during the profiling interval
	 * @param idleCPU
	 *        the percentage of time the CPU(s) spent in state IDLE during the profiling interval
	 * @param userCPU
	 *        the percentage of time the CPU(s) spent in state USER during the profiling interval
	 * @param systemCPU
	 *        the percentage of time the CPU(s) spent in state SYSTEM during the profiling interval
	 * @param hardIrqCPU
	 *        the percentage of time the CPU(s) spent in state HARD_IRQ during the profiling interval
	 * @param softIrqCPU
	 *        the percentage of time the CPU(s) spent in state SOFT_IRQ during the profiling interval
	 * @param totalMemory
	 *        the total amount of this instance's main memory in bytes
	 * @param freeMemory
	 *        the free amount of this instance's main memory in bytes
	 * @param bufferedMemory
	 *        the amount of main memory the instance uses for file buffers
	 * @param cachedMemory
	 *        the amount of main memory the instance uses as cache memory
	 * @param cachedSwapMemory
	 *        The amount of main memory the instance uses for cached swaps
	 * @param receivedBytes
	 *        the number of bytes received via network during the profiling interval
	 * @param transmittedBytes
	 *        the number of bytes transmitted via network during the profiling interval
	 * @param jobID
	 *        the ID of this job this profiling event belongs to
	 * @param timestamp
	 *        the time stamp of this profiling event's creation
	 * @param profilingTimestamp
	 *        the time stamp relative to the beginning of the job's execution
	 */
	public InstanceSummaryProfilingEvent(final int profilingInterval, final int ioWaitCPU, final int idleCPU,
			final int userCPU, final int systemCPU, final int hardIrqCPU, final int softIrqCPU, final long totalMemory,
			final long freeMemory, final long bufferedMemory, final long cachedMemory, final long cachedSwapMemory,
			final long receivedBytes, final long transmittedBytes, final JobID jobID,
			final long timestamp, final long profilingTimestamp) {
		super(profilingInterval, ioWaitCPU, idleCPU, userCPU, systemCPU, hardIrqCPU, softIrqCPU, totalMemory,
			freeMemory, bufferedMemory, cachedMemory, cachedSwapMemory, receivedBytes, transmittedBytes, jobID,
			timestamp, profilingTimestamp);
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public InstanceSummaryProfilingEvent() {
		super();
	}


	@Override
	public boolean equals(final Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof InstanceSummaryProfilingEvent)) {
			return false;
		}

		return true;
	}


	@Override
	public int hashCode() {

		return super.hashCode();
	}
}
