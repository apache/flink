/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.profiling.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * A single instance profiling event encapsulates profiling information for one particular instance.
 * 
 */
public final class SingleInstanceProfilingEvent extends InstanceProfilingEvent {

	private String instanceName;

	/**
	 * Constructs a new instance profiling event.
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
	 * @param instanceName
	 *        the name of the instance this profiling event refers to
	 */
	public SingleInstanceProfilingEvent(final int profilingInterval, final int ioWaitCPU, final int idleCPU,
			final int userCPU, final int systemCPU, final int hardIrqCPU, final int softIrqCPU, final long totalMemory,
			final long freeMemory, final long bufferedMemory, final long cachedMemory, final long cachedSwapMemory,
			final long receivedBytes, final long transmittedBytes, final JobID jobID, final long timestamp,
			final long profilingTimestamp, final String instanceName) {
		super(profilingInterval, ioWaitCPU, idleCPU, userCPU, systemCPU, hardIrqCPU, softIrqCPU, totalMemory,
			freeMemory, bufferedMemory, cachedMemory, cachedSwapMemory, receivedBytes, transmittedBytes, jobID,
			timestamp, profilingTimestamp);

		this.instanceName = instanceName;
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public SingleInstanceProfilingEvent() {
		super();
	}

	/**
	 * Returns the name of the instance.
	 * 
	 * @return the name of the instance
	 */
	public String getInstanceName() {
		return this.instanceName;
	}


	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);

		this.instanceName = StringRecord.readString(in);
	}


	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		StringRecord.writeString(out, this.instanceName);
	}


	@Override
	public boolean equals(Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof SingleInstanceProfilingEvent)) {
			return false;
		}

		final SingleInstanceProfilingEvent singleInstanceProfilingEvent = (SingleInstanceProfilingEvent) obj;

		if (!this.instanceName.equals(singleInstanceProfilingEvent.getInstanceName())) {
			return false;
		}

		return true;
	}


	@Override
	public int hashCode() {

		return super.hashCode();
	}
}
