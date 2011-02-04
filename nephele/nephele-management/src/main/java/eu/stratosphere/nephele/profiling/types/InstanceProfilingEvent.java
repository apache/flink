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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.jobgraph.JobID;

/**
 * Instance profiling events are a special subclass of profiling events. They contain profiling information about the
 * utilization of a particular instance during a job execution.
 * <p>
 * This class is not thread-safe.
 * 
 * @author stanik
 */
public abstract class InstanceProfilingEvent extends ProfilingEvent {

	private int profilingInterval;

	private int ioWaitCPU;

	private int idleCPU;

	private int userCPU;

	private int systemCPU;

	private int hardIrqCPU;

	private int softIrqCPU;

	private long totalMemory;

	private long freeMemory;

	private long bufferedMemory;

	private long cachedMemory;

	private long cachedSwapMemory;

	private long receivedBytes;

	private long transmittedBytes;

	public InstanceProfilingEvent(int profilingInterval, int ioWaitCPU, int idleCPU, int userCPU, int systemCPU,
			int hardIrqCPU, int softIrqCPU, long totalMemory, long freeMemory, long bufferedMemory, long cachedMemory,
			long cachedSwapMemory, long receivedBytes, long transmittedBytes, JobID jobID, long timestamp,
			long profilingTimestamp) {

		super(jobID, timestamp, profilingTimestamp);

		this.profilingInterval = profilingInterval;

		this.ioWaitCPU = ioWaitCPU;
		this.idleCPU = idleCPU;
		this.userCPU = userCPU;
		this.systemCPU = systemCPU;
		this.hardIrqCPU = hardIrqCPU;
		this.softIrqCPU = softIrqCPU;

		this.totalMemory = totalMemory;
		this.freeMemory = freeMemory;
		this.bufferedMemory = bufferedMemory;
		this.cachedMemory = cachedMemory;
		this.cachedSwapMemory = cachedSwapMemory;

		this.receivedBytes = receivedBytes;
		this.transmittedBytes = transmittedBytes;
	}

	public InstanceProfilingEvent() {
		super();
	}

	/**
	 * The interval in milliseconds to which the rest
	 * of the profiling data relates to.
	 * 
	 * @return the profiling interval given in milliseconds
	 */
	public int getProfilingInterval() {
		return this.profilingInterval;
	}

	/**
	 * Returns the total amount of memory of the corresponding instance.
	 * 
	 * @return the total amount of memory in KB
	 */
	public long getTotalMemory() {
		return this.totalMemory;
	}

	/**
	 * Returns the amount of free memory of the corresponding instance.
	 * 
	 * @return the amount of free memory in KB
	 */
	public long getFreeMemory() {
		return this.freeMemory;
	}

	/**
	 * Returns the amount of memory, in KB, used for file buffers.
	 * 
	 * @return the amount of memory used for file buffers in KB
	 */
	public long getBufferedMemory() {
		return this.bufferedMemory;
	}

	/**
	 * Returns the amount of memory, in KB, used as cache memory.
	 * 
	 * @return the amount of memory used as cache memory in KB
	 */
	public long getCachedMemory() {
		return this.cachedMemory;
	}

	/**
	 * Returns the amount of swap, in KB, used as cache memory.
	 * 
	 * @return the amount of, in KB, used as cache memory
	 */
	public long getCachedSwapMemory() {
		return this.cachedSwapMemory;
	}

	// TODO: Adapt this method
	/**
	 * Returns the users CPU utilization in percent in the last period.
	 * Time spent running non-kernel code. (user time, including nice time)
	 */
	public int getUserCPU() {
		return this.userCPU;
	}

	// TODO: Adapt this method
	/**
	 * Returns the system CPU utilization in percent in the last period.
	 * Time spent running kernel code. (system time)
	 */
	public int getSystemCPU() {
		return this.systemCPU;
	}

	// TODO: Adapt this method
	/**
	 * Returns the idle CPU utilization in percent in the last period.
	 * Time spent idle. Prior to Linux 2.5.41, this includes IO-wait time.
	 */
	public int getIdleCPU() {
		return this.idleCPU;
	}

	// TODO: Adapt this method
	/**
	 * Returns the IO wait CPU utilization in percent in the last period.
	 * Time spent waiting for IO. Prior to Linux 2.5.41, included in idle.
	 */
	public int getIOWaitCPU() {
		return this.ioWaitCPU;
	}

	/**
	 * Returns the percentage of time the CPU spent servicing hard interrupts in the last period.
	 * 
	 * @return the percentage of time the CPU spent servicing hard interrupts
	 */
	public int getHardIrqCPU() {
		return this.hardIrqCPU;
	}

	/**
	 * Returns the percentage of time the CPU spent servicing soft interrupts in the last period.
	 * 
	 * @return the percentage of time the CPU spent servicing soft interrupts
	 */
	public int getSoftIrqCPU() {
		return this.softIrqCPU;
	}

	/**
	 * Returns the number of bytes received on all interfaces in the
	 * last period.
	 * 
	 * @return the number of bytes received on all interfaces
	 */
	public long getReceivedBytes() {
		return this.receivedBytes;
	}

	/**
	 * Returns the number of bytes transmitted via all interfaces in the
	 * last period.
	 * 
	 * @return the number of bytes transmitted via all interfaces
	 */
	public long getTransmittedBytes() {
		return this.transmittedBytes;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);

		this.profilingInterval = in.readInt();

		this.ioWaitCPU = in.readInt();
		this.idleCPU = in.readInt();
		this.userCPU = in.readInt();
		this.systemCPU = in.readInt();
		this.hardIrqCPU = in.readInt();
		this.softIrqCPU = in.readInt();

		this.totalMemory = in.readLong();
		this.freeMemory = in.readLong();
		this.bufferedMemory = in.readLong();
		this.cachedMemory = in.readLong();
		this.cachedSwapMemory = in.readLong();

		this.receivedBytes = in.readLong();
		this.transmittedBytes = in.readLong();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		out.writeInt(this.profilingInterval);

		out.writeInt(this.ioWaitCPU);
		out.writeInt(this.idleCPU);
		out.writeInt(this.userCPU);
		out.writeInt(this.systemCPU);
		out.writeInt(this.hardIrqCPU);
		out.writeInt(this.softIrqCPU);

		out.writeLong(totalMemory);
		out.writeLong(freeMemory);
		out.writeLong(bufferedMemory);
		out.writeLong(cachedMemory);
		out.writeLong(cachedSwapMemory);

		out.writeLong(receivedBytes);
		out.writeLong(transmittedBytes);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof InstanceProfilingEvent)) {
			return false;
		}

		final InstanceProfilingEvent instanceProfilingEvent = (InstanceProfilingEvent) obj;

		if (this.profilingInterval != instanceProfilingEvent.getProfilingInterval()) {
			return false;
		}

		if (this.ioWaitCPU != instanceProfilingEvent.getIOWaitCPU()) {
			return false;
		}
		if (this.idleCPU != instanceProfilingEvent.getIdleCPU()) {
			return false;
		}

		if (this.userCPU != instanceProfilingEvent.getUserCPU()) {
			return false;
		}

		if (this.systemCPU != instanceProfilingEvent.getSystemCPU()) {
			return false;
		}

		if (this.hardIrqCPU != instanceProfilingEvent.getHardIrqCPU()) {
			return false;
		}

		if (this.softIrqCPU != instanceProfilingEvent.getSoftIrqCPU()) {
			return false;
		}

		if (this.totalMemory != instanceProfilingEvent.getTotalMemory()) {
			return false;
		}

		if (this.freeMemory != instanceProfilingEvent.getFreeMemory()) {
			return false;
		}

		if (this.bufferedMemory != instanceProfilingEvent.getBufferedMemory()) {
			return false;
		}

		if (this.cachedMemory != instanceProfilingEvent.getCachedMemory()) {
			return false;
		}

		if (this.cachedSwapMemory != instanceProfilingEvent.getCachedSwapMemory()) {
			return false;
		}

		if (this.receivedBytes != instanceProfilingEvent.getReceivedBytes()) {
			return false;
		}

		if (this.transmittedBytes != instanceProfilingEvent.getTransmittedBytes()) {
			return false;
		}

		return true;
	}
}
