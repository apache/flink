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
import eu.stratosphere.nephele.managementgraph.ManagementVertexID;

/**
 * Through this interface it is possible to access profiling data about the CPU utilization
 * of the corresponding execution thread during its execution.
 * 
 * @author stanik
 */
public class ThreadProfilingEvent extends VertexProfilingEvent {

	private int userTime;

	private int systemTime;

	private int blockedTime;

	private int waitedTime;

	public ThreadProfilingEvent(int userTime, int systemTime, int blockedTime, int waitedTime,
			ManagementVertexID vertexID, int profilingInterval, JobID jobID, long timestamp, long profilingTimestamp) {
		super(vertexID, profilingInterval, jobID, timestamp, profilingTimestamp);

		this.userTime = userTime;
		this.systemTime = systemTime;
		this.blockedTime = blockedTime;
		this.waitedTime = waitedTime;
	}

	public ThreadProfilingEvent() {
		super();
	}

	/**
	 * Returns the percentage of time the execution thread spent in
	 * user mode in the given profiling interval.
	 * 
	 * @return the percentage of time spent in user mode
	 */
	public int getUserTime() {
		return this.userTime;
	}

	/**
	 * Returns the percentage of time the execution thread spent in
	 * system mode in the given profiling interval.
	 * 
	 * @return the percentage of time spent in system mode
	 */
	public int getSystemTime() {
		return this.systemTime;
	}

	/**
	 * Returns the percentage of time the execution thread has been
	 * blocked to enter or reenter a monitor in the given profiling interval.
	 * 
	 * @return the percentage of time the thread has been blocked
	 */
	public int getBlockedTime() {
		return this.blockedTime;
	}

	/**
	 * Returns the percentage of time the execution thread spent in
	 * either <code>WAITING</code> or <code>TIMED_WAITING</code> state in the given profiling interval.
	 * 
	 * @return the percentage of time the thread spent waiting
	 */
	public int getWaitedTime() {
		return this.waitedTime;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {
		super.read(in);

		this.userTime = in.readInt();
		this.systemTime = in.readInt();
		this.blockedTime = in.readInt();
		this.waitedTime = in.readInt();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);

		out.writeInt(this.userTime);
		out.writeInt(this.systemTime);
		out.writeInt(this.blockedTime);
		out.writeInt(this.waitedTime);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {

		if (super.equals(obj)) {
			return false;
		}

		if (!(obj instanceof ThreadProfilingEvent)) {
			return false;
		}

		final ThreadProfilingEvent threadProfilingEvent = (ThreadProfilingEvent) obj;

		if (this.userTime != threadProfilingEvent.getUserTime()) {
			return false;
		}

		if (this.systemTime != threadProfilingEvent.getSystemTime()) {
			return false;
		}

		if (this.blockedTime != threadProfilingEvent.getBlockedTime()) {
			return false;
		}

		if (this.waitedTime != threadProfilingEvent.getWaitedTime()) {
			return false;
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {

		return super.hashCode();
	}
}
