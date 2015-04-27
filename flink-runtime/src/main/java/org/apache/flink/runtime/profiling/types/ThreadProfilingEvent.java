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

package org.apache.flink.runtime.profiling.types;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * Through this interface it is possible to access profiling data about the CPU utilization
 * of the corresponding execution thread during its execution.
 */
public class ThreadProfilingEvent extends VertexProfilingEvent {

	private static final long serialVersionUID = -3006867830244444710L;

	private int userTime;

	private int systemTime;

	private int blockedTime;

	private int waitedTime;

	public ThreadProfilingEvent(int userTime, int systemTime, int blockedTime, int waitedTime,
			JobVertexID vertexId, int subtask, ExecutionAttemptID executionId,
			int profilingInterval, JobID jobID, long timestamp, long profilingTimestamp)
	{
		super(vertexId, subtask, executionId, profilingInterval, jobID, timestamp, profilingTimestamp);

		this.userTime = userTime;
		this.systemTime = systemTime;
		this.blockedTime = blockedTime;
		this.waitedTime = waitedTime;
	}

	public ThreadProfilingEvent() {
		super();
	}
	
	// --------------------------------------------------------------------------------------------

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

	// --------------------------------------------------------------------------------------------
	//  Serialization
	// --------------------------------------------------------------------------------------------

	@Override
	public void read(DataInputView in) throws IOException {
		super.read(in);

		this.userTime = in.readInt();
		this.systemTime = in.readInt();
		this.blockedTime = in.readInt();
		this.waitedTime = in.readInt();
	}


	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		out.writeInt(this.userTime);
		out.writeInt(this.systemTime);
		out.writeInt(this.blockedTime);
		out.writeInt(this.waitedTime);
	}


	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ThreadProfilingEvent) {
			final ThreadProfilingEvent other = (ThreadProfilingEvent) obj;
			
			return this.userTime == other.userTime &&
					this.systemTime == other.systemTime &&
					this.blockedTime == other.blockedTime &&
					this.waitedTime == other.waitedTime && 
					super.equals(obj);
		}
		else {
			return false;
		}
	}
	
	// hash code is inherited from the superclass
}
