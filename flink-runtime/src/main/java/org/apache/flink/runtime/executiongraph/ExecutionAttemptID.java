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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/**
 * Unique identifier for the attempt to execute a tasks. Multiple attempts happen
 * in cases of failures and recovery.
 */
public class ExecutionAttemptID implements java.io.Serializable {

	private static final long serialVersionUID = -1169683445778281344L;

	private final JobID jobId;
	private final ExecutionVertexID executionVertexId;
	private final int attemptNumber;

	/**
	 * Get a random execution attempt id.
	 */
	public ExecutionAttemptID() {
		this(new JobID(), new ExecutionVertexID(new JobVertexID(), 0), 0);
	}

	public ExecutionAttemptID(JobID jobId, ExecutionVertexID executionVertexId, int attemptNumber) {
		Preconditions.checkState(attemptNumber >= 0);
		this.jobId = Preconditions.checkNotNull(jobId);
		this.executionVertexId = Preconditions.checkNotNull(executionVertexId);
		this.attemptNumber = attemptNumber;
	}

	public void writeTo(ByteBuf buf) {
		writeJobIdTo(buf);
		executionVertexId.writeTo(buf);
		buf.writeInt(this.attemptNumber);
	}

	public static ExecutionAttemptID fromByteBuf(ByteBuf buf) {
		final JobID jobId = jobIdFromByteBuf(buf);
		final ExecutionVertexID executionVertexId = ExecutionVertexID.fromByteBuf(buf);
		final int attemptNumber = buf.readInt();
		return new ExecutionAttemptID(jobId, executionVertexId, attemptNumber);
	}

	private static JobID jobIdFromByteBuf(ByteBuf buf) {
		final long lower = buf.readLong();
		final long upper = buf.readLong();
		return new JobID(lower, upper);
	}

	private void writeJobIdTo(ByteBuf buf) {
		buf.writeLong(jobId.getLowerPart());
		buf.writeLong(jobId.getUpperPart());
	}

	@VisibleForTesting
	public int getAttemptNumber() {
		return attemptNumber;
	}

	@VisibleForTesting
	public ExecutionVertexID getExecutionVertexId() {
		return executionVertexId;
	}

	@VisibleForTesting
	public JobID getJobId() {
		return jobId;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj != null && obj.getClass() == getClass()) {
			ExecutionAttemptID that = (ExecutionAttemptID) obj;
			return that.jobId.equals(this.jobId)
				&& that.executionVertexId.equals(this.executionVertexId)
				&& that.attemptNumber == this.attemptNumber;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(jobId, executionVertexId, attemptNumber);
	}

	@Override
	public String toString() {
		return jobId.toString() + "_" + executionVertexId.toString() + "_" + attemptNumber;
	}
}
