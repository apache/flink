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

package org.apache.flink.runtime.rest.handler.legacy.utils;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Utility class for constructing an ArchivedExecution.
 */
public class ArchivedExecutionBuilder {
	private ExecutionAttemptID attemptId;
	private long[] stateTimestamps;
	private int attemptNumber;
	private ExecutionState state;
	private String failureCause;
	private TaskManagerLocation assignedResourceLocation;
	private AllocationID assignedAllocationID;
	private StringifiedAccumulatorResult[] userAccumulators;
	private IOMetrics ioMetrics;
	private int parallelSubtaskIndex;

	public ArchivedExecutionBuilder setAttemptId(ExecutionAttemptID attemptId) {
		this.attemptId = attemptId;
		return this;
	}

	public ArchivedExecutionBuilder setStateTimestamps(long[] stateTimestamps) {
		Preconditions.checkArgument(stateTimestamps.length == ExecutionState.values().length);
		this.stateTimestamps = stateTimestamps;
		return this;
	}

	public ArchivedExecutionBuilder setAttemptNumber(int attemptNumber) {
		this.attemptNumber = attemptNumber;
		return this;
	}

	public ArchivedExecutionBuilder setState(ExecutionState state) {
		this.state = state;
		return this;
	}

	public ArchivedExecutionBuilder setFailureCause(String failureCause) {
		this.failureCause = failureCause;
		return this;
	}

	public ArchivedExecutionBuilder setAssignedResourceLocation(TaskManagerLocation assignedResourceLocation) {
		this.assignedResourceLocation = assignedResourceLocation;
		return this;
	}

	public ArchivedExecutionBuilder setAssignedAllocationID(AllocationID assignedAllocationID) {
		this.assignedAllocationID = assignedAllocationID;
		return this;
	}

	public ArchivedExecutionBuilder setUserAccumulators(StringifiedAccumulatorResult[] userAccumulators) {
		this.userAccumulators = userAccumulators;
		return this;
	}

	public ArchivedExecutionBuilder setParallelSubtaskIndex(int parallelSubtaskIndex) {
		this.parallelSubtaskIndex = parallelSubtaskIndex;
		return this;
	}

	public ArchivedExecutionBuilder setIOMetrics(IOMetrics ioMetrics) {
		this.ioMetrics = ioMetrics;
		return this;
	}

	public ArchivedExecution build() throws UnknownHostException {
		return new ArchivedExecution(
			userAccumulators != null ? userAccumulators : new StringifiedAccumulatorResult[0],
			ioMetrics != null ? ioMetrics : new TestIOMetrics(),
			attemptId != null ? attemptId : new ExecutionAttemptID(),
			attemptNumber,
			state != null ? state : ExecutionState.FINISHED,
			failureCause != null ? failureCause : "(null)",
			assignedResourceLocation != null ? assignedResourceLocation : new TaskManagerLocation(new ResourceID("tm"), InetAddress.getLocalHost(), 1234),
			assignedAllocationID != null ? assignedAllocationID : new AllocationID(0L, 0L),
			parallelSubtaskIndex,
			stateTimestamps != null ? stateTimestamps : new long[]{1, 2, 3, 4, 5, 5, 5, 5}
		);
	}

	private static class TestIOMetrics extends IOMetrics {
		private static final long serialVersionUID = -5920076211680012555L;

		public TestIOMetrics() {
			super(
				new MeterView(new TestCounter(1), 0),
				new MeterView(new TestCounter(2), 0),
				new MeterView(new TestCounter(3), 0),
				new MeterView(new TestCounter(4), 0),
				new MeterView(new TestCounter(5), 0));
		}
	}

	private static class TestCounter implements Counter {
		private final long count;

		private TestCounter(long count) {
			this.count = count;
		}

		@Override
		public void inc() {
		}

		@Override
		public void inc(long n) {
		}

		@Override
		public void dec() {
		}

		@Override
		public void dec(long n) {
		}

		@Override
		public long getCount() {
			return count;
		}
	}
}
