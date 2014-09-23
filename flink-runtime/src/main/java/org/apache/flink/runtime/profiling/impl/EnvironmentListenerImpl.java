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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.runtime.execution.ExecutionListener;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.RuntimeEnvironment;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

public class EnvironmentListenerImpl implements ExecutionListener {

	private static final Logger LOG = LoggerFactory.getLogger(EnvironmentListenerImpl.class);

	private final TaskManagerProfilerImpl taskManagerProfiler;

	private final RuntimeEnvironment environment;

	
	public EnvironmentListenerImpl(TaskManagerProfilerImpl taskManagerProfiler, RuntimeEnvironment environment) {
		this.taskManagerProfiler = taskManagerProfiler;
		this.environment = environment;
	}


	@Override
	public void executionStateChanged(JobID jobID, JobVertexID vertexId, int subtaskIndex, ExecutionAttemptID executionId, ExecutionState newExecutionState, String optionalMessage) {

		switch (newExecutionState) {
		case RUNNING:
			this.taskManagerProfiler.registerMainThreadForCPUProfiling(this.environment, this.environment.getExecutingThread(), vertexId, subtaskIndex, executionId);
			break;
			
		case FINISHED:
		case CANCELING:
		case CANCELED:
		case FAILED:
			this.taskManagerProfiler.unregisterMainThreadFromCPUProfiling(this.environment, this.environment.getExecutingThread());
			break;
			
		default:
			LOG.error(String.format("Unexpected state transition to %s for vertex %s (%d) attempt %s", newExecutionState, vertexId, subtaskIndex, executionId));
			break;
		}
	}
}
