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

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;

/**
 * Utilities for the web runtime monitor. This class contains for example methods to build
 * messages with aggregate information about the state of an execution graph, to be send
 * to the web server.
 */
public class WebMonitorUtils {

	public static JobDetails createDetailsForJob(ExecutionGraph job) {
		JobStatus status = job.getState();
		
		long started = job.getStatusTimestamp(JobStatus.CREATED);
		long finished = status.isTerminalState() ? job.getStatusTimestamp(status) : -1L;
		
		int[] countsPerStatus = new int[ExecutionState.values().length];
		long lastChanged = 0;
		int numTotalTasks = 0;
		
		for (ExecutionJobVertex ejv : job.getVerticesTopologically()) {
			ExecutionVertex[] vertices = ejv.getTaskVertices();
			numTotalTasks += vertices.length;
			
			for (ExecutionVertex vertex : vertices) {
				ExecutionState state = vertex.getExecutionState();
				countsPerStatus[state.ordinal()]++;
				lastChanged = Math.max(lastChanged, vertex.getStateTimestamp(state));
			}
		}
		
		lastChanged = Math.max(lastChanged, finished);
		
		return new JobDetails(job.getJobID(), job.getJobName(),
				started, finished, status, lastChanged,  
				countsPerStatus, numTotalTasks);
	}
}
