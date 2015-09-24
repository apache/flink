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
	
	public static void aggregateExecutionStateTimestamps(long[] timestamps, long[] other) {
		timestamps[CREATED_POS] = Math.min(timestamps[CREATED_POS], other[CREATED_POS]);
		timestamps[SCHEDULED_POS] = Math.min(timestamps[SCHEDULED_POS], other[SCHEDULED_POS]);
		timestamps[DEPLOYING_POS] = Math.min(timestamps[DEPLOYING_POS], other[DEPLOYING_POS]);
		timestamps[RUNNING_POS] = Math.min(timestamps[RUNNING_POS], other[RUNNING_POS]);
		timestamps[FINISHED_POS] = Math.max(timestamps[FINISHED_POS], other[FINISHED_POS]);
		timestamps[CANCELING_POS] = Math.min(timestamps[CANCELING_POS], other[CANCELING_POS]);
		timestamps[CANCELED_POS] = Math.max(timestamps[CANCELED_POS], other[CANCELED_POS]);
		timestamps[FAILED_POS] = Math.min(timestamps[FAILED_POS], other[FAILED_POS]);
	}
	
	// ------------------------------------------------------------------------

	private static final int CREATED_POS = ExecutionState.CREATED.ordinal();
	private static final int SCHEDULED_POS = ExecutionState.SCHEDULED.ordinal();
	private static final int DEPLOYING_POS = ExecutionState.DEPLOYING.ordinal();
	private static final int RUNNING_POS = ExecutionState.RUNNING.ordinal();
	private static final int FINISHED_POS = ExecutionState.FINISHED.ordinal();
	private static final int CANCELING_POS = ExecutionState.CANCELING.ordinal();
	private static final int CANCELED_POS = ExecutionState.CANCELED.ordinal();
	private static final int FAILED_POS = ExecutionState.FAILED.ordinal();
}
