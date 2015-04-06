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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.JsonFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Request handler that returns a summary of the job status.
 */
public class JobSummaryHandler extends AbstractExecutionGraphRequestHandler implements RequestHandler.JsonResponse {

	private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public JobSummaryHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String handleRequest(ExecutionGraph graph, Map<String, String> params) throws Exception {

		JobID jid = graph.getJobID();
		String name = graph.getJobName();
		
		long startTime = graph.getStatusTimestamp(JobStatus.CREATED);
		long endTime = graph.getState().isTerminalState() ?
				graph.getStatusTimestamp(graph.getState()) : -1;
		
		long duration = endTime == -1 ? System.currentTimeMillis() - startTime :
				endTime - startTime;
		
		String startTimeString;
		String endTimeTimeString;
		String durationString = duration + " msecs";
		
		synchronized (dateFormatter) {
			startTimeString = dateFormatter.format(new Date(startTime));
			endTimeTimeString =  endTime == -1 ? "(pending)" : dateFormatter.format(new Date(endTime));
		}
		
		String status = graph.getState().name();
		
		int pending = 0;
		int running = 0;
		int finished = 0;
		int canceling = 0;
		int canceled = 0;
		int failed = 0;
		
		for (ExecutionJobVertex vertex : graph.getVerticesTopologically()) {
			ExecutionState aggState = vertex.getAggregateState();
			switch (aggState) {
				case FINISHED:
					finished++;
					break;
				case FAILED:
					failed++;
					break;
				case CANCELED:
					canceled++;
					break;
				case RUNNING:
					running++;
					break;
				case CANCELING:
					canceling++;
					break;
				default:
					pending++;
			}
		}
		
		int total = pending + running + finished + canceling + canceled + failed;
		
		return JsonFactory.createJobSummaryJSON(jid, name, status, startTimeString, endTimeTimeString, durationString, 
				total, pending, running, finished, canceling, canceled, failed);
	}
}
