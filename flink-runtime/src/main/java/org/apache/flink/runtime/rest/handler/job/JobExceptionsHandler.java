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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobExceptionsInfo;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Handler serving the job exceptions.
 */
public class JobExceptionsHandler extends AbstractExecutionGraphHandler<JobExceptionsInfo, JobMessageParameters> implements JsonArchivist {

	static final int MAX_NUMBER_EXCEPTION_TO_REPORT = 20;

	public JobExceptionsHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, JobExceptionsInfo, JobMessageParameters> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor) {

		super(
			localRestAddress,
			leaderRetriever,
			timeout,
			responseHeaders,
			messageHeaders,
			executionGraphCache,
			executor);
	}

	@Override
	protected JobExceptionsInfo handleRequest(HandlerRequest<EmptyRequestBody, JobMessageParameters> request, AccessExecutionGraph executionGraph) {
		return createJobExceptionsInfo(executionGraph);
	}

	@Override
	public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
		ResponseBody json = createJobExceptionsInfo(graph);
		String path = getMessageHeaders().getTargetRestEndpointURL()
			.replace(':' + JobIDPathParameter.KEY, graph.getJobID().toString());
		return Collections.singletonList(new ArchivedJson(path, json));
	}

	private static JobExceptionsInfo createJobExceptionsInfo(AccessExecutionGraph executionGraph) {
		ErrorInfo rootException = executionGraph.getFailureInfo();
		String rootExceptionMessage = null;
		Long rootTimestamp = null;
		if (rootException != null) {
			rootExceptionMessage = rootException.getExceptionAsString();
			rootTimestamp = rootException.getTimestamp();
		}

		List<JobExceptionsInfo.ExecutionExceptionInfo> taskExceptionList = new ArrayList<>();
		boolean truncated = false;
		for (AccessExecutionVertex task : executionGraph.getAllExecutionVertices()) {
			String t = task.getFailureCauseAsString();
			if (t != null && !t.equals(ExceptionUtils.STRINGIFIED_NULL_EXCEPTION)) {
				if (taskExceptionList.size() >= MAX_NUMBER_EXCEPTION_TO_REPORT) {
					truncated = true;
					break;
				}

				TaskManagerLocation location = task.getCurrentAssignedResourceLocation();
				String locationString = location != null ?
					location.getFQDNHostname() + ':' + location.dataPort() : "(unassigned)";
				long timestamp = task.getStateTimestamp(ExecutionState.FAILED);
				taskExceptionList.add(new JobExceptionsInfo.ExecutionExceptionInfo(
					t,
					task.getTaskNameWithSubtaskIndex(),
					locationString,
					timestamp == 0 ? -1 : timestamp));
			}
		}

		return new JobExceptionsInfo(rootExceptionMessage, rootTimestamp, taskExceptionList, truncated);
	}
}
