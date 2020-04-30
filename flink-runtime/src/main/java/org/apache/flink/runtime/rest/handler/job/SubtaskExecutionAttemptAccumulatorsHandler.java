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
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.SubtaskIndexPathParameter;
import org.apache.flink.runtime.rest.messages.job.SubtaskAttemptMessageParameters;
import org.apache.flink.runtime.rest.messages.job.SubtaskAttemptPathParameter;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptAccumulatorsInfo;
import org.apache.flink.runtime.rest.messages.job.UserAccumulator;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Request handler for the subtask execution attempt accumulators.
 */
public class SubtaskExecutionAttemptAccumulatorsHandler
	extends AbstractSubtaskAttemptHandler<SubtaskExecutionAttemptAccumulatorsInfo, SubtaskAttemptMessageParameters>
	implements JsonArchivist {

	/**
	 * Instantiates a new Abstract job vertex handler.
	 *
	 * @param leaderRetriever     the leader retriever
	 * @param timeout             the timeout
	 * @param responseHeaders     the response headers
	 * @param messageHeaders      the message headers
	 * @param executionGraphCache the execution graph cache
	 * @param executor            the executor
	 */
	public SubtaskExecutionAttemptAccumulatorsHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, SubtaskExecutionAttemptAccumulatorsInfo, SubtaskAttemptMessageParameters> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor) {

		super(leaderRetriever, timeout, responseHeaders, messageHeaders, executionGraphCache, executor);
	}

	@Override
	protected SubtaskExecutionAttemptAccumulatorsInfo handleRequest(
			HandlerRequest<EmptyRequestBody, SubtaskAttemptMessageParameters> request,
			AccessExecution execution) throws RestHandlerException {
		return createAccumulatorInfo(execution);
	}

	@Override
	public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
		List<ArchivedJson> archive = new ArrayList<>(16);
		for (AccessExecutionJobVertex task : graph.getAllVertices().values()) {
			for (AccessExecutionVertex subtask : task.getTaskVertices()) {
				ResponseBody curAttemptJson = createAccumulatorInfo(subtask.getCurrentExecutionAttempt());
				String curAttemptPath = getMessageHeaders().getTargetRestEndpointURL()
					.replace(':' + JobIDPathParameter.KEY, graph.getJobID().toString())
					.replace(':' + JobVertexIdPathParameter.KEY, task.getJobVertexId().toString())
					.replace(':' + SubtaskIndexPathParameter.KEY, String.valueOf(subtask.getParallelSubtaskIndex()))
					.replace(':' + SubtaskAttemptPathParameter.KEY, String.valueOf(subtask.getCurrentExecutionAttempt().getAttemptNumber()));

				archive.add(new ArchivedJson(curAttemptPath, curAttemptJson));

				for (int x = 0; x < subtask.getCurrentExecutionAttempt().getAttemptNumber(); x++) {
					AccessExecution attempt = subtask.getPriorExecutionAttempt(x);
					if (attempt != null){
						ResponseBody json = createAccumulatorInfo(attempt);
						String path = getMessageHeaders().getTargetRestEndpointURL()
							.replace(':' + JobIDPathParameter.KEY, graph.getJobID().toString())
							.replace(':' + JobVertexIdPathParameter.KEY, task.getJobVertexId().toString())
							.replace(':' + SubtaskIndexPathParameter.KEY, String.valueOf(subtask.getParallelSubtaskIndex()))
							.replace(':' + SubtaskAttemptPathParameter.KEY, String.valueOf(attempt.getAttemptNumber()));
						archive.add(new ArchivedJson(path, json));
					}
				}
			}
		}
		return archive;
	}

	private static SubtaskExecutionAttemptAccumulatorsInfo createAccumulatorInfo(AccessExecution execution) {
		final StringifiedAccumulatorResult[] accs = execution.getUserAccumulatorsStringified();
		final ArrayList<UserAccumulator> userAccumulatorList = new ArrayList<>(accs.length);

		for (StringifiedAccumulatorResult acc : accs) {
			userAccumulatorList.add(new UserAccumulator(acc.getName(), acc.getType(), acc.getValue()));
		}

		return new SubtaskExecutionAttemptAccumulatorsInfo(
			execution.getParallelSubtaskIndex(),
			execution.getAttemptNumber(),
			execution.getAttemptId().toString(),
			userAccumulatorList);
	}
}
