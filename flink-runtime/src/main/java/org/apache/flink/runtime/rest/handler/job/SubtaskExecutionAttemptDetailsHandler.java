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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.util.MutableIOMetrics;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.job.SubtaskAttemptMessageParameters;
import org.apache.flink.runtime.rest.messages.job.SubtaskExecutionAttemptDetailsInfo;
import org.apache.flink.runtime.rest.messages.job.metrics.IOMetricsInfo;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Handler of specific sub task execution attempt.
 */
public class SubtaskExecutionAttemptDetailsHandler extends AbstractSubtaskAttemptHandler<SubtaskExecutionAttemptDetailsInfo, SubtaskAttemptMessageParameters> {

	private final MetricFetcher<?> metricFetcher;

	/**
	 * Instantiates a new Abstract job vertex handler.
	 *
	 * @param localRestAddress    the local rest address
	 * @param leaderRetriever     the leader retriever
	 * @param timeout             the timeout
	 * @param responseHeaders     the response headers
	 * @param messageHeaders      the message headers
	 * @param executionGraphCache the execution graph cache
	 * @param executor            the executor
	 */
	public SubtaskExecutionAttemptDetailsHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, SubtaskExecutionAttemptDetailsInfo, SubtaskAttemptMessageParameters> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor,
			MetricFetcher<?> metricFetcher) {

		super(localRestAddress, leaderRetriever, timeout, responseHeaders, messageHeaders, executionGraphCache, executor);

		this.metricFetcher = Preconditions.checkNotNull(metricFetcher);
	}

	@Override
	protected SubtaskExecutionAttemptDetailsInfo handleRequest(
			HandlerRequest<EmptyRequestBody, SubtaskAttemptMessageParameters> request,
			AccessExecution execution) throws RestHandlerException {

		final ExecutionState status = execution.getState();
		final long now = System.currentTimeMillis();

		final TaskManagerLocation location = execution.getAssignedResourceLocation();
		final String locationString = location == null ? "(unassigned)" : location.getHostname();

		long startTime = execution.getStateTimestamp(ExecutionState.DEPLOYING);
		if (startTime == 0) {
			startTime = -1;
		}
		final long endTime = status.isTerminal() ? execution.getStateTimestamp(status) : -1;
		final long duration = startTime > 0 ? ((endTime > 0 ? endTime : now) - startTime) : -1;

		final MutableIOMetrics counts = new MutableIOMetrics();

		final JobID jobID = request.getPathParameter(JobIDPathParameter.class);
		final JobVertexID jobVertexID = request.getPathParameter(JobVertexIdPathParameter.class);

		counts.addIOMetrics(
			execution,
			metricFetcher,
			jobID.toString(),
			jobVertexID.toString()
		);

		final IOMetricsInfo ioMetricsInfo = new IOMetricsInfo(
			counts.getNumBytesInLocal() + counts.getNumBytesInRemote(),
			counts.isNumBytesInLocalComplete() && counts.isNumBytesInRemoteComplete(),
			counts.getNumBytesOut(),
			counts.isNumBytesOutComplete(),
			counts.getNumRecordsIn(),
			counts.isNumRecordsInComplete(),
			counts.getNumRecordsOut(),
			counts.isNumRecordsOutComplete());

		return new SubtaskExecutionAttemptDetailsInfo(
			execution.getParallelSubtaskIndex(),
			status,
			execution.getAttemptNumber(),
			locationString,
			startTime,
			endTime,
			duration,
			ioMetricsInfo
		);
	}
}
