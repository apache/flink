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
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStats;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexBackPressureInfo;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Request handler for the job vertex back pressure.
 */
public class JobVertexBackPressureHandler extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, JobVertexBackPressureInfo, JobVertexMessageParameters> {

	public JobVertexBackPressureHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, JobVertexBackPressureInfo, JobVertexMessageParameters> messageHeaders) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders);
	}

	@Override
	protected CompletableFuture<JobVertexBackPressureInfo> handleRequest(
			@Nonnull HandlerRequest<EmptyRequestBody, JobVertexMessageParameters> request,
			@Nonnull RestfulGateway gateway) throws RestHandlerException {
		final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
		final JobVertexID jobVertexId = request.getPathParameter(JobVertexIdPathParameter.class);
		return gateway
			.requestOperatorBackPressureStats(jobId, jobVertexId)
			.thenApply(
				operatorBackPressureStats ->
					operatorBackPressureStats.getOperatorBackPressureStats().map(
						JobVertexBackPressureHandler::createJobVertexBackPressureInfo).orElse(
						JobVertexBackPressureInfo.deprecated()));
	}

	private static JobVertexBackPressureInfo createJobVertexBackPressureInfo(
			final OperatorBackPressureStats operatorBackPressureStats) {
		return new JobVertexBackPressureInfo(
			JobVertexBackPressureInfo.VertexBackPressureStatus.OK,
			getBackPressureLevel(operatorBackPressureStats.getMaxBackPressureRatio()),
			operatorBackPressureStats.getEndTimestamp(),
			IntStream.range(0, operatorBackPressureStats.getNumberOfSubTasks())
				.mapToObj(subtask -> {
					final double backPressureRatio = operatorBackPressureStats.getBackPressureRatio(subtask);
					return new JobVertexBackPressureInfo.SubtaskBackPressureInfo(
						subtask,
						getBackPressureLevel(backPressureRatio),
						backPressureRatio);
				})
				.collect(Collectors.toList()));
	}

	/**
	 * Returns the back pressure level as a String.
	 *
	 * @param backPressureRatio Ratio of back pressures samples to total number of samples.
	 *
	 * @return Back pressure level ('ok', 'low', or 'high')
	 */
	private static JobVertexBackPressureInfo.VertexBackPressureLevel getBackPressureLevel(double backPressureRatio) {
		if (backPressureRatio <= 0.10) {
			return JobVertexBackPressureInfo.VertexBackPressureLevel.OK;
		} else if (backPressureRatio <= 0.5) {
			return JobVertexBackPressureInfo.VertexBackPressureLevel.LOW;
		} else {
			return JobVertexBackPressureInfo.VertexBackPressureLevel.HIGH;
		}
	}
}
