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

package org.apache.flink.runtime.rest.handler.job.checkpoints;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.job.AbstractExecutionGraphHandler;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigHeaders;
import org.apache.flink.runtime.rest.messages.checkpoints.CheckpointConfigInfo;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.JsonArchivist;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * Handler which serves the checkpoint configuration.
 */
public class CheckpointConfigHandler extends AbstractExecutionGraphHandler<CheckpointConfigInfo, JobMessageParameters> implements JsonArchivist {

	public CheckpointConfigHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, CheckpointConfigInfo, JobMessageParameters> messageHeaders,
			ExecutionGraphCache executionGraphCache,
			Executor executor) {
		super(
			leaderRetriever,
			timeout,
			responseHeaders,
			messageHeaders,
			executionGraphCache,
			executor);
	}

	@Override
	protected CheckpointConfigInfo handleRequest(HandlerRequest<EmptyRequestBody, JobMessageParameters> request, AccessExecutionGraph executionGraph) throws RestHandlerException {
		return createCheckpointConfigInfo(executionGraph);
	}

	@Override
	public Collection<ArchivedJson> archiveJsonWithPath(AccessExecutionGraph graph) throws IOException {
		ResponseBody response;
		try {
			response = createCheckpointConfigInfo(graph);
		} catch (RestHandlerException rhe) {
			response = new ErrorResponseBody(rhe.getMessage());
		}
		String path = CheckpointConfigHeaders.getInstance().getTargetRestEndpointURL()
			.replace(':' + JobIDPathParameter.KEY, graph.getJobID().toString());
		return Collections.singletonList(new ArchivedJson(path, response));
	}

	private static CheckpointConfigInfo createCheckpointConfigInfo(AccessExecutionGraph executionGraph) throws RestHandlerException {
		final CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration = executionGraph.getCheckpointCoordinatorConfiguration();

		if (checkpointCoordinatorConfiguration == null) {
			throw new RestHandlerException(
				"Checkpointing is not enabled for this job (" + executionGraph.getJobID() + ").",
				HttpResponseStatus.NOT_FOUND);
		} else {
			CheckpointRetentionPolicy retentionPolicy = checkpointCoordinatorConfiguration.getCheckpointRetentionPolicy();

			CheckpointConfigInfo.ExternalizedCheckpointInfo externalizedCheckpointInfo = new CheckpointConfigInfo.ExternalizedCheckpointInfo(
					retentionPolicy != CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
					retentionPolicy != CheckpointRetentionPolicy.RETAIN_ON_CANCELLATION);

			String stateBackendName = executionGraph.getStateBackendName().orElse(null);

			return new CheckpointConfigInfo(
				checkpointCoordinatorConfiguration.isExactlyOnce() ? CheckpointConfigInfo.ProcessingMode.EXACTLY_ONCE : CheckpointConfigInfo.ProcessingMode.AT_LEAST_ONCE,
				checkpointCoordinatorConfiguration.getCheckpointInterval(),
				checkpointCoordinatorConfiguration.getCheckpointTimeout(),
				checkpointCoordinatorConfiguration.getMinPauseBetweenCheckpoints(),
				checkpointCoordinatorConfiguration.getMaxConcurrentCheckpoints(),
				externalizedCheckpointInfo,
				stateBackendName);
		}
	}
}
