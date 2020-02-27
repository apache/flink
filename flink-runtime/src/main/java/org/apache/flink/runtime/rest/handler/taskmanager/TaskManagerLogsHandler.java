/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.exceptions.UnknownTaskExecutorException;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.LogInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.LogsInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

/**
 * Handler which serves detailed TaskManager information.
 */
public class TaskManagerLogsHandler extends AbstractTaskManagerHandler<RestfulGateway, EmptyRequestBody, LogsInfo, TaskManagerMessageParameters> {

	private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;

	public TaskManagerLogsHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, LogsInfo, TaskManagerMessageParameters> messageHeaders,
			GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders, resourceManagerGatewayRetriever);

		this.resourceManagerGatewayRetriever = Preconditions.checkNotNull(resourceManagerGatewayRetriever);
	}

	@Override
	protected CompletableFuture<LogsInfo> handleRequest(
			@Nonnull HandlerRequest<EmptyRequestBody, TaskManagerMessageParameters> request,
			@Nonnull ResourceManagerGateway gateway) throws RestHandlerException {
		final ResourceID taskManagerId = request.getPathParameter(TaskManagerIdPathParameter.class);
		final ResourceManagerGateway resourceManagerGateway = getResourceManagerGateway(resourceManagerGatewayRetriever);
		final CompletableFuture<Collection<Tuple2<String, Long>>> logsWithLengthFuture = resourceManagerGateway.requestTaskManagerLogList(taskManagerId, timeout);

		return logsWithLengthFuture.thenApply(logName2Sizes -> {
			if (null != logName2Sizes) {
				Collection<LogInfo> logs = logName2Sizes.stream().map(logName2Size -> new LogInfo(logName2Size.f0, logName2Size.f1)).collect(Collectors.toSet());
				return new LogsInfo(logs);
			} else {
				return LogsInfo.empty();
			}
		}).exceptionally(
				(Throwable throwable) -> {
					final Throwable strippedThrowable = ExceptionUtils.stripExecutionException(throwable);

					if (strippedThrowable instanceof UnknownTaskExecutorException) {
						throw new CompletionException(
							new RestHandlerException(
								"Could not find TaskExecutor " + taskManagerId + '.',
								HttpResponseStatus.NOT_FOUND,
								strippedThrowable));
					} else {
						throw new CompletionException(strippedThrowable);
					}
				}
			);
	}

	/**
	 * get ResourceManagerGateway from GatewayRetriever ResourceManagerGateway.
	 * @param resourceManagerGatewayRetriever
	 * @return ResourceManagerGateway
	 * @throws RestHandlerException
	 */
	protected ResourceManagerGateway getResourceManagerGateway(GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever) throws RestHandlerException {
		ResourceManagerGateway resourceManagerGateway = resourceManagerGatewayRetriever
			.getNow()
			.orElseThrow(() -> new RestHandlerException(
				"Cannot connect to ResourceManager right now. Please try to refresh.",
				HttpResponseStatus.NOT_FOUND));
		return resourceManagerGateway;
	}
}
