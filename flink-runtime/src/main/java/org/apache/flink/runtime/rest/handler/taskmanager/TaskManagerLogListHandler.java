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

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.LogListInfo;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * TaskManagerLogListHandler serves the request which gets the historical log file list of a given taskmanager.
 */
public class TaskManagerLogListHandler extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, LogListInfo, TaskManagerMessageParameters> {
	private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
	public TaskManagerLogListHandler(
		@Nonnull GatewayRetriever leaderRetriever,
		@Nonnull Time timeout,
		@Nonnull Map responseHeaders,
		@Nonnull MessageHeaders messageHeaders,
		GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders);

		this.resourceManagerGatewayRetriever = Preconditions.checkNotNull(resourceManagerGatewayRetriever);
	}

	@Override
	protected CompletableFuture<LogListInfo> handleRequest(@Nonnull HandlerRequest request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
		final ResourceID taskManagerId = (ResourceID) request.getPathParameter(TaskManagerIdPathParameter.class);
		Optional<ResourceManagerGateway> resourceManagerGatewayOptional = resourceManagerGatewayRetriever.getNow();
		ResourceManagerGateway resourceManagerGateway = resourceManagerGatewayOptional.orElseThrow(
			() -> new RestHandlerException("Cannot connect to ResourceManager right now. Please try to refresh.", HttpResponseStatus.NOT_FOUND));
		try {
			String[] relatedFiles = resourceManagerGateway.requestTaskManagerLogList(taskManagerId, timeout).get();
			LogListInfo logListInfo = new LogListInfo(relatedFiles);
			return CompletableFuture.completedFuture(logListInfo);
		} catch (InterruptedException e) {
			throw new RestHandlerException("Get log list is Interrupted", HttpResponseStatus.INTERNAL_SERVER_ERROR, e);
		} catch (ExecutionException e) {
			throw new RestHandlerException("Failed to get log list from " + taskManagerId.getResourceIdString(), HttpResponseStatus.EXPECTATION_FAILED, e);
		}
	}
}
