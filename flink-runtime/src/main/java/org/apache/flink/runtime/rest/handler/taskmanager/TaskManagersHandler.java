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
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersInfo;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Returns an overview over all registered TaskManagers of the cluster.
 */
public class TaskManagersHandler extends AbstractTaskManagerHandler<RestfulGateway, EmptyRequestBody, TaskManagersInfo, EmptyMessageParameters> {

	public TaskManagersHandler(
			CompletableFuture<String> localRestAddress,
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout,
			Map<String, String> responseHeaders,
			MessageHeaders<EmptyRequestBody, TaskManagersInfo, EmptyMessageParameters> messageHeaders,
			GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever) {
		super(
			localRestAddress,
			leaderRetriever,
			timeout,
			responseHeaders,
			messageHeaders,
			resourceManagerGatewayRetriever);
	}

	@Override
	protected CompletableFuture<TaskManagersInfo> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request, @Nonnull ResourceManagerGateway gateway) throws RestHandlerException {
		return gateway
			.requestTaskManagerInfo(timeout)
			.thenApply(TaskManagersInfo::new);
	}
}
