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

package org.apache.flink.runtime.rest.handler.files;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.LogListInfo;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * JobManagerLogListHandler serves the request which gets the historical log file list of jobmanager.
 */
public class JobManagerLogListHandler extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, LogListInfo, MessageParameters> {
	private final File parentPath;

	public JobManagerLogListHandler(
		@Nonnull GatewayRetriever leaderRetriever,
		@Nonnull Time timeout,
		@Nonnull Map responseHeaders,
		@Nonnull MessageHeaders messageHeaders,
		File parentPath) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders);
		this.parentPath = parentPath;
	}

	@Override
	protected CompletableFuture<LogListInfo> handleRequest(@Nonnull HandlerRequest<EmptyRequestBody, MessageParameters> request, @Nonnull RestfulGateway gateway) throws RestHandlerException {
		String[] relatedFiles = parentPath.list((dir, name) -> !(name.endsWith(".out") || name.endsWith(".err")));
		LogListInfo logListInfo = new LogListInfo(relatedFiles);
		return CompletableFuture.completedFuture(logListInfo);
	}
}
