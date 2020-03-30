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

package org.apache.flink.runtime.rest.handler.cluster;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.LogFileNamePathParameter;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;
import org.apache.flink.runtime.rest.messages.cluster.FileMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.StringUtils;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.io.File;
import java.util.Map;
import java.util.concurrent.CompletionException;

/**
 * Rest handler which serves the custom log file from JobManager.
 */
public class JobManagerCustomLogHandler extends AbstractJobManagerFileHandler<FileMessageParameters> {

	public JobManagerCustomLogHandler(
			GatewayRetriever<? extends RestfulGateway> leaderRetriever,
			Time timeout, Map<String, String> responseHeaders,
			UntypedResponseMessageHeaders<EmptyRequestBody, FileMessageParameters> messageHeaders,
			WebMonitorUtils.LogFileLocation logFileLocation) {
		super(leaderRetriever, timeout, responseHeaders, messageHeaders, logFileLocation);
	}

	@Override
	protected File getFile(HandlerRequest<EmptyRequestBody, FileMessageParameters> handlerRequest) {
		if (logFileLocation.logFile == null) {
			throw new CompletionException(new RestHandlerException("Can not get JobManager log dir.", HttpResponseStatus.NOT_FOUND));
		}
		final String logDir = logFileLocation.logFile.getParent();
		if (StringUtils.isNullOrWhitespaceOnly(logDir)) {
			throw new CompletionException(new RestHandlerException("JobManager log dir is empty.", HttpResponseStatus.NOT_FOUND));
		}
		String filename = handlerRequest.getPathParameter(LogFileNamePathParameter.class);
		return new File(logDir, filename);
	}
}
