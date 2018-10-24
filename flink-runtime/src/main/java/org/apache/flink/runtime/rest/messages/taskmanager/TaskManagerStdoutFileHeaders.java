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

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.taskmanager.TaskManagerLogFileHandler;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.LogSizeQueryParameter;
import org.apache.flink.runtime.rest.messages.LogStartOffsetQueryParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Collection;

/**
 * Headers for the {@link TaskManagerLogFileHandler}.
 */
public class TaskManagerStdoutFileHeaders implements UntypedResponseMessageHeaders<EmptyRequestBody, TaskManagerMessageParameters> {
	private static final TaskManagerStdoutFileHeaders INSTANCE = new TaskManagerStdoutFileHeaders();

	private static final String URL = String.format("/taskmanagers/:%s/stdout", TaskManagerIdPathParameter.KEY);

	private TaskManagerStdoutFileHeaders() {}

	@Override
	public Class<EmptyRequestBody> getRequestClass() {
		return EmptyRequestBody.class;
	}

	@Override
	public TaskManagerMessageParameters getUnresolvedMessageParameters() {
		return new TaskManagerMessageParameters() {
			private final LogStartOffsetQueryParameter logStartOffsetQueryParameter = new LogStartOffsetQueryParameter();
			private final LogSizeQueryParameter logSizeQueryParameter = new LogSizeQueryParameter();

			@Override
			public Collection<MessageQueryParameter<?>> getQueryParameters() {
				return Lists.newArrayList(logStartOffsetQueryParameter, logSizeQueryParameter);
			}
		};
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.GET;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	public static TaskManagerStdoutFileHeaders getInstance() {
		return INSTANCE;
	}
}
