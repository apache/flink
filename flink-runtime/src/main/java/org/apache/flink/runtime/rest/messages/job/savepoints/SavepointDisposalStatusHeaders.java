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

package org.apache.flink.runtime.rest.messages.job.savepoints;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationInfo;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationStatusMessageHeaders;
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationTriggerMessageHeaders;
import org.apache.flink.runtime.rest.handler.job.savepoints.SavepointDisposalHandlers;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.TriggerIdPathParameter;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * {@link AsynchronousOperationTriggerMessageHeaders} implementation for the {@link SavepointDisposalHandlers.SavepointDisposalStatusHandler}.
 */
public class SavepointDisposalStatusHeaders extends AsynchronousOperationStatusMessageHeaders<AsynchronousOperationInfo, SavepointDisposalStatusMessageParameters> {

	private static final SavepointDisposalStatusHeaders INSTANCE = new SavepointDisposalStatusHeaders();

	private static final String URL = String.format("/savepoint-disposal/:%s", TriggerIdPathParameter.KEY);

	private SavepointDisposalStatusHeaders() {}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public Class<EmptyRequestBody> getRequestClass() {
		return EmptyRequestBody.class;
	}

	@Override
	public SavepointDisposalStatusMessageParameters getUnresolvedMessageParameters() {
		return new SavepointDisposalStatusMessageParameters();
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.GET;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	public static SavepointDisposalStatusHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	protected Class<AsynchronousOperationInfo> getValueClass() {
		return AsynchronousOperationInfo.class;
	}
}
