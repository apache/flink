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
import org.apache.flink.runtime.rest.handler.async.AsynchronousOperationTriggerMessageHeaders;
import org.apache.flink.runtime.rest.handler.job.savepoints.SavepointDisposalHandlers;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * {@link AsynchronousOperationTriggerMessageHeaders} for the {@link SavepointDisposalHandlers.SavepointDisposalTriggerHandler}.
 */
public class SavepointDisposalTriggerHeaders extends AsynchronousOperationTriggerMessageHeaders<SavepointDisposalRequest, EmptyMessageParameters> {

	private static final SavepointDisposalTriggerHeaders INSTANCE = new SavepointDisposalTriggerHeaders();

	private static final String URL = "/savepoint-disposal";

	private SavepointDisposalTriggerHeaders() {}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public Class<SavepointDisposalRequest> getRequestClass() {
		return SavepointDisposalRequest.class;
	}

	@Override
	public EmptyMessageParameters getUnresolvedMessageParameters() {
		return EmptyMessageParameters.getInstance();
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.POST;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	public static SavepointDisposalTriggerHeaders getInstance() {
		return INSTANCE;
	}
}
