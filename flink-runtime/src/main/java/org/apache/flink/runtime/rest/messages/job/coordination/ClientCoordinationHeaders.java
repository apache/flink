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

package org.apache.flink.runtime.rest.messages.job.coordination;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.job.coordination.ClientCoordinationHandler;
import org.apache.flink.runtime.rest.messages.MessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Message headers for the {@link ClientCoordinationHandler}.
 */
@Documentation.ExcludeFromDocumentation(
	"This API is not exposed to the users, as coordinators are used only internally.")
public class ClientCoordinationHeaders implements MessageHeaders<ClientCoordinationRequestBody, ClientCoordinationResponseBody, ClientCoordinationMessageParameters> {

	public static final String URL = "/jobs/:jobid/coordinators/:operatorid";

	private static final ClientCoordinationHeaders INSTANCE = new ClientCoordinationHeaders();

	private ClientCoordinationHeaders() {}

	@Override
	public Class<ClientCoordinationRequestBody> getRequestClass() {
		return ClientCoordinationRequestBody.class;
	}

	@Override
	public Class<ClientCoordinationResponseBody> getResponseClass() {
		return ClientCoordinationResponseBody.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public ClientCoordinationMessageParameters getUnresolvedMessageParameters() {
		return new ClientCoordinationMessageParameters();
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.POST;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	public static ClientCoordinationHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	public String getDescription() {
		return "Send a request to a specified coordinator of the specified job and get the response. " +
			"This API is for internal use only.";
	}
}
