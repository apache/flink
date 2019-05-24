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

package org.apache.flink.runtime.webmonitor.handlers;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.MessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import static org.apache.flink.runtime.webmonitor.handlers.utils.ArtifactHandlerConstants.ARTIFACT_MESSAGE_HEADER_PREFIX;

/**
 * {@link MessageHeaders} for {@link ArtifactRunHandler}.
 */
public class ArtifactRunHeaders implements MessageHeaders<ArtifactRunRequestBody, ArtifactRunResponseBody, ArtifactRunMessageParameters> {

	private static final ArtifactRunHeaders INSTANCE = new ArtifactRunHeaders();

	private ArtifactRunHeaders() {}

	@Override
	public Class<ArtifactRunResponseBody> getResponseClass() {
		return ArtifactRunResponseBody.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public Class<ArtifactRunRequestBody> getRequestClass() {
		return ArtifactRunRequestBody.class;
	}

	@Override
	public ArtifactRunMessageParameters getUnresolvedMessageParameters() {
		return new ArtifactRunMessageParameters();
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.POST;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return ARTIFACT_MESSAGE_HEADER_PREFIX + "/:" + ArtifactIdPathParameter.KEY + "/run";
	}

	public static ArtifactRunHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	public String getDescription() {
		return "Submits a job by running an artifact previously uploaded via '" + ArtifactUploadHeaders.URL + "'. " +
			"Program arguments can be passed both via the JSON request (recommended) or query parameters.";
	}
}
