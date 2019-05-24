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
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import static org.apache.flink.runtime.webmonitor.handlers.utils.ArtifactHandlerConstants.ARTIFACT_MESSAGE_HEADER_PREFIX;

/**
 * Message headers for the {@link ArtifactListHandler}.
 */
public class ArtifactListHeaders implements MessageHeaders<EmptyRequestBody, ArtifactListInfo, EmptyMessageParameters> {

	public static final String URL = ARTIFACT_MESSAGE_HEADER_PREFIX;

	private static final ArtifactListHeaders INSTANCE = new ArtifactListHeaders();

	private ArtifactListHeaders() {}

	@Override
	public Class<EmptyRequestBody> getRequestClass() {
		return EmptyRequestBody.class;
	}

	@Override
	public Class<ArtifactListInfo> getResponseClass() {
		return ArtifactListInfo.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public EmptyMessageParameters getUnresolvedMessageParameters() {
		return EmptyMessageParameters.getInstance();
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.GET;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return ARTIFACT_MESSAGE_HEADER_PREFIX;
	}

	public static ArtifactListHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	public String getDescription() {
		return "Returns a list of all artifacts previously uploaded via '" + ArtifactUploadHeaders.URL + "'.";
	}
}
