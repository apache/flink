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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.legacy.ClusterConfigHandler;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Message headers for the {@link ClusterConfigHandler}.
 */
public final class ClusterConfigurationInfoHeaders implements MessageHeaders<EmptyRequestBody, ClusterConfigurationInfo, EmptyMessageParameters> {

	private static final ClusterConfigurationInfoHeaders INSTANCE = new ClusterConfigurationInfoHeaders();

	// TODO this REST path is inappropriately set due to legacy design reasons, and ideally should be '/config';
	// TODO changing it would require corresponding path changes in flink-runtime-web
	public static final String CLUSTER_CONFIG_REST_PATH = "/jobmanager/config";

	private ClusterConfigurationInfoHeaders() {}

	@Override
	public Class<EmptyRequestBody> getRequestClass() {
		return EmptyRequestBody.class;
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.GET;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return CLUSTER_CONFIG_REST_PATH;
	}

	@Override
	public Class<ClusterConfigurationInfo> getResponseClass() {
		return ClusterConfigurationInfo.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public EmptyMessageParameters getUnresolvedMessageParameters() {
		return EmptyMessageParameters.getInstance();
	}

	public static ClusterConfigurationInfoHeaders getInstance() {
		return INSTANCE;
	}

	@Override
	public String getDescription() {
		return "Returns the cluster configuration.";
	}
}
