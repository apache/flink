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

package org.apache.flink.runtime.webmonitor.handlers.legacy;

import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.rest.handler.legacy.AbstractJsonRequestHandler;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Handler to deny access to jar-related REST calls.
 */
public class JarAccessDeniedHandler extends AbstractJsonRequestHandler {

	private static final String ERROR_MESSAGE = "{\"error\": \"Web submission interface is not " +
			"available for this cluster. To enable it, set the configuration key ' jobmanager.web.submit.enable.'\"}";

	public JarAccessDeniedHandler(Executor executor) {
		super(executor);
	}

	@Override
	public String[] getPaths() {
		return new String[]{
			JarListHandler.JAR_LIST_REST_PATH,
			JarPlanHandler.JAR_PLAN_REST_PATH,
			JarRunHandler.JAR_RUN_REST_PATH,
			JarUploadHandler.JAR_UPLOAD_REST_PATH,
			JarDeleteHandler.JAR_DELETE_REST_PATH
		};
	}

	@Override
	public CompletableFuture<String> handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) {
		return CompletableFuture.completedFuture(ERROR_MESSAGE);
	}
}
