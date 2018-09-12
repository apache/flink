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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.util.JsonUtils;

import java.io.File;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * JobManagerLogListHandler serves the request which gets all log files list.
 */
public class JobManagerLogListHandler extends AbstractJsonRequestHandler {
	private static final String JOBMANAGER_LOG_FILE_LIST_PATH = "/jobmanager/loglist";
	private final File basePath;
	public JobManagerLogListHandler(Executor executor, File basePath) {
		super(executor);
		this.basePath = basePath;
	}

	@Override
	public String[] getPaths() {
		return new String[]{JOBMANAGER_LOG_FILE_LIST_PATH};
	}

	@Override
	public CompletableFuture<String> handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) {
		File parent = new File(basePath.getParent());
		String[] relatedFiles = parent.list((dir, name) -> !(name.endsWith(".out") || name.endsWith(".err")));
		return CompletableFuture.completedFuture(JsonUtils.createSortedFilesJson(relatedFiles));
	}
}
