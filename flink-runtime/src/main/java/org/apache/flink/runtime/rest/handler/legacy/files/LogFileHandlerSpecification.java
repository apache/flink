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

package org.apache.flink.runtime.rest.handler.legacy.files;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.handler.RestHandlerSpecification;

/**
 * Rest handler specification for the log file of the main cluster component.
 */
public class LogFileHandlerSpecification implements RestHandlerSpecification {
	private static final String URL = "/jobmanager/log";
	private static final String URL_WITH_RANGE = "/jobmanager/log/:range";
	private static final String URL_WITH_FILENAME_AND_RANGE = "/jobmanager/logs/:filename/:range";

	private static final LogFileHandlerSpecification INSTANCE = new LogFileHandlerSpecification(URL);
	private static final LogFileHandlerSpecification INSTANCE_WITH_RANGE = new LogFileHandlerSpecification(URL_WITH_RANGE);
	private static final LogFileHandlerSpecification INSTANCE_WITH_FILENAME_AND_RANGE = new LogFileHandlerSpecification(URL_WITH_FILENAME_AND_RANGE);
	private final String url;

	private LogFileHandlerSpecification(String url) {
		this.url = url;
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.GET;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return url;
	}

	public static LogFileHandlerSpecification getInstance() {
		return INSTANCE;
	}

	public static LogFileHandlerSpecification getInstanceWithRange() {
		return INSTANCE_WITH_RANGE;
	}

	public static LogFileHandlerSpecification getInstanceWithFilenameAndRange() {
		return INSTANCE_WITH_FILENAME_AND_RANGE;
	}
}
