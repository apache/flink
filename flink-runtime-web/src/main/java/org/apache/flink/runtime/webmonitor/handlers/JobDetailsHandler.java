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

import com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.apache.flink.runtime.webmonitor.metrics.MetricFetcher;
import org.apache.flink.runtime.webmonitor.utils.JsonUtils;

import java.io.StringWriter;
import java.util.Map;

/**
 * Request handler that returns details about a job, including:
 * <ul>
 *     <li>Dataflow plan</li>
 *     <li>id, name, and current status</li>
 *     <li>start time, end time, duration</li>
 *     <li>number of job vertices in each state (pending, running, finished, failed)</li>
 *     <li>info about job vertices, including runtime, status, I/O bytes and records, subtasks in each status</li>
 * </ul>
 */
public class JobDetailsHandler extends AbstractExecutionGraphRequestHandler {

	private final MetricFetcher fetcher;

	public JobDetailsHandler(ExecutionGraphHolder executionGraphHolder, MetricFetcher fetcher) {
		super(executionGraphHolder);
		this.fetcher = fetcher;
	}

	@Override
	public String handleRequest(AccessExecutionGraph graph, Map<String, String> params) throws Exception {
		final StringWriter writer = new StringWriter();
		final JsonGenerator gen = JsonFactory.jacksonFactory.createGenerator(writer);
		JsonUtils.writeJobDetailsAsJson(graph, fetcher, gen);
		gen.close();
		return writer.toString();
	}
}
