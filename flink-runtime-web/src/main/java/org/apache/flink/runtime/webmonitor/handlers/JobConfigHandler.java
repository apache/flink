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

import java.util.Map;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Request handler that returns the configuration of a job.
 */
public class JobConfigHandler extends AbstractExecutionGraphRequestHandler implements RequestHandler.JsonResponse {

	private static final Logger LOG = LoggerFactory.getLogger(JobConfigHandler.class);

	public JobConfigHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String handleRequest(ExecutionGraph graph, Map<String, String> params) throws Exception {

		JSONObject obj = new JSONObject();

		obj.put("jid", graph.getJobID().toString());
		obj.put("name", graph.getJobName());

		JSONObject execConfig = new JSONObject();

		ExecutionConfig ec = graph.getExecutionConfig();
		if (ec != null) {
			execConfig.put("execution-mode", ec.getExecutionMode());
			execConfig.put("max-execution-retries", ec.getNumberOfExecutionRetries());
			execConfig.put("job-parallelism", ec.getParallelism());
			execConfig.put("object-reuse-mode", ec.isObjectReuseEnabled());

			ExecutionConfig.GlobalJobParameters uc = ec.getGlobalJobParameters();
			if (uc != null) {
				Map<String, String> ucVals = uc.toMap();
				if (ucVals != null) {
					JSONObject userConfig = new JSONObject();
					for (Map.Entry<String, String> ucVal : ucVals.entrySet()) {
						userConfig.put(ucVal.getKey(), ucVal.getValue());
					}
					execConfig.put("user-config", userConfig);
				} else {
					LOG.debug("GlobalJobParameters.toMap() did not return anything");
				}
			} else {
				LOG.debug("No GlobalJobParameters were set in the execution config");
			}
		} else {
			LOG.warn("Unable to retrieve execution config from execution graph");
		}

		obj.put("execution-config", execConfig);

		return obj.toString();
	}
}
