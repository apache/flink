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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.webmonitor.ExecutionGraphHolder;
import org.codehaus.jettison.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Request handler that returns the JSON program plan of a job graph.
 */
public class JobVerticesOverviewHandler extends AbstractExecutionGraphRequestHandler implements RequestHandler.JsonResponse {

	
	public JobVerticesOverviewHandler(ExecutionGraphHolder executionGraphHolder) {
		super(executionGraphHolder);
	}

	@Override
	public String handleRequest(ExecutionGraph graph, Map<String, String> params) throws Exception {

		JSONObject obj = new JSONObject();
		
		obj.put("jid", graph.getJobID().toString());
		obj.put("name", graph.getJobName());

		List<JSONObject> vertexJSONs = new ArrayList<JSONObject>();

		for (ExecutionJobVertex vertex : graph.getVerticesTopologically()) {
			JSONObject vertexJSON = new JSONObject();
			vertexJSONs.add(vertexJSON);

			// identifying parameters
			JobVertex jobVertex = vertex.getJobVertex();
			vertexJSON.put("id", jobVertex.getID().toString());
			vertexJSON.put("name", jobVertex.getName());

			// time
			vertexJSON.put("start-time", System.currentTimeMillis() - 10000);
			vertexJSON.put("end-time", System.currentTimeMillis() - 6453);

			// read / write
			vertexJSON.put("bytes-read-local", 14355376592L);
			vertexJSON.put("bytes-read-remote", 607623465782L);
			vertexJSON.put("bytes-written", 5372934L);
			vertexJSON.put("records-read", 4659765L);
			vertexJSON.put("records-written", 4659765L);

			vertexJSON.put("parallelism", vertex.getParallelism());

			JSONObject states = new JSONObject();
			{
				// count the occurrence of each state
				int[] statesCount = new int[ExecutionState.values().length];
				for (ExecutionVertex ev: vertex.getTaskVertices()) {
					Execution ee = ev.getCurrentExecutionAttempt();
					if (ee != null) {
						statesCount[ee.getState().ordinal()]++;
					}
				}
				
				int i = 0;
				for (ExecutionState state : ExecutionState.values()) {
					states.put(state.name(), statesCount[i++]);
				}
			}
			vertexJSON.put("states", states);
		}

		obj.put("vertices", vertexJSONs);
		
		return obj.toString();
	}
}
