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

package org.apache.flink.runtime.webmonitor.legacy;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.util.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonFactory {

	public static String toJson(ExecutionVertex vertex) {
		StringBuilder json = new StringBuilder("");
		json.append("{");
		json.append("\"vertexid\": \"").append(vertex.getCurrentExecutionAttempt().getAttemptId()).append("\",");
		json.append("\"vertexname\": \"").append(StringUtils.escapeHtml(vertex.getSimpleName())).append("\",");
		json.append("\"vertexstatus\": \"").append(vertex.getExecutionState()).append("\",");
		
		InstanceConnectionInfo location = vertex.getCurrentAssignedResourceLocation();
		String instanceName = location == null ? "(null)" : location.getFQDNHostname();
		
		json.append("\"vertexinstancename\": \"").append(instanceName).append("\"");
		json.append("}");
		return json.toString();
	}
	
	public static String toJson(ExecutionJobVertex jobVertex) {
		StringBuilder json = new StringBuilder("");
		
		json.append("{");
		json.append("\"groupvertexid\": \"").append(jobVertex.getJobVertexId()).append("\",");
		json.append("\"groupvertexname\": \"").append(StringUtils.escapeHtml(jobVertex.getJobVertex().getName())).append("\",");
		json.append("\"numberofgroupmembers\": ").append(jobVertex.getParallelism()).append(",");
		json.append("\"groupmembers\": [");
		
		// Count state status of group members
		Map<ExecutionState, Integer> stateCounts = new HashMap<ExecutionState, Integer>();
		
		// initialize with 0
		for (ExecutionState state : ExecutionState.values()) {
			stateCounts.put(state, 0);
		}
		
		ExecutionVertex[] vertices = jobVertex.getTaskVertices();
		
		for (int j = 0; j < vertices.length; j++) {
			ExecutionVertex vertex = vertices[j];
			
			json.append(toJson(vertex));
			
			// print delimiter
			if (j != vertices.length - 1) {
				json.append(",");
			}
			
			// Increment state status count
			int count =  stateCounts.get(vertex.getExecutionState()) + 1;
			stateCounts.put(vertex.getExecutionState(), count);
		}
		
		json.append("],");
		json.append("\"backwardEdges\": [");
		
		List<IntermediateResult> inputs = jobVertex.getInputs();
		
		for (int inputNumber = 0; inputNumber < inputs.size(); inputNumber++) {
			ExecutionJobVertex input = inputs.get(inputNumber).getProducer();
			
			json.append("{");
			json.append("\"groupvertexid\": \"").append(input.getJobVertexId()).append("\",");
			json.append("\"groupvertexname\": \"").append(StringUtils.escapeHtml(jobVertex.getJobVertex().getName())).append("\"");
			json.append("}");
			
			// print delimiter
			if(inputNumber != inputs.size() - 1) {
				json.append(",");
			}
		}
		json.append("]");
		
		// list number of members for each status
		for (Map.Entry<ExecutionState, Integer> stateCount : stateCounts.entrySet()) {
			json.append(",\"").append(stateCount.getKey()).append("\": ").append(stateCount.getValue());
		}
		
		json.append("}");
		
		return json.toString();
	}
}
