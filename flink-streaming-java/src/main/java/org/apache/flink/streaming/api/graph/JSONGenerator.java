/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.StreamOperator;

import org.apache.sling.commons.json.JSONArray;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper class for generating a JSON representation from a {@link StreamGraph}.
 */
@Internal
public class JSONGenerator {

	public static final String STEPS = "step_function";
	public static final String ID = "id";
	public static final String SIDE = "side";
	public static final String SHIP_STRATEGY = "ship_strategy";
	public static final String PREDECESSORS = "predecessors";
	public static final String TYPE = "type";
	public static final String PACT = "pact";
	public static final String CONTENTS = "contents";
	public static final String PARALLELISM = "parallelism";

	private StreamGraph streamGraph;

	public JSONGenerator(StreamGraph streamGraph) {
		this.streamGraph = streamGraph;
	}

	public String getJSON() throws JSONException {
		JSONObject json = new JSONObject();
		JSONArray nodes = new JSONArray();
		json.put("nodes", nodes);
		List<Integer> operatorIDs = new ArrayList<Integer>(streamGraph.getVertexIDs());
		Collections.sort(operatorIDs, new Comparator<Integer>() {
			@Override
			public int compare(Integer idOne, Integer idTwo) {
				boolean isIdOneSinkId = streamGraph.getSinkIDs().contains(idOne);
				boolean isIdTwoSinkId = streamGraph.getSinkIDs().contains(idTwo);
				// put sinks at the back
				if (isIdOneSinkId == isIdTwoSinkId) {
					return idOne.compareTo(idTwo);
				} else if (isIdOneSinkId) {
					return 1;
				} else {
					return -1;
				}
			}
		});
		visit(nodes, operatorIDs, new HashMap<Integer, Integer>());
		return json.toString();
	}

	private void visit(JSONArray jsonArray, List<Integer> toVisit,
			Map<Integer, Integer> edgeRemapings) throws JSONException {

		Integer vertexID = toVisit.get(0);
		StreamNode vertex = streamGraph.getStreamNode(vertexID);

		if (streamGraph.getSourceIDs().contains(vertexID)
				|| Collections.disjoint(vertex.getInEdges(), toVisit)) {

			JSONObject node = new JSONObject();
			decorateNode(vertexID, node);

			if (!streamGraph.getSourceIDs().contains(vertexID)) {
				JSONArray inputs = new JSONArray();
				node.put(PREDECESSORS, inputs);

				for (StreamEdge inEdge : vertex.getInEdges()) {
					int inputID = inEdge.getSourceId();

					Integer mappedID = (edgeRemapings.keySet().contains(inputID)) ? edgeRemapings
							.get(inputID) : inputID;
					decorateEdge(inputs, inEdge, mappedID);
				}
			}
			jsonArray.put(node);
			toVisit.remove(vertexID);
		} else {
			Integer iterationHead = -1;
			for (StreamEdge inEdge : vertex.getInEdges()) {
				int operator = inEdge.getSourceId();

				if (streamGraph.vertexIDtoLoopTimeout.containsKey(operator)) {
					iterationHead = operator;
				}
			}

			JSONObject obj = new JSONObject();
			JSONArray iterationSteps = new JSONArray();
			obj.put(STEPS, iterationSteps);
			obj.put(ID, iterationHead);
			obj.put(PACT, "IterativeDataStream");
			obj.put(PARALLELISM, streamGraph.getStreamNode(iterationHead).getParallelism());
			obj.put(CONTENTS, "Stream Iteration");
			JSONArray iterationInputs = new JSONArray();
			obj.put(PREDECESSORS, iterationInputs);
			toVisit.remove(iterationHead);
			visitIteration(iterationSteps, toVisit, iterationHead, edgeRemapings, iterationInputs);
			jsonArray.put(obj);
		}

		if (!toVisit.isEmpty()) {
			visit(jsonArray, toVisit, edgeRemapings);
		}
	}

	private void visitIteration(JSONArray jsonArray, List<Integer> toVisit, int headId,
			Map<Integer, Integer> edgeRemapings, JSONArray iterationInEdges) throws JSONException {

		Integer vertexID = toVisit.get(0);
		StreamNode vertex = streamGraph.getStreamNode(vertexID);
		toVisit.remove(vertexID);

		// Ignoring head and tail to avoid redundancy
		if (!streamGraph.vertexIDtoLoopTimeout.containsKey(vertexID)) {
			JSONObject obj = new JSONObject();
			jsonArray.put(obj);
			decorateNode(vertexID, obj);
			JSONArray inEdges = new JSONArray();
			obj.put(PREDECESSORS, inEdges);

			for (StreamEdge inEdge : vertex.getInEdges()) {
				int inputID = inEdge.getSourceId();

				if (edgeRemapings.keySet().contains(inputID)) {
					decorateEdge(inEdges, inEdge, inputID);
				} else if (!streamGraph.vertexIDtoLoopTimeout.containsKey(inputID)) {
					decorateEdge(iterationInEdges, inEdge, inputID);
				}
			}

			edgeRemapings.put(vertexID, headId);
			visitIteration(jsonArray, toVisit, headId, edgeRemapings, iterationInEdges);
		}

	}

	private void decorateEdge(JSONArray inputArray, StreamEdge inEdge, int mappedInputID)
			throws JSONException {
		JSONObject input = new JSONObject();
		inputArray.put(input);
		input.put(ID, mappedInputID);
		input.put(SHIP_STRATEGY, inEdge.getPartitioner());
		input.put(SIDE, (inputArray.length() == 0) ? "first" : "second");
	}

	private void decorateNode(Integer vertexID, JSONObject node) throws JSONException {

		StreamNode vertex = streamGraph.getStreamNode(vertexID);

		node.put(ID, vertexID);
		node.put(TYPE, vertex.getOperatorName());

		if (streamGraph.getSourceIDs().contains(vertexID)) {
			node.put(PACT, "Data Source");
		} else if (streamGraph.getSinkIDs().contains(vertexID)) {
			node.put(PACT, "Data Sink");
		} else {
			node.put(PACT, "Operator");
		}

		StreamOperator<?> operator = streamGraph.getStreamNode(vertexID).getOperator();

		node.put(CONTENTS, vertex.getOperatorName());

		node.put(PARALLELISM, streamGraph.getStreamNode(vertexID).getParallelism());
	}

}
