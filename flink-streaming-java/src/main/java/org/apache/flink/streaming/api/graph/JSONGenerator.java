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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Helper class for generating a JSON representation from a {@link StreamGraph}. */
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
    private final ObjectMapper mapper = new ObjectMapper();

    public JSONGenerator(StreamGraph streamGraph) {
        this.streamGraph = streamGraph;
    }

    public String getJSON() {
        ObjectNode json = mapper.createObjectNode();
        ArrayNode nodes = mapper.createArrayNode();
        json.put("nodes", nodes);

        List<Integer> operatorIDs = new ArrayList<>(streamGraph.getVertexIDs());
        Comparator<Integer> operatorIDComparator =
                Comparator.comparingInt(
                                (Integer id) -> streamGraph.getSinkIDs().contains(id) ? 1 : 0)
                        .thenComparingInt(id -> id);
        operatorIDs.sort(operatorIDComparator);

        visit(nodes, operatorIDs, new HashMap<>());

        return json.toPrettyString();
    }

    private void visit(
            ArrayNode jsonArray, List<Integer> toVisit, Map<Integer, Integer> edgeRemapings) {

        Integer vertexID = toVisit.get(0);
        StreamNode vertex = streamGraph.getStreamNode(vertexID);

        if (streamGraph.getSourceIDs().contains(vertexID)
                || Collections.disjoint(vertex.getInEdges(), toVisit)) {

            ObjectNode node = mapper.createObjectNode();
            decorateNode(vertexID, node);

            if (!streamGraph.getSourceIDs().contains(vertexID)) {
                ArrayNode inputs = mapper.createArrayNode();
                node.put(PREDECESSORS, inputs);

                for (StreamEdge inEdge : vertex.getInEdges()) {
                    int inputID = inEdge.getSourceId();

                    Integer mappedID =
                            (edgeRemapings.keySet().contains(inputID))
                                    ? edgeRemapings.get(inputID)
                                    : inputID;
                    decorateEdge(inputs, inEdge, mappedID);
                }
            }
            jsonArray.add(node);
            toVisit.remove(vertexID);
        } else {
            Integer iterationHead = -1;
            for (StreamEdge inEdge : vertex.getInEdges()) {
                int operator = inEdge.getSourceId();

                if (streamGraph.vertexIDtoLoopTimeout.containsKey(operator)) {
                    iterationHead = operator;
                }
            }

            ObjectNode obj = mapper.createObjectNode();
            ArrayNode iterationSteps = mapper.createArrayNode();
            obj.put(STEPS, iterationSteps);
            obj.put(ID, iterationHead);
            obj.put(PACT, "IterativeDataStream");
            obj.put(PARALLELISM, streamGraph.getStreamNode(iterationHead).getParallelism());
            obj.put(CONTENTS, "Stream Iteration");
            ArrayNode iterationInputs = mapper.createArrayNode();
            obj.put(PREDECESSORS, iterationInputs);
            toVisit.remove(iterationHead);
            visitIteration(iterationSteps, toVisit, iterationHead, edgeRemapings, iterationInputs);
            jsonArray.add(obj);
        }

        if (!toVisit.isEmpty()) {
            visit(jsonArray, toVisit, edgeRemapings);
        }
    }

    private void visitIteration(
            ArrayNode jsonArray,
            List<Integer> toVisit,
            int headId,
            Map<Integer, Integer> edgeRemapings,
            ArrayNode iterationInEdges) {

        Integer vertexID = toVisit.get(0);
        StreamNode vertex = streamGraph.getStreamNode(vertexID);
        toVisit.remove(vertexID);

        // Ignoring head and tail to avoid redundancy
        if (!streamGraph.vertexIDtoLoopTimeout.containsKey(vertexID)) {
            ObjectNode obj = mapper.createObjectNode();
            jsonArray.add(obj);
            decorateNode(vertexID, obj);
            ArrayNode inEdges = mapper.createArrayNode();
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

    private void decorateEdge(ArrayNode inputArray, StreamEdge inEdge, int mappedInputID) {
        ObjectNode input = mapper.createObjectNode();
        inputArray.add(input);
        input.put(ID, mappedInputID);
        input.put(SHIP_STRATEGY, inEdge.getPartitioner().toString());
        input.put(SIDE, (inputArray.size() == 0) ? "first" : "second");
    }

    private void decorateNode(Integer vertexID, ObjectNode node) {

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

        node.put(CONTENTS, vertex.getOperatorName());

        node.put(PARALLELISM, streamGraph.getStreamNode(vertexID).getParallelism());
    }
}
