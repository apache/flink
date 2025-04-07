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

package org.apache.flink.runtime.jobgraph.jsonplan;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.util.JobVertexConnectionUtils.connectNewDataSetAsInput;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonGeneratorTest {

    @Test
    public void testGeneratorWithoutAnyAttachements() {
        try {
            JobVertex source1 = new JobVertex("source 1");

            JobVertex source2 = new JobVertex("source 2");
            source2.setInvokableClass(DummyInvokable.class);

            JobVertex source3 = new JobVertex("source 3");

            JobVertex intermediate1 = new JobVertex("intermediate 1");
            JobVertex intermediate2 = new JobVertex("intermediate 2");

            JobVertex join1 = new JobVertex("join 1");
            JobVertex join2 = new JobVertex("join 2");

            JobVertex sink1 = new JobVertex("sink 1");
            JobVertex sink2 = new JobVertex("sink 2");

            connectNewDataSetAsInput(
                    intermediate1,
                    source1,
                    DistributionPattern.POINTWISE,
                    ResultPartitionType.PIPELINED);
            connectNewDataSetAsInput(
                    intermediate2,
                    source2,
                    DistributionPattern.ALL_TO_ALL,
                    ResultPartitionType.PIPELINED);

            connectNewDataSetAsInput(
                    join1,
                    intermediate1,
                    DistributionPattern.POINTWISE,
                    ResultPartitionType.BLOCKING);
            connectNewDataSetAsInput(
                    join1,
                    intermediate2,
                    DistributionPattern.ALL_TO_ALL,
                    ResultPartitionType.BLOCKING);

            connectNewDataSetAsInput(
                    join2, join1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            connectNewDataSetAsInput(
                    join2, source3, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

            connectNewDataSetAsInput(
                    sink1, join2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
            connectNewDataSetAsInput(
                    sink2, join1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

            JobGraph jg =
                    JobGraphTestUtils.batchJobGraph(
                            source1,
                            source2,
                            source3,
                            intermediate1,
                            intermediate2,
                            join1,
                            join2,
                            sink1,
                            sink2);

            String plan = JsonPlanGenerator.generatePlan(jg);
            assertNotNull(plan);

            // validate the produced JSON
            ObjectMapper m = JacksonMapperFactory.createObjectMapper();
            JsonNode rootNode = m.readTree(plan);

            // core fields
            assertEquals(new TextNode(jg.getJobID().toString()), rootNode.get("jid"));
            assertEquals(new TextNode(jg.getName()), rootNode.get("name"));
            assertEquals(new TextNode(jg.getJobType().name()), rootNode.get("type"));

            assertTrue(rootNode.path("nodes").isArray());

            for (Iterator<JsonNode> iter = rootNode.path("nodes").elements(); iter.hasNext(); ) {
                JsonNode next = iter.next();

                JsonNode idNode = next.get("id");
                assertNotNull(idNode);
                assertTrue(idNode.isTextual());
                checkVertexExists(idNode.asText(), jg);

                String description = next.get("description").asText();
                assertTrue(
                        description.startsWith("source")
                                || description.startsWith("sink")
                                || description.startsWith("intermediate")
                                || description.startsWith("join"));
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private void checkVertexExists(String vertexId, JobGraph graph) {
        // validate that the vertex has a valid
        JobVertexID id = JobVertexID.fromHexString(vertexId);
        for (JobVertex vertex : graph.getVertices()) {
            if (vertex.getID().equals(id)) {
                return;
            }
        }
        fail("could not find vertex with id " + vertexId + " in JobGraph");
    }

    @Test
    public void testGenerateStreamGraphJson() throws JsonProcessingException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromSequence(0L, 1L).disableChaining().print();
        StreamGraph streamGraph = env.getStreamGraph();
        Map<Integer, JobVertexID> jobVertexIdMap = new HashMap<>();
        String streamGraphJson =
                JsonPlanGenerator.generateStreamGraphJson(streamGraph, jobVertexIdMap);

        ObjectMapper mapper = JacksonMapperFactory.createObjectMapper();
        StreamGraphJsonSchema parsedStreamGraph =
                mapper.readValue(streamGraphJson, StreamGraphJsonSchema.class);

        List<String> expectedJobVertexIds = new ArrayList<>();
        expectedJobVertexIds.add(null);
        expectedJobVertexIds.add(null);
        validateStreamGraph(streamGraph, parsedStreamGraph, expectedJobVertexIds);

        for (StreamNode node : streamGraph.getStreamNodes()) {
            jobVertexIdMap.put(node.getId(), new JobVertexID());
        }
        streamGraphJson = JsonPlanGenerator.generateStreamGraphJson(streamGraph, jobVertexIdMap);

        parsedStreamGraph = mapper.readValue(streamGraphJson, StreamGraphJsonSchema.class);
        validateStreamGraph(
                streamGraph,
                parsedStreamGraph,
                jobVertexIdMap.values().stream()
                        .map(JobVertexID::toString)
                        .collect(Collectors.toList()));
    }

    private static void validateStreamGraph(
            StreamGraph streamGraph,
            StreamGraphJsonSchema parsedStreamGraph,
            List<String> expectedJobVertexIds) {
        List<String> realJobVertexIds = new ArrayList<>();
        parsedStreamGraph
                .getNodes()
                .forEach(
                        node -> {
                            StreamNode streamNode =
                                    streamGraph.getStreamNode(Integer.parseInt(node.getId()));
                            assertEquals(node.getOperator(), streamNode.getOperatorName());
                            assertEquals(
                                    node.getParallelism(), (Integer) streamNode.getParallelism());
                            assertEquals(
                                    node.getDescription(), streamNode.getOperatorDescription());
                            validateStreamEdge(node.getInputs(), streamNode.getInEdges());
                            realJobVertexIds.add(node.getJobVertexId());
                        });
        assertEquals(expectedJobVertexIds, realJobVertexIds);
    }

    private static void validateStreamEdge(
            List<StreamGraphJsonSchema.JsonStreamEdgeSchema> jsonStreamEdges,
            List<StreamEdge> streamEdges) {
        assertEquals(jsonStreamEdges.size(), streamEdges.size());
        for (int i = 0; i < jsonStreamEdges.size(); i++) {
            StreamGraphJsonSchema.JsonStreamEdgeSchema edgeToValidate = jsonStreamEdges.get(i);
            StreamEdge expectedEdge = streamEdges.get(i);
            assertEquals(String.valueOf(expectedEdge.getSourceId()), edgeToValidate.getId());
            assertEquals(
                    expectedEdge.getPartitioner().toString(), edgeToValidate.getShipStrategy());
            assertEquals(expectedEdge.getExchangeMode().name(), edgeToValidate.getExchange());
        }
    }
}
