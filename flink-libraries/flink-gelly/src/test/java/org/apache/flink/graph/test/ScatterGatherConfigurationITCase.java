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

package org.apache.flink.graph.test;

import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;
import org.apache.flink.graph.spargel.ScatterGatherIteration;
import org.apache.flink.graph.utils.VertexToTuple2Map;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.LongValue;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashSet;
import java.util.List;

/** Tests for {@link ScatterGatherConfiguration}. */
@RunWith(Parameterized.class)
public class ScatterGatherConfigurationITCase extends MultipleProgramsTestBase {

    public ScatterGatherConfigurationITCase(TestExecutionMode mode) {
        super(mode);
    }

    private String expectedResult;

    @Test
    public void testRunWithConfiguration() throws Exception {
        /*
         * Test Graph's runScatterGatherIteration when configuration parameters are provided
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromCollection(
                                TestGraphUtils.getLongLongVertices(),
                                TestGraphUtils.getLongLongEdges(),
                                env)
                        .mapVertices(new AssignOneMapper());

        // create the configuration object
        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();

        parameters.addBroadcastSetForScatterFunction(
                "messagingBcastSet", env.fromElements(4, 5, 6));
        parameters.addBroadcastSetForGatherFunction("updateBcastSet", env.fromElements(1, 2, 3));
        parameters.registerAggregator("superstepAggregator", new LongSumAggregator());
        parameters.setOptNumVertices(true);

        Graph<Long, Long, Long> res =
                graph.runScatterGatherIteration(
                        new MessageFunction(), new UpdateFunction(), 10, parameters);

        DataSet<Vertex<Long, Long>> data = res.getVertices();
        List<Vertex<Long, Long>> result = data.collect();

        expectedResult = "1,11\n" + "2,11\n" + "3,11\n" + "4,11\n" + "5,11";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testIterationConfiguration() throws Exception {
        /*
         * Test name, parallelism and solutionSetUnmanaged parameters
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ScatterGatherIteration<Long, Long, Long, Long> iteration =
                ScatterGatherIteration.withEdges(
                        TestGraphUtils.getLongLongEdgeData(env),
                        new DummyMessageFunction(),
                        new DummyUpdateFunction(),
                        10);

        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();
        parameters.setName("gelly iteration");
        parameters.setParallelism(2);
        parameters.setSolutionSetUnmanagedMemory(true);

        iteration.configure(parameters);

        Assert.assertEquals("gelly iteration", iteration.getIterationConfiguration().getName(""));
        Assert.assertEquals(2, iteration.getIterationConfiguration().getParallelism());
        Assert.assertEquals(
                true, iteration.getIterationConfiguration().isSolutionSetUnmanagedMemory());

        DataSet<Vertex<Long, Long>> data =
                TestGraphUtils.getLongLongVertexData(env).runOperation(iteration);
        List<Vertex<Long, Long>> result = data.collect();

        expectedResult = "1,11\n" + "2,12\n" + "3,13\n" + "4,14\n" + "5,15";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testDefaultConfiguration() throws Exception {
        /*
         * Test Graph's runScatterGatherIteration when configuration parameters are not provided
         * i.e. degrees and numVertices will be -1, EdgeDirection will be OUT.
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromCollection(
                                TestGraphUtils.getLongLongVertices(),
                                TestGraphUtils.getLongLongEdges(),
                                env)
                        .mapVertices(new AssignOneMapper());

        Graph<Long, Long, Long> res =
                graph.runScatterGatherIteration(
                        new MessageFunctionDefault(), new UpdateFunctionDefault(), 5);

        DataSet<Tuple2<Long, Long>> data = res.getVertices().map(new VertexToTuple2Map<>());
        List<Tuple2<Long, Long>> result = data.collect();

        expectedResult = "1,6\n" + "2,6\n" + "3,6\n" + "4,6\n" + "5,6";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testIterationDefaultDirection() throws Exception {
        /*
         * Test that if no direction parameter is given, the iteration works as before
         * (i.e. it collects messages from the in-neighbors and sends them to the out-neighbors)
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, HashSet<Long>, Long> graph =
                Graph.fromCollection(
                                TestGraphUtils.getLongLongVertices(),
                                TestGraphUtils.getLongLongEdges(),
                                env)
                        .mapVertices(new InitialiseHashSetMapper());

        DataSet<Vertex<Long, HashSet<Long>>> resultedVertices =
                graph.runScatterGatherIteration(
                                new IdMessengerTrg(), new VertexUpdateDirection(), 5)
                        .getVertices();

        List<Vertex<Long, HashSet<Long>>> result = resultedVertices.collect();

        expectedResult = "1,[5]\n" + "2,[1]\n" + "3,[1, 2]\n" + "4,[3]\n" + "5,[3, 4]";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testIterationINDirection() throws Exception {
        /*
         * Test that if the direction parameter is set to IN,
         * messages are collected from the out-neighbors and sent to the in-neighbors.
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, HashSet<Long>, Long> graph =
                Graph.fromCollection(
                                TestGraphUtils.getLongLongVertices(),
                                TestGraphUtils.getLongLongEdges(),
                                env)
                        .mapVertices(new InitialiseHashSetMapper());

        // configure the iteration
        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();

        parameters.setDirection(EdgeDirection.IN);

        DataSet<Vertex<Long, HashSet<Long>>> resultedVertices =
                graph.runScatterGatherIteration(
                                new IdMessengerSrc(), new VertexUpdateDirection(), 5, parameters)
                        .getVertices();

        List<Vertex<Long, HashSet<Long>>> result = resultedVertices.collect();

        expectedResult = "1,[2, 3]\n" + "2,[3]\n" + "3,[4, 5]\n" + "4,[5]\n" + "5,[1]";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testIterationALLDirection() throws Exception {
        /*
         * Test that if the direction parameter is set to ALL,
         * messages are collected from all the neighbors and sent to all the neighbors.
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, HashSet<Long>, Long> graph =
                Graph.fromCollection(
                                TestGraphUtils.getLongLongVertices(),
                                TestGraphUtils.getLongLongEdges(),
                                env)
                        .mapVertices(new InitialiseHashSetMapper());

        // configure the iteration
        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();

        parameters.setDirection(EdgeDirection.ALL);

        DataSet<Vertex<Long, HashSet<Long>>> resultedVertices =
                graph.runScatterGatherIteration(
                                new IdMessengerAll(), new VertexUpdateDirection(), 5, parameters)
                        .getVertices();

        List<Vertex<Long, HashSet<Long>>> result = resultedVertices.collect();

        expectedResult =
                "1,[2, 3, 5]\n" + "2,[1, 3]\n" + "3,[1, 2, 4, 5]\n" + "4,[3, 5]\n" + "5,[1, 3, 4]";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testSendToAllDirectionIN() throws Exception {
        /*
         * Test that sendMessageToAllNeighbors() works correctly
         * when the direction is set to IN
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, HashSet<Long>, Long> graph =
                Graph.fromCollection(
                                TestGraphUtils.getLongLongVertices(),
                                TestGraphUtils.getLongLongEdges(),
                                env)
                        .mapVertices(new InitialiseHashSetMapper());

        // configure the iteration
        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();

        parameters.setDirection(EdgeDirection.IN);

        DataSet<Vertex<Long, HashSet<Long>>> resultedVertices =
                graph.runScatterGatherIteration(
                                new SendMsgToAll(), new VertexUpdateDirection(), 5, parameters)
                        .getVertices();

        List<Vertex<Long, HashSet<Long>>> result = resultedVertices.collect();

        expectedResult = "1,[2, 3]\n" + "2,[3]\n" + "3,[4, 5]\n" + "4,[5]\n" + "5,[1]";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testSendToAllDirectionOUT() throws Exception {
        /*
         * Test that sendMessageToAllNeighbors() works correctly
         * when the direction is set to OUT
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, HashSet<Long>, Long> graph =
                Graph.fromCollection(
                                TestGraphUtils.getLongLongVertices(),
                                TestGraphUtils.getLongLongEdges(),
                                env)
                        .mapVertices(new InitialiseHashSetMapper());

        // configure the iteration
        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();

        parameters.setDirection(EdgeDirection.OUT);

        DataSet<Vertex<Long, HashSet<Long>>> resultedVertices =
                graph.runScatterGatherIteration(
                                new SendMsgToAll(), new VertexUpdateDirection(), 5, parameters)
                        .getVertices();

        List<Vertex<Long, HashSet<Long>>> result = resultedVertices.collect();

        expectedResult = "1,[5]\n" + "2,[1]\n" + "3,[1, 2]\n" + "4,[3]\n" + "5,[3, 4]";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testSendToAllDirectionALL() throws Exception {
        /*
         * Test that sendMessageToAllNeighbors() works correctly
         * when the direction is set to ALL
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, HashSet<Long>, Long> graph =
                Graph.fromCollection(
                                TestGraphUtils.getLongLongVertices(),
                                TestGraphUtils.getLongLongEdges(),
                                env)
                        .mapVertices(new InitialiseHashSetMapper());

        // configure the iteration
        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();

        parameters.setDirection(EdgeDirection.ALL);

        DataSet<Vertex<Long, HashSet<Long>>> resultedVertices =
                graph.runScatterGatherIteration(
                                new SendMsgToAll(), new VertexUpdateDirection(), 5, parameters)
                        .getVertices();

        List<Vertex<Long, HashSet<Long>>> result = resultedVertices.collect();

        expectedResult =
                "1,[2, 3, 5]\n" + "2,[1, 3]\n" + "3,[1, 2, 4, 5]\n" + "4,[3, 5]\n" + "5,[1, 3, 4]";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testNumVerticesNotSet() throws Exception {

        /*
         * Test that if the number of vertices option is not set, -1 is returned as value.
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromCollection(
                        TestGraphUtils.getLongLongVertices(),
                        TestGraphUtils.getLongLongEdges(),
                        env);

        DataSet<Vertex<Long, Long>> verticesWithNumVertices =
                graph.runScatterGatherIteration(
                                new DummyMessageFunction(), new UpdateFunctionNumVertices(), 2)
                        .getVertices();

        List<Vertex<Long, Long>> result = verticesWithNumVertices.collect();

        expectedResult = "1,-1\n" + "2,-1\n" + "3,-1\n" + "4,-1\n" + "5,-1";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testInDegreesSet() throws Exception {
        /*
         * Test that if the degrees are set, they can be accessed in every superstep
         * inside the update function and the value
         * is correctly computed for degrees in the scatter function.
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromCollection(
                        TestGraphUtils.getLongLongVertices(),
                        TestGraphUtils.getLongLongEdges(),
                        env);

        // configure the iteration
        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();

        parameters.setOptDegrees(true);

        DataSet<Vertex<Long, Long>> verticesWithDegrees =
                graph.runScatterGatherIteration(
                                new DegreesMessageFunction(),
                                new UpdateFunctionInDegrees(),
                                5,
                                parameters)
                        .getVertices();

        List<Vertex<Long, Long>> result = verticesWithDegrees.collect();

        expectedResult = "1,1\n" + "2,1\n" + "3,2\n" + "4,1\n" + "5,2";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testInDegreesNotSet() throws Exception {
        /*
         * Test that if the degrees option is not set, then -1 is returned as a value for in-degree.
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromCollection(
                        TestGraphUtils.getLongLongVertices(),
                        TestGraphUtils.getLongLongEdges(),
                        env);

        DataSet<Vertex<Long, Long>> verticesWithDegrees =
                graph.runScatterGatherIteration(
                                new DummyMessageFunction(), new UpdateFunctionInDegrees(), 2)
                        .getVertices();

        List<Vertex<Long, Long>> result = verticesWithDegrees.collect();

        expectedResult = "1,-1\n" + "2,-1\n" + "3,-1\n" + "4,-1\n" + "5,-1";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testOutDegreesSet() throws Exception {
        /*
         * Test that if the degrees are set, they can be accessed in every superstep
         * inside the update function and the value
         * is correctly computed for degrees in the scatter function.
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromCollection(
                        TestGraphUtils.getLongLongVertices(),
                        TestGraphUtils.getLongLongEdges(),
                        env);

        // configure the iteration
        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();

        parameters.setOptDegrees(true);

        DataSet<Vertex<Long, Long>> verticesWithDegrees =
                graph.runScatterGatherIteration(
                                new DegreesMessageFunction(),
                                new UpdateFunctionOutDegrees(),
                                5,
                                parameters)
                        .getVertices();

        List<Vertex<Long, Long>> result = verticesWithDegrees.collect();

        expectedResult = "1,2\n" + "2,1\n" + "3,2\n" + "4,1\n" + "5,1";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testOutDegreesNotSet() throws Exception {
        /*
         * Test that if the degrees option is not set, then -1 is returned as a value for out-degree.
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromCollection(
                        TestGraphUtils.getLongLongVertices(),
                        TestGraphUtils.getLongLongEdges(),
                        env);

        DataSet<Vertex<Long, Long>> verticesWithDegrees =
                graph.runScatterGatherIteration(
                                new DummyMessageFunction(), new UpdateFunctionOutDegrees(), 2)
                        .getVertices();

        List<Vertex<Long, Long>> result = verticesWithDegrees.collect();

        expectedResult = "1,-1\n" + "2,-1\n" + "3,-1\n" + "4,-1\n" + "5,-1";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testDirectionALLAndDegrees() throws Exception {
        /*
         * Compute the number of neighbors in a vertex - centric manner, and verify that it is equal to
         * the sum: inDegree + outDegree.
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Boolean, Long> graph =
                Graph.fromCollection(
                        TestGraphUtils.getLongBooleanVertices(),
                        TestGraphUtils.getLongLongEdges(),
                        env);

        // configure the iteration
        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();

        parameters.setOptDegrees(true);
        parameters.setDirection(EdgeDirection.ALL);

        DataSet<Vertex<Long, Boolean>> verticesWithNumNeighbors =
                graph.runScatterGatherIteration(
                                new IdMessenger(), new VertexUpdateNumNeighbors(), 1, parameters)
                        .getVertices();

        List<Vertex<Long, Boolean>> result = verticesWithNumNeighbors.collect();

        expectedResult = "1,true\n" + "2,true\n" + "3,true\n" + "4,true\n" + "5,true";

        compareResultAsTuples(result, expectedResult);
    }

    @SuppressWarnings("serial")
    private static final class MessageFunction extends ScatterFunction<Long, Long, Long, Long> {

        @Override
        public void preSuperstep() {

            // test bcast variable
            @SuppressWarnings("unchecked")
            List<Integer> bcastSet = (List<Integer>) (List<?>) getBroadcastSet("messagingBcastSet");
            Assert.assertEquals(4, bcastSet.get(0).intValue());
            Assert.assertEquals(5, bcastSet.get(1).intValue());
            Assert.assertEquals(6, bcastSet.get(2).intValue());

            // test number of vertices
            Assert.assertEquals(5, getNumberOfVertices());

            // test aggregator
            if (getSuperstepNumber() == 2) {
                long aggrValue =
                        ((LongValue) getPreviousIterationAggregate("superstepAggregator"))
                                .getValue();
                Assert.assertEquals(5, aggrValue);
            }
        }

        @Override
        public void sendMessages(Vertex<Long, Long> vertex) {
            // send message to keep vertices active
            sendMessageToAllNeighbors(vertex.getValue());
        }
    }

    @SuppressWarnings("serial")
    private static final class MessageFunctionDefault
            extends ScatterFunction<Long, Long, Long, Long> {

        @Override
        public void sendMessages(Vertex<Long, Long> vertex) {
            // test number of vertices
            Assert.assertEquals(-1, getNumberOfVertices());

            // test degrees
            Assert.assertEquals(-1, getInDegree());
            Assert.assertEquals(-1, getOutDegree());
            // send message to keep vertices active
            sendMessageToAllNeighbors(vertex.getValue());
        }
    }

    @SuppressWarnings("serial")
    private static final class UpdateFunction extends GatherFunction<Long, Long, Long> {

        LongSumAggregator aggregator = new LongSumAggregator();

        @Override
        public void preSuperstep() {

            // test bcast variable
            @SuppressWarnings("unchecked")
            List<Integer> bcastSet = (List<Integer>) (List<?>) getBroadcastSet("updateBcastSet");
            Assert.assertEquals(1, bcastSet.get(0).intValue());
            Assert.assertEquals(2, bcastSet.get(1).intValue());
            Assert.assertEquals(3, bcastSet.get(2).intValue());

            // test aggregator
            aggregator = getIterationAggregator("superstepAggregator");

            // test number of vertices
            Assert.assertEquals(5, getNumberOfVertices());
        }

        @Override
        public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) {
            long superstep = getSuperstepNumber();
            aggregator.aggregate(superstep);

            setNewVertexValue(vertex.getValue() + 1);
        }
    }

    @SuppressWarnings("serial")
    private static final class UpdateFunctionDefault extends GatherFunction<Long, Long, Long> {

        @Override
        public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) {

            // test number of vertices
            Assert.assertEquals(-1, getNumberOfVertices());

            // test degrees
            Assert.assertEquals(-1, getInDegree());
            Assert.assertEquals(-1, getOutDegree());

            setNewVertexValue(vertex.getValue() + 1);
        }
    }

    @SuppressWarnings("serial")
    private static final class UpdateFunctionNumVertices extends GatherFunction<Long, Long, Long> {

        @Override
        public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) {
            setNewVertexValue(getNumberOfVertices());
        }
    }

    @SuppressWarnings("serial")
    private static final class DummyUpdateFunction extends GatherFunction<Long, Long, Long> {

        @Override
        public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) {
            setNewVertexValue(vertex.getValue() + 1);
        }
    }

    @SuppressWarnings("serial")
    private static final class DummyMessageFunction
            extends ScatterFunction<Long, Long, Long, Long> {
        @Override
        public void sendMessages(Vertex<Long, Long> vertex) {
            // send message to keep vertices active
            sendMessageToAllNeighbors(vertex.getValue());
        }
    }

    @SuppressWarnings("serial")
    private static final class DegreesMessageFunction
            extends ScatterFunction<Long, Long, Long, Long> {

        @Override
        public void sendMessages(Vertex<Long, Long> vertex) {
            if (vertex.getId() == 1) {
                Assert.assertEquals(2, getOutDegree());
                Assert.assertEquals(1, getInDegree());
            } else if (vertex.getId() == 3) {
                Assert.assertEquals(2, getOutDegree());
                Assert.assertEquals(2, getInDegree());
            }
            // send message to keep vertices active
            sendMessageToAllNeighbors(vertex.getValue());
        }
    }

    @SuppressWarnings("serial")
    private static final class VertexUpdateDirection
            extends GatherFunction<Long, HashSet<Long>, Long> {

        @Override
        public void updateVertex(Vertex<Long, HashSet<Long>> vertex, MessageIterator<Long> messages)
                throws Exception {
            vertex.getValue().clear();

            for (long msg : messages) {
                vertex.getValue().add(msg);
            }

            setNewVertexValue(vertex.getValue());
        }
    }

    @SuppressWarnings("serial")
    private static final class UpdateFunctionInDegrees extends GatherFunction<Long, Long, Long> {

        @Override
        public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) {
            long inDegree = getInDegree();
            setNewVertexValue(inDegree);
        }
    }

    @SuppressWarnings("serial")
    private static final class UpdateFunctionOutDegrees extends GatherFunction<Long, Long, Long> {

        @Override
        public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) {
            long outDegree = getOutDegree();
            setNewVertexValue(outDegree);
        }
    }

    @SuppressWarnings("serial")
    private static final class VertexUpdateNumNeighbors
            extends GatherFunction<Long, Boolean, Long> {

        @Override
        public void updateVertex(Vertex<Long, Boolean> vertex, MessageIterator<Long> messages)
                throws Exception {

            long count = 0;

            for (@SuppressWarnings("unused") long msg : messages) {
                count++;
            }
            setNewVertexValue(count == (getInDegree() + getOutDegree()));
        }
    }

    @SuppressWarnings("serial")
    private static final class UpdateFunctionDegrees extends GatherFunction<Long, Long, Long> {

        @Override
        public void updateVertex(Vertex<Long, Long> vertex, MessageIterator<Long> inMessages) {
            long inDegree = getInDegree();
            long outDegree = getOutDegree();
            setNewVertexValue(inDegree + outDegree);
        }
    }

    @SuppressWarnings("serial")
    private static final class IdMessengerSrc
            extends ScatterFunction<Long, HashSet<Long>, Long, Long> {

        @Override
        public void sendMessages(Vertex<Long, HashSet<Long>> vertex) throws Exception {
            for (Edge<Long, Long> edge : getEdges()) {
                sendMessageTo(edge.getSource(), vertex.getId());
            }
        }
    }

    @SuppressWarnings("serial")
    private static final class IdMessengerAll
            extends ScatterFunction<Long, HashSet<Long>, Long, Long> {

        @Override
        public void sendMessages(Vertex<Long, HashSet<Long>> vertex) throws Exception {
            for (Edge<Long, Long> edge : getEdges()) {
                if (!edge.getSource().equals(vertex.getId())) {
                    sendMessageTo(edge.getSource(), vertex.getId());
                } else {
                    sendMessageTo(edge.getTarget(), vertex.getId());
                }
            }
        }
    }

    @SuppressWarnings("serial")
    private static final class SendMsgToAll
            extends ScatterFunction<Long, HashSet<Long>, Long, Long> {

        @Override
        public void sendMessages(Vertex<Long, HashSet<Long>> vertex) throws Exception {
            sendMessageToAllNeighbors(vertex.getId());
        }
    }

    @SuppressWarnings("serial")
    private static final class IdMessenger extends ScatterFunction<Long, Boolean, Long, Long> {

        @Override
        public void sendMessages(Vertex<Long, Boolean> vertex) throws Exception {
            for (Edge<Long, Long> edge : getEdges()) {
                if (!edge.getSource().equals(vertex.getId())) {
                    sendMessageTo(edge.getSource(), vertex.getId());
                } else {
                    sendMessageTo(edge.getTarget(), vertex.getId());
                }
            }
        }
    }

    @SuppressWarnings("serial")
    private static final class IdMessengerTrg
            extends ScatterFunction<Long, HashSet<Long>, Long, Long> {

        @Override
        public void sendMessages(Vertex<Long, HashSet<Long>> vertex) throws Exception {
            for (Edge<Long, Long> edge : getEdges()) {
                sendMessageTo(edge.getTarget(), vertex.getId());
            }
        }
    }

    @SuppressWarnings("serial")
    private static final class AssignOneMapper implements MapFunction<Vertex<Long, Long>, Long> {

        public Long map(Vertex<Long, Long> value) {
            return 1L;
        }
    }

    @SuppressWarnings("serial")
    private static final class InitialiseHashSetMapper
            implements MapFunction<Vertex<Long, Long>, HashSet<Long>> {

        @Override
        public HashSet<Long> map(Vertex<Long, Long> value) throws Exception {
            return new HashSet<>();
        }
    }
}
