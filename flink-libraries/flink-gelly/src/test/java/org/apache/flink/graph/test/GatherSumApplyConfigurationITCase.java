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
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GSAConfiguration;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.GatherSumApplyIteration;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.LongValue;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.HashSet;
import java.util.List;

/** Tests for {@link GSAConfiguration}. */
@RunWith(Parameterized.class)
public class GatherSumApplyConfigurationITCase extends MultipleProgramsTestBase {

    public GatherSumApplyConfigurationITCase(TestExecutionMode mode) {
        super(mode);
    }

    private String expectedResult;

    @Test
    public void testRunWithConfiguration() throws Exception {
        /*
         * Test Graph's runGatherSumApplyIteration when configuration parameters are provided
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromCollection(
                                TestGraphUtils.getLongLongVertices(),
                                TestGraphUtils.getLongLongEdges(),
                                env)
                        .mapVertices(new AssignOneMapper());

        // create the configuration object
        GSAConfiguration parameters = new GSAConfiguration();

        parameters.addBroadcastSetForGatherFunction("gatherBcastSet", env.fromElements(1, 2, 3));
        parameters.addBroadcastSetForSumFunction("sumBcastSet", env.fromElements(4, 5, 6));
        parameters.addBroadcastSetForApplyFunction("applyBcastSet", env.fromElements(7, 8, 9));
        parameters.registerAggregator("superstepAggregator", new LongSumAggregator());
        parameters.setOptNumVertices(true);

        Graph<Long, Long, Long> res =
                graph.runGatherSumApplyIteration(
                        new Gather(), new Sum(), new Apply(), 10, parameters);

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

        GatherSumApplyIteration<Long, Long, Long, Long> iteration =
                GatherSumApplyIteration.withEdges(
                        TestGraphUtils.getLongLongEdgeData(env),
                        new DummyGather(),
                        new DummySum(),
                        new DummyApply(),
                        10);

        GSAConfiguration parameters = new GSAConfiguration();
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
    public void testIterationDefaultDirection() throws Exception {
        /*
         * Test that if no direction parameter is given, the iteration works as before
         * (i.e. it gathers information from the IN edges and neighbors and the information is calculated for an OUT edge
         * Default direction parameter is OUT for the GatherSumApplyIterations)
         * When data is gathered from the IN edges the Gather Sum and Apply functions
         * set the set of vertices which have path to a vertex as the value of that vertex
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Edge<Long, Long>> edges = TestGraphUtils.getLongLongEdges();

        edges.remove(0);

        Graph<Long, HashSet<Long>, Long> graph =
                Graph.fromCollection(TestGraphUtils.getLongLongVertices(), edges, env)
                        .mapVertices(
                                new GatherSumApplyConfigurationITCase.InitialiseHashSetMapper());

        DataSet<Vertex<Long, HashSet<Long>>> resultedVertices =
                graph.runGatherSumApplyIteration(
                                new GetReachableVertices(),
                                new FindAllReachableVertices(),
                                new UpdateReachableVertices(),
                                4)
                        .getVertices();

        List<Vertex<Long, HashSet<Long>>> result = resultedVertices.collect();

        expectedResult =
                "1,[1, 2, 3, 4, 5]\n"
                        + "2,[2]\n"
                        + "3,[1, 2, 3, 4, 5]\n"
                        + "4,[1, 2, 3, 4, 5]\n"
                        + "5,[1, 2, 3, 4, 5]\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testIterationDirectionIN() throws Exception {
        /*
         * Test that if the direction parameter IN is given, the iteration works as expected
         * (i.e. it gathers information from the OUT edges and neighbors and the information is calculated for an IN edge
         * When data is gathered from the OUT edges the Gather Sum and Apply functions
         * set the set of vertices which have path from a vertex as the value of that vertex
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        GSAConfiguration parameters = new GSAConfiguration();

        parameters.setDirection(EdgeDirection.IN);

        List<Edge<Long, Long>> edges = TestGraphUtils.getLongLongEdges();

        edges.remove(0);

        Graph<Long, HashSet<Long>, Long> graph =
                Graph.fromCollection(TestGraphUtils.getLongLongVertices(), edges, env)
                        .mapVertices(
                                new GatherSumApplyConfigurationITCase.InitialiseHashSetMapper());

        DataSet<Vertex<Long, HashSet<Long>>> resultedVertices =
                graph.runGatherSumApplyIteration(
                                new GetReachableVertices(),
                                new FindAllReachableVertices(),
                                new UpdateReachableVertices(),
                                4,
                                parameters)
                        .getVertices();
        List<Vertex<Long, HashSet<Long>>> result = resultedVertices.collect();

        expectedResult =
                "1,[1, 3, 4, 5]\n"
                        + "2,[1, 2, 3, 4, 5]\n"
                        + "3,[1, 3, 4, 5]\n"
                        + "4,[1, 3, 4, 5]\n"
                        + "5,[1, 3, 4, 5]\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testIterationDirectionALL() throws Exception {
        /*
         * Test that if the direction parameter OUT is given, the iteration works as expected
         * (i.e. it gathers information from both IN and OUT edges and neighbors
         * When data is gathered from the ALL edges the Gather Sum and Apply functions
         * set the set of vertices which are connected to a Vertex through some path as value of that vertex
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        GSAConfiguration parameters = new GSAConfiguration();
        parameters.setDirection(EdgeDirection.ALL);

        List<Edge<Long, Long>> edges = TestGraphUtils.getLongLongEdges();

        edges.remove(0);

        Graph<Long, HashSet<Long>, Long> graph =
                Graph.fromCollection(TestGraphUtils.getLongLongVertices(), edges, env)
                        .mapVertices(
                                new GatherSumApplyConfigurationITCase.InitialiseHashSetMapper());

        DataSet<Vertex<Long, HashSet<Long>>> resultedVertices =
                graph.runGatherSumApplyIteration(
                                new GetReachableVertices(),
                                new FindAllReachableVertices(),
                                new UpdateReachableVertices(),
                                4,
                                parameters)
                        .getVertices();

        List<Vertex<Long, HashSet<Long>>> result = resultedVertices.collect();

        expectedResult =
                "1,[1, 2, 3, 4, 5]\n"
                        + "2,[1, 2, 3, 4, 5]\n"
                        + "3,[1, 2, 3, 4, 5]\n"
                        + "4,[1, 2, 3, 4, 5]\n"
                        + "5,[1, 2, 3, 4, 5]\n";

        compareResultAsTuples(result, expectedResult);
    }

    @SuppressWarnings("serial")
    private static final class Gather extends GatherFunction<Long, Long, Long> {

        @Override
        public void preSuperstep() {

            // test bcast variable
            @SuppressWarnings("unchecked")
            List<Integer> bcastSet = (List<Integer>) (List<?>) getBroadcastSet("gatherBcastSet");
            Assert.assertEquals(1, bcastSet.get(0).intValue());
            Assert.assertEquals(2, bcastSet.get(1).intValue());
            Assert.assertEquals(3, bcastSet.get(2).intValue());

            // test aggregator
            if (getSuperstepNumber() == 2) {
                long aggrValue =
                        ((LongValue) getPreviousIterationAggregate("superstepAggregator"))
                                .getValue();

                Assert.assertEquals(7, aggrValue);
            }

            // test number of vertices
            Assert.assertEquals(5, getNumberOfVertices());
        }

        public Long gather(Neighbor<Long, Long> neighbor) {
            return neighbor.getNeighborValue();
        }
    }

    @SuppressWarnings("serial")
    private static final class Sum extends SumFunction<Long, Long, Long> {

        LongSumAggregator aggregator = new LongSumAggregator();

        @Override
        public void preSuperstep() {

            // test bcast variable
            @SuppressWarnings("unchecked")
            List<Integer> bcastSet = (List<Integer>) (List<?>) getBroadcastSet("sumBcastSet");
            Assert.assertEquals(4, bcastSet.get(0).intValue());
            Assert.assertEquals(5, bcastSet.get(1).intValue());
            Assert.assertEquals(6, bcastSet.get(2).intValue());

            // test aggregator
            aggregator = getIterationAggregator("superstepAggregator");

            // test number of vertices
            Assert.assertEquals(5, getNumberOfVertices());
        }

        public Long sum(Long newValue, Long currentValue) {
            long superstep = getSuperstepNumber();
            aggregator.aggregate(superstep);
            return 0L;
        }
    }

    @SuppressWarnings("serial")
    private static final class Apply extends ApplyFunction<Long, Long, Long> {

        LongSumAggregator aggregator = new LongSumAggregator();

        @Override
        public void preSuperstep() {

            // test bcast variable
            @SuppressWarnings("unchecked")
            List<Integer> bcastSet = (List<Integer>) (List<?>) getBroadcastSet("applyBcastSet");
            Assert.assertEquals(7, bcastSet.get(0).intValue());
            Assert.assertEquals(8, bcastSet.get(1).intValue());
            Assert.assertEquals(9, bcastSet.get(2).intValue());

            // test aggregator
            aggregator = getIterationAggregator("superstepAggregator");

            // test number of vertices
            Assert.assertEquals(5, getNumberOfVertices());
        }

        public void apply(Long summedValue, Long origValue) {
            long superstep = getSuperstepNumber();
            aggregator.aggregate(superstep);
            setResult(origValue + 1);
        }
    }

    @SuppressWarnings("serial")
    private static final class DummyGather extends GatherFunction<Long, Long, Long> {

        @Override
        public void preSuperstep() {
            // test number of vertices
            // when the numVertices option is not set, -1 is returned
            Assert.assertEquals(-1, getNumberOfVertices());
        }

        public Long gather(Neighbor<Long, Long> neighbor) {
            return neighbor.getNeighborValue();
        }
    }

    @SuppressWarnings("serial")
    private static final class DummySum extends SumFunction<Long, Long, Long> {

        public Long sum(Long newValue, Long currentValue) {
            return 0L;
        }
    }

    @SuppressWarnings("serial")
    private static final class DummyApply extends ApplyFunction<Long, Long, Long> {

        public void apply(Long summedValue, Long origValue) {
            setResult(origValue + 1);
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
            HashSet<Long> h = new HashSet<>();
            h.add(value.getId());
            return h;
        }
    }

    @SuppressWarnings("serial")
    private static final class GetReachableVertices
            extends GatherFunction<HashSet<Long>, Long, HashSet<Long>> {

        @Override
        public HashSet<Long> gather(Neighbor<HashSet<Long>, Long> neighbor) {
            return neighbor.getNeighborValue();
        }
    }

    @SuppressWarnings("serial")
    private static final class FindAllReachableVertices
            extends SumFunction<HashSet<Long>, Long, HashSet<Long>> {
        @Override
        public HashSet<Long> sum(HashSet<Long> newSet, HashSet<Long> currentSet) {
            for (Long l : newSet) {
                currentSet.add(l);
            }
            return currentSet;
        }
    }

    @SuppressWarnings("serial")
    private static final class UpdateReachableVertices
            extends ApplyFunction<Long, HashSet<Long>, HashSet<Long>> {

        @Override
        public void apply(HashSet<Long> newValue, HashSet<Long> currentValue) {
            newValue.addAll(currentValue);
            if (newValue.size() > currentValue.size()) {
                setResult(newValue);
            }
        }
    }
}
