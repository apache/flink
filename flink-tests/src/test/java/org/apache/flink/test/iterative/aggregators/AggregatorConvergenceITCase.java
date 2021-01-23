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

package org.apache.flink.test.iterative.aggregators;

import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Connected Components test case that uses a parameterizable convergence criterion. */
@RunWith(Parameterized.class)
@SuppressWarnings("serial")
public class AggregatorConvergenceITCase extends MultipleProgramsTestBase {

    public AggregatorConvergenceITCase(TestExecutionMode mode) {
        super(mode);
    }

    final List<Tuple2<Long, Long>> verticesInput =
            Arrays.asList(
                    new Tuple2<>(1L, 1L),
                    new Tuple2<>(2L, 2L),
                    new Tuple2<>(3L, 3L),
                    new Tuple2<>(4L, 4L),
                    new Tuple2<>(5L, 5L),
                    new Tuple2<>(6L, 6L),
                    new Tuple2<>(7L, 7L),
                    new Tuple2<>(8L, 8L),
                    new Tuple2<>(9L, 9L));

    final List<Tuple2<Long, Long>> edgesInput =
            Arrays.asList(
                    new Tuple2<>(1L, 2L),
                    new Tuple2<>(1L, 3L),
                    new Tuple2<>(2L, 3L),
                    new Tuple2<>(2L, 4L),
                    new Tuple2<>(2L, 1L),
                    new Tuple2<>(3L, 1L),
                    new Tuple2<>(3L, 2L),
                    new Tuple2<>(4L, 2L),
                    new Tuple2<>(4L, 6L),
                    new Tuple2<>(5L, 6L),
                    new Tuple2<>(6L, 4L),
                    new Tuple2<>(6L, 5L),
                    new Tuple2<>(7L, 8L),
                    new Tuple2<>(7L, 9L),
                    new Tuple2<>(8L, 7L),
                    new Tuple2<>(8L, 9L),
                    new Tuple2<>(9L, 7L),
                    new Tuple2<>(9L, 8L));

    final List<Tuple2<Long, Long>> expectedResult =
            Arrays.asList(
                    new Tuple2<>(1L, 1L),
                    new Tuple2<>(2L, 1L),
                    new Tuple2<>(3L, 1L),
                    new Tuple2<>(4L, 1L),
                    new Tuple2<>(5L, 2L),
                    new Tuple2<>(6L, 1L),
                    new Tuple2<>(7L, 7L),
                    new Tuple2<>(8L, 7L),
                    new Tuple2<>(9L, 7L));

    @Test
    public void testConnectedComponentsWithParametrizableConvergence() throws Exception {

        // name of the aggregator that checks for convergence
        final String updatedElements = "updated.elements.aggr";

        // the iteration stops if less than this number of elements change value
        final long convergenceThreshold = 3;

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Long, Long>> initialSolutionSet = env.fromCollection(verticesInput);
        DataSet<Tuple2<Long, Long>> edges = env.fromCollection(edgesInput);

        IterativeDataSet<Tuple2<Long, Long>> iteration = initialSolutionSet.iterate(10);

        // register the convergence criterion
        iteration.registerAggregationConvergenceCriterion(
                updatedElements,
                new LongSumAggregator(),
                new UpdatedElementsConvergenceCriterion(convergenceThreshold));

        DataSet<Tuple2<Long, Long>> verticesWithNewComponents =
                iteration
                        .join(edges)
                        .where(0)
                        .equalTo(0)
                        .with(new NeighborWithComponentIDJoin())
                        .groupBy(0)
                        .min(1);

        DataSet<Tuple2<Long, Long>> updatedComponentId =
                verticesWithNewComponents
                        .join(iteration)
                        .where(0)
                        .equalTo(0)
                        .flatMap(new MinimumIdFilter(updatedElements));

        List<Tuple2<Long, Long>> result = iteration.closeWith(updatedComponentId).collect();
        Collections.sort(result, new TestBaseUtils.TupleComparator<Tuple2<Long, Long>>());

        assertEquals(expectedResult, result);
    }

    @Test
    public void testDeltaConnectedComponentsWithParametrizableConvergence() throws Exception {

        // name of the aggregator that checks for convergence
        final String updatedElements = "updated.elements.aggr";

        // the iteration stops if less than this number of elements change value
        final long convergenceThreshold = 3;

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Long, Long>> initialSolutionSet = env.fromCollection(verticesInput);
        DataSet<Tuple2<Long, Long>> edges = env.fromCollection(edgesInput);

        DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
                initialSolutionSet.iterateDelta(initialSolutionSet, 10, 0);

        // register the convergence criterion
        iteration.registerAggregationConvergenceCriterion(
                updatedElements,
                new LongSumAggregator(),
                new UpdatedElementsConvergenceCriterion(convergenceThreshold));

        DataSet<Tuple2<Long, Long>> verticesWithNewComponents =
                iteration
                        .getWorkset()
                        .join(edges)
                        .where(0)
                        .equalTo(0)
                        .with(new NeighborWithComponentIDJoin())
                        .groupBy(0)
                        .min(1);

        DataSet<Tuple2<Long, Long>> updatedComponentId =
                verticesWithNewComponents
                        .join(iteration.getSolutionSet())
                        .where(0)
                        .equalTo(0)
                        .flatMap(new MinimumIdFilter(updatedElements));

        List<Tuple2<Long, Long>> result =
                iteration.closeWith(updatedComponentId, updatedComponentId).collect();
        Collections.sort(result, new TestBaseUtils.TupleComparator<Tuple2<Long, Long>>());

        assertEquals(expectedResult, result);
    }

    @Test
    public void testParameterizableAggregator() throws Exception {

        final int maxIterations = 5;
        final String aggregatorName = "elements.in.component.aggregator";
        final long componentId = 1L;

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Long, Long>> initialSolutionSet = env.fromCollection(verticesInput);
        DataSet<Tuple2<Long, Long>> edges = env.fromCollection(edgesInput);

        IterativeDataSet<Tuple2<Long, Long>> iteration = initialSolutionSet.iterate(maxIterations);

        // register the aggregator
        iteration.registerAggregator(
                aggregatorName, new LongSumAggregatorWithParameter(componentId));

        DataSet<Tuple2<Long, Long>> verticesWithNewComponents =
                iteration
                        .join(edges)
                        .where(0)
                        .equalTo(0)
                        .with(new NeighborWithComponentIDJoin())
                        .groupBy(0)
                        .min(1);

        DataSet<Tuple2<Long, Long>> updatedComponentId =
                verticesWithNewComponents
                        .join(iteration)
                        .where(0)
                        .equalTo(0)
                        .flatMap(new MinimumIdFilterCounting(aggregatorName));

        List<Tuple2<Long, Long>> result = iteration.closeWith(updatedComponentId).collect();

        Collections.sort(result, new TestBaseUtils.TupleComparator<Tuple2<Long, Long>>());

        List<Tuple2<Long, Long>> expectedResult =
                Arrays.asList(
                        new Tuple2<>(1L, 1L),
                        new Tuple2<>(2L, 1L),
                        new Tuple2<>(3L, 1L),
                        new Tuple2<>(4L, 1L),
                        new Tuple2<>(5L, 1L),
                        new Tuple2<>(6L, 1L),
                        new Tuple2<>(7L, 7L),
                        new Tuple2<>(8L, 7L),
                        new Tuple2<>(9L, 7L));

        // check program result
        assertEquals(expectedResult, result);

        // check aggregators
        long[] aggrValues = MinimumIdFilterCounting.aggr_value;

        // note that position 0 has the end result from superstep 1, retrieved at the start of
        // iteration 2
        // position one as superstep 2, retrieved at the start of iteration 3.
        // the result from iteration 5 is not available, because no iteration 6 happens
        assertEquals(3, aggrValues[0]);
        assertEquals(4, aggrValues[1]);
        assertEquals(5, aggrValues[2]);
        assertEquals(6, aggrValues[3]);
    }

    // ------------------------------------------------------------------------
    //  Test Functions
    // ------------------------------------------------------------------------

    private static final class NeighborWithComponentIDJoin
            extends RichJoinFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Long, Long> join(
                Tuple2<Long, Long> vertexWithCompId, Tuple2<Long, Long> edge) {
            vertexWithCompId.f0 = edge.f1;
            return vertexWithCompId;
        }
    }

    private static class MinimumIdFilter
            extends RichFlatMapFunction<
                    Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

        private final String aggName;
        private LongSumAggregator aggr;

        public MinimumIdFilter(String aggName) {
            this.aggName = aggName;
        }

        @Override
        public void open(Configuration conf) {
            aggr = getIterationRuntimeContext().getIterationAggregator(aggName);
        }

        @Override
        public void flatMap(
                Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> vertexWithNewAndOldId,
                Collector<Tuple2<Long, Long>> out) {

            if (vertexWithNewAndOldId.f0.f1 < vertexWithNewAndOldId.f1.f1) {
                out.collect(vertexWithNewAndOldId.f0);
                aggr.aggregate(1L);
            } else {
                out.collect(vertexWithNewAndOldId.f1);
            }
        }
    }

    private static final class MinimumIdFilterCounting
            extends RichFlatMapFunction<
                    Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>, Tuple2<Long, Long>> {

        private static final long[] aggr_value = new long[5];

        private final String aggName;
        private LongSumAggregatorWithParameter aggr;

        public MinimumIdFilterCounting(String aggName) {
            this.aggName = aggName;
        }

        @Override
        public void open(Configuration conf) {
            final int superstep = getIterationRuntimeContext().getSuperstepNumber();

            aggr = getIterationRuntimeContext().getIterationAggregator(aggName);

            if (superstep > 1 && getIterationRuntimeContext().getIndexOfThisSubtask() == 0) {
                LongValue val = getIterationRuntimeContext().getPreviousIterationAggregate(aggName);
                aggr_value[superstep - 2] = val.getValue();
            }
        }

        @Override
        public void flatMap(
                Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>> vertexWithNewAndOldId,
                Collector<Tuple2<Long, Long>> out) {

            if (vertexWithNewAndOldId.f0.f1 < vertexWithNewAndOldId.f1.f1) {
                out.collect(vertexWithNewAndOldId.f0);
                if (vertexWithNewAndOldId.f0.f1 == aggr.getComponentId()) {
                    aggr.aggregate(1L);
                }
            } else {
                out.collect(vertexWithNewAndOldId.f1);
                if (vertexWithNewAndOldId.f1.f1 == aggr.getComponentId()) {
                    aggr.aggregate(1L);
                }
            }
        }
    }

    /** A Convergence Criterion with one parameter. */
    private static class UpdatedElementsConvergenceCriterion
            implements ConvergenceCriterion<LongValue> {

        private final long threshold;

        public UpdatedElementsConvergenceCriterion(long uThreshold) {
            this.threshold = uThreshold;
        }

        @Override
        public boolean isConverged(int iteration, LongValue value) {
            return value.getValue() < this.threshold;
        }
    }

    private static final class LongSumAggregatorWithParameter extends LongSumAggregator {

        private long componentId;

        public LongSumAggregatorWithParameter(long compId) {
            this.componentId = compId;
        }

        public long getComponentId() {
            return this.componentId;
        }
    }
}
