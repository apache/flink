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

package org.apache.flink.api.java.operators.translation;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.GenericDataSinkBase;
import org.apache.flink.api.common.operators.base.DeltaIterationBase;
import org.apache.flink.api.common.operators.base.InnerJoinOperatorBase;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Tests for translation of delta iterations. */
@SuppressWarnings("serial")
public class DeltaIterationTranslationTest implements java.io.Serializable {

    @Test
    public void testCorrectTranslation() {
        try {
            final String jobName = "Test JobName";
            final String iterationName = "Test Name";

            final String beforeNextWorksetMap = "Some Mapper";

            final String aggregatorName = "AggregatorName";

            final int[] iterationKeys = new int[] {2};
            final int numIterations = 13;

            final int defaultParallelism = 133;
            final int iterationParallelism = 77;

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            // ------------ construct the test program ------------------
            {
                env.setParallelism(defaultParallelism);

                @SuppressWarnings("unchecked")
                DataSet<Tuple3<Double, Long, String>> initialSolutionSet =
                        env.fromElements(new Tuple3<Double, Long, String>(3.44, 5L, "abc"));

                @SuppressWarnings("unchecked")
                DataSet<Tuple2<Double, String>> initialWorkSet =
                        env.fromElements(new Tuple2<Double, String>(1.23, "abc"));

                DeltaIteration<Tuple3<Double, Long, String>, Tuple2<Double, String>> iteration =
                        initialSolutionSet.iterateDelta(
                                initialWorkSet, numIterations, iterationKeys);
                iteration.name(iterationName).parallelism(iterationParallelism);

                iteration.registerAggregator(aggregatorName, new LongSumAggregator());

                // test that multiple workset consumers are supported
                DataSet<Tuple2<Double, String>> worksetSelfJoin =
                        iteration
                                .getWorkset()
                                .map(new IdentityMapper<Tuple2<Double, String>>())
                                .join(iteration.getWorkset())
                                .where(1)
                                .equalTo(1)
                                .projectFirst(0, 1);

                DataSet<Tuple3<Double, Long, String>> joined =
                        worksetSelfJoin
                                .join(iteration.getSolutionSet())
                                .where(1)
                                .equalTo(2)
                                .with(new SolutionWorksetJoin());

                DataSet<Tuple3<Double, Long, String>> result =
                        iteration.closeWith(
                                joined,
                                joined.map(new NextWorksetMapper()).name(beforeNextWorksetMap));

                result.output(new DiscardingOutputFormat<Tuple3<Double, Long, String>>());
                result.writeAsText("/dev/null");
            }

            Plan p = env.createProgramPlan(jobName);

            // ------------- validate the plan ----------------
            assertEquals(jobName, p.getJobName());
            assertEquals(defaultParallelism, p.getDefaultParallelism());

            // validate the iteration
            GenericDataSinkBase<?> sink1, sink2;
            {
                Iterator<? extends GenericDataSinkBase<?>> sinks = p.getDataSinks().iterator();
                sink1 = sinks.next();
                sink2 = sinks.next();
            }

            DeltaIterationBase<?, ?> iteration = (DeltaIterationBase<?, ?>) sink1.getInput();

            // check that multi consumer translation works for iterations
            assertEquals(iteration, sink2.getInput());

            // check the basic iteration properties
            assertEquals(numIterations, iteration.getMaximumNumberOfIterations());
            assertArrayEquals(iterationKeys, iteration.getSolutionSetKeyFields());
            assertEquals(iterationParallelism, iteration.getParallelism());
            assertEquals(iterationName, iteration.getName());

            MapOperatorBase<?, ?, ?> nextWorksetMapper =
                    (MapOperatorBase<?, ?, ?>) iteration.getNextWorkset();
            InnerJoinOperatorBase<?, ?, ?, ?> solutionSetJoin =
                    (InnerJoinOperatorBase<?, ?, ?, ?>) iteration.getSolutionSetDelta();
            InnerJoinOperatorBase<?, ?, ?, ?> worksetSelfJoin =
                    (InnerJoinOperatorBase<?, ?, ?, ?>) solutionSetJoin.getFirstInput();
            MapOperatorBase<?, ?, ?> worksetMapper =
                    (MapOperatorBase<?, ?, ?>) worksetSelfJoin.getFirstInput();

            assertEquals(
                    IdentityMapper.class, worksetMapper.getUserCodeWrapper().getUserCodeClass());
            assertEquals(
                    NextWorksetMapper.class,
                    nextWorksetMapper.getUserCodeWrapper().getUserCodeClass());
            if (solutionSetJoin.getUserCodeWrapper().getUserCodeObject()
                    instanceof WrappingFunction) {
                WrappingFunction<?> wf =
                        (WrappingFunction<?>)
                                solutionSetJoin.getUserCodeWrapper().getUserCodeObject();
                assertEquals(SolutionWorksetJoin.class, wf.getWrappedFunction().getClass());
            } else {
                assertEquals(
                        SolutionWorksetJoin.class,
                        solutionSetJoin.getUserCodeWrapper().getUserCodeClass());
            }

            assertEquals(beforeNextWorksetMap, nextWorksetMapper.getName());

            assertEquals(
                    aggregatorName,
                    iteration
                            .getAggregators()
                            .getAllRegisteredAggregators()
                            .iterator()
                            .next()
                            .getName());
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testRejectWhenSolutionSetKeysDontMatchJoin() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            @SuppressWarnings("unchecked")
            DataSet<Tuple3<Double, Long, String>> initialSolutionSet =
                    env.fromElements(new Tuple3<Double, Long, String>(3.44, 5L, "abc"));

            @SuppressWarnings("unchecked")
            DataSet<Tuple2<Double, String>> initialWorkSet =
                    env.fromElements(new Tuple2<Double, String>(1.23, "abc"));

            DeltaIteration<Tuple3<Double, Long, String>, Tuple2<Double, String>> iteration =
                    initialSolutionSet.iterateDelta(initialWorkSet, 10, 1);

            try {
                iteration.getWorkset().join(iteration.getSolutionSet()).where(1).equalTo(2);
                fail("Accepted invalid program.");
            } catch (InvalidProgramException e) {
                // all good!
            }

            try {
                iteration.getSolutionSet().join(iteration.getWorkset()).where(2).equalTo(1);
                fail("Accepted invalid program.");
            } catch (InvalidProgramException e) {
                // all good!
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testRejectWhenSolutionSetKeysDontMatchCoGroup() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            @SuppressWarnings("unchecked")
            DataSet<Tuple3<Double, Long, String>> initialSolutionSet =
                    env.fromElements(new Tuple3<Double, Long, String>(3.44, 5L, "abc"));

            @SuppressWarnings("unchecked")
            DataSet<Tuple2<Double, String>> initialWorkSet =
                    env.fromElements(new Tuple2<Double, String>(1.23, "abc"));

            DeltaIteration<Tuple3<Double, Long, String>, Tuple2<Double, String>> iteration =
                    initialSolutionSet.iterateDelta(initialWorkSet, 10, 1);

            try {
                iteration
                        .getWorkset()
                        .coGroup(iteration.getSolutionSet())
                        .where(1)
                        .equalTo(2)
                        .with(new SolutionWorksetCoGroup1());
                fail("Accepted invalid program.");
            } catch (InvalidProgramException e) {
                // all good!
            }

            try {
                iteration
                        .getSolutionSet()
                        .coGroup(iteration.getWorkset())
                        .where(2)
                        .equalTo(1)
                        .with(new SolutionWorksetCoGroup2());
                fail("Accepted invalid program.");
            } catch (InvalidProgramException e) {
                // all good!
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // --------------------------------------------------------------------------------------------

    private static class SolutionWorksetJoin
            extends RichJoinFunction<
                    Tuple2<Double, String>,
                    Tuple3<Double, Long, String>,
                    Tuple3<Double, Long, String>> {
        @Override
        public Tuple3<Double, Long, String> join(
                Tuple2<Double, String> first, Tuple3<Double, Long, String> second) {
            return null;
        }
    }

    private static class NextWorksetMapper
            extends RichMapFunction<Tuple3<Double, Long, String>, Tuple2<Double, String>> {
        @Override
        public Tuple2<Double, String> map(Tuple3<Double, Long, String> value) {
            return null;
        }
    }

    private static class IdentityMapper<T> extends RichMapFunction<T, T> {

        @Override
        public T map(T value) throws Exception {
            return value;
        }
    }

    private static class SolutionWorksetCoGroup1
            extends RichCoGroupFunction<
                    Tuple2<Double, String>,
                    Tuple3<Double, Long, String>,
                    Tuple3<Double, Long, String>> {

        @Override
        public void coGroup(
                Iterable<Tuple2<Double, String>> first,
                Iterable<Tuple3<Double, Long, String>> second,
                Collector<Tuple3<Double, Long, String>> out) {}
    }

    private static class SolutionWorksetCoGroup2
            extends RichCoGroupFunction<
                    Tuple3<Double, Long, String>,
                    Tuple2<Double, String>,
                    Tuple3<Double, Long, String>> {

        @Override
        public void coGroup(
                Iterable<Tuple3<Double, Long, String>> second,
                Iterable<Tuple2<Double, String>> first,
                Collector<Tuple3<Double, Long, String>> out) {}
    }
}
