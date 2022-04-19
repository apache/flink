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

package org.apache.flink.optimizer.java;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase.CombineHint;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plan.SourcePlanNode;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.DriverStrategy;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@SuppressWarnings("serial")
public class ReduceCompilationTest extends CompilerTestBase implements java.io.Serializable {

    @Test
    void testAllReduceNoCombiner() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(8);

            DataSet<Double> data = env.fromElements(0.2, 0.3, 0.4, 0.5).name("source");

            data.reduce(
                            new RichReduceFunction<Double>() {

                                @Override
                                public Double reduce(Double value1, Double value2) {
                                    return value1 + value2;
                                }
                            })
                    .name("reducer")
                    .output(new DiscardingOutputFormat<Double>())
                    .name("sink");

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(op);

            // the all-reduce has no combiner, when the parallelism of the input is one

            SourcePlanNode sourceNode = resolver.getNode("source");
            SingleInputPlanNode reduceNode = resolver.getNode("reducer");
            SinkPlanNode sinkNode = resolver.getNode("sink");

            // check wiring
            assertThat(reduceNode.getInput().getSource()).isEqualTo(sourceNode);
            assertThat(sinkNode.getInput().getSource()).isEqualTo(reduceNode);

            // check parallelism
            assertThat(sourceNode.getParallelism()).isEqualTo(1);
            assertThat(reduceNode.getParallelism()).isEqualTo(1);
            assertThat(sinkNode.getParallelism()).isEqualTo(1);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail(e.getClass().getSimpleName() + " in test: " + e.getMessage());
        }
    }

    @Test
    void testAllReduceWithCombiner() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(8);

            DataSet<Long> data = env.generateSequence(1, 8000000).name("source");

            data.reduce(
                            new RichReduceFunction<Long>() {

                                @Override
                                public Long reduce(Long value1, Long value2) {
                                    return value1 + value2;
                                }
                            })
                    .name("reducer")
                    .output(new DiscardingOutputFormat<Long>())
                    .name("sink");

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(op);

            // get the original nodes
            SourcePlanNode sourceNode = resolver.getNode("source");
            SingleInputPlanNode reduceNode = resolver.getNode("reducer");
            SinkPlanNode sinkNode = resolver.getNode("sink");

            // get the combiner
            SingleInputPlanNode combineNode =
                    (SingleInputPlanNode) reduceNode.getInput().getSource();

            // check wiring
            assertThat(combineNode.getInput().getSource()).isEqualTo(sourceNode);
            assertThat(sinkNode.getInput().getSource()).isEqualTo(reduceNode);

            // check that both reduce and combiner have the same strategy
            assertThat(reduceNode.getDriverStrategy()).isEqualTo(DriverStrategy.ALL_REDUCE);
            assertThat(combineNode.getDriverStrategy()).isEqualTo(DriverStrategy.ALL_REDUCE);

            // check parallelism
            assertThat(sourceNode.getParallelism()).isEqualTo(8);
            assertThat(combineNode.getParallelism()).isEqualTo(8);
            assertThat(reduceNode.getParallelism()).isEqualTo(1);
            assertThat(sinkNode.getParallelism()).isEqualTo(1);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail(e.getClass().getSimpleName() + " in test: " + e.getMessage());
        }
    }

    @Test
    void testGroupedReduceWithFieldPositionKey() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(8);

            DataSet<Tuple2<String, Double>> data =
                    env.readCsvFile("file:///will/never/be/read")
                            .types(String.class, Double.class)
                            .name("source")
                            .setParallelism(6);

            data.groupBy(1)
                    .reduce(
                            new RichReduceFunction<Tuple2<String, Double>>() {
                                @Override
                                public Tuple2<String, Double> reduce(
                                        Tuple2<String, Double> value1,
                                        Tuple2<String, Double> value2) {
                                    return null;
                                }
                            })
                    .name("reducer")
                    .output(new DiscardingOutputFormat<Tuple2<String, Double>>())
                    .name("sink");

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(op);

            // get the original nodes
            SourcePlanNode sourceNode = resolver.getNode("source");
            SingleInputPlanNode reduceNode = resolver.getNode("reducer");
            SinkPlanNode sinkNode = resolver.getNode("sink");

            // get the combiner
            SingleInputPlanNode combineNode =
                    (SingleInputPlanNode) reduceNode.getInput().getSource();

            // check wiring
            assertThat(combineNode.getInput().getSource()).isEqualTo(sourceNode);
            assertThat(sinkNode.getInput().getSource()).isEqualTo(reduceNode);

            // check the strategies
            assertThat(reduceNode.getDriverStrategy()).isEqualTo(DriverStrategy.SORTED_REDUCE);
            assertThat(combineNode.getDriverStrategy())
                    .isEqualTo(DriverStrategy.SORTED_PARTIAL_REDUCE);

            // check the keys
            assertThat(reduceNode.getKeys(0)).isEqualTo(new FieldList(1));
            assertThat(combineNode.getKeys(0)).isEqualTo(new FieldList(1));
            assertThat(reduceNode.getInput().getLocalStrategyKeys()).isEqualTo(new FieldList(1));

            // check parallelism
            assertThat(sourceNode.getParallelism()).isEqualTo(6);
            assertThat(combineNode.getParallelism()).isEqualTo(6);
            assertThat(reduceNode.getParallelism()).isEqualTo(8);
            assertThat(sinkNode.getParallelism()).isEqualTo(8);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail(e.getClass().getSimpleName() + " in test: " + e.getMessage());
        }
    }

    @Test
    void testGroupedReduceWithSelectorFunctionKey() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(8);

            DataSet<Tuple2<String, Double>> data =
                    env.readCsvFile("file:///will/never/be/read")
                            .types(String.class, Double.class)
                            .name("source")
                            .setParallelism(6);

            data.groupBy(
                            new KeySelector<Tuple2<String, Double>, String>() {
                                public String getKey(Tuple2<String, Double> value) {
                                    return value.f0;
                                }
                            })
                    .reduce(
                            new RichReduceFunction<Tuple2<String, Double>>() {
                                @Override
                                public Tuple2<String, Double> reduce(
                                        Tuple2<String, Double> value1,
                                        Tuple2<String, Double> value2) {
                                    return null;
                                }
                            })
                    .name("reducer")
                    .output(new DiscardingOutputFormat<Tuple2<String, Double>>())
                    .name("sink");

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(op);

            // get the original nodes
            SourcePlanNode sourceNode = resolver.getNode("source");
            SingleInputPlanNode reduceNode = resolver.getNode("reducer");
            SinkPlanNode sinkNode = resolver.getNode("sink");

            // get the combiner
            SingleInputPlanNode combineNode =
                    (SingleInputPlanNode) reduceNode.getInput().getSource();

            // get the key extractors and projectors
            SingleInputPlanNode keyExtractor =
                    (SingleInputPlanNode) combineNode.getInput().getSource();
            SingleInputPlanNode keyProjector =
                    (SingleInputPlanNode) sinkNode.getInput().getSource();

            // check wiring
            assertThat(keyExtractor.getInput().getSource()).isEqualTo(sourceNode);
            assertThat(sinkNode.getInput().getSource()).isEqualTo(keyProjector);

            // check the strategies
            assertThat(reduceNode.getDriverStrategy()).isEqualTo(DriverStrategy.SORTED_REDUCE);
            assertThat(combineNode.getDriverStrategy())
                    .isEqualTo(DriverStrategy.SORTED_PARTIAL_REDUCE);

            // check the keys
            assertThat(reduceNode.getKeys(0)).isEqualTo(new FieldList(0));
            assertThat(combineNode.getKeys(0)).isEqualTo(new FieldList(0));
            assertThat(reduceNode.getInput().getLocalStrategyKeys()).isEqualTo(new FieldList(0));

            // check parallelism
            assertThat(sourceNode.getParallelism()).isEqualTo(6);
            assertThat(keyExtractor.getParallelism()).isEqualTo(6);
            assertThat(combineNode.getParallelism()).isEqualTo(6);

            assertThat(reduceNode.getParallelism()).isEqualTo(8);
            assertThat(keyProjector.getParallelism()).isEqualTo(8);
            assertThat(sinkNode.getParallelism()).isEqualTo(8);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail(e.getClass().getSimpleName() + " in test: " + e.getMessage());
        }
    }

    @Test
    void testGroupedReduceWithHint() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(8);

            DataSet<Tuple2<String, Double>> data =
                    env.readCsvFile("file:///will/never/be/read")
                            .types(String.class, Double.class)
                            .name("source")
                            .setParallelism(6);

            data.groupBy(
                            new KeySelector<Tuple2<String, Double>, String>() {
                                public String getKey(Tuple2<String, Double> value) {
                                    return value.f0;
                                }
                            })
                    .reduce(
                            new RichReduceFunction<Tuple2<String, Double>>() {
                                @Override
                                public Tuple2<String, Double> reduce(
                                        Tuple2<String, Double> value1,
                                        Tuple2<String, Double> value2) {
                                    return null;
                                }
                            })
                    .setCombineHint(CombineHint.HASH)
                    .name("reducer")
                    .output(new DiscardingOutputFormat<Tuple2<String, Double>>())
                    .name("sink");

            Plan p = env.createProgramPlan();
            OptimizedPlan op = compileNoStats(p);

            OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(op);

            // get the original nodes
            SourcePlanNode sourceNode = resolver.getNode("source");
            SingleInputPlanNode reduceNode = resolver.getNode("reducer");
            SinkPlanNode sinkNode = resolver.getNode("sink");

            // get the combiner
            SingleInputPlanNode combineNode =
                    (SingleInputPlanNode) reduceNode.getInput().getSource();

            // get the key extractors and projectors
            SingleInputPlanNode keyExtractor =
                    (SingleInputPlanNode) combineNode.getInput().getSource();
            SingleInputPlanNode keyProjector =
                    (SingleInputPlanNode) sinkNode.getInput().getSource();

            // check wiring
            assertThat(keyExtractor.getInput().getSource()).isEqualTo(sourceNode);
            assertThat(sinkNode.getInput().getSource()).isEqualTo(keyProjector);

            // check the strategies
            assertThat(reduceNode.getDriverStrategy()).isEqualTo(DriverStrategy.SORTED_REDUCE);
            assertThat(combineNode.getDriverStrategy())
                    .isEqualTo(DriverStrategy.HASHED_PARTIAL_REDUCE);

            // check the keys
            assertThat(reduceNode.getKeys(0)).isEqualTo(new FieldList(0));
            assertThat(combineNode.getKeys(0)).isEqualTo(new FieldList(0));
            assertThat(reduceNode.getInput().getLocalStrategyKeys()).isEqualTo(new FieldList(0));

            // check parallelism
            assertThat(sourceNode.getParallelism()).isEqualTo(6);
            assertThat(keyExtractor.getParallelism()).isEqualTo(6);
            assertThat(combineNode.getParallelism()).isEqualTo(6);

            assertThat(reduceNode.getParallelism()).isEqualTo(8);
            assertThat(keyProjector.getParallelism()).isEqualTo(8);
            assertThat(sinkNode.getParallelism()).isEqualTo(8);
        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            fail(e.getClass().getSimpleName() + " in test: " + e.getMessage());
        }
    }

    /**
     * Test program compilation when the Reduce's combiner has been excluded by setting {@code
     * CombineHint.NONE}.
     */
    @Test
    void testGroupedReduceWithoutCombiner() {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);

        DataSet<Tuple2<String, Double>> data =
                env.readCsvFile("file:///will/never/be/read")
                        .types(String.class, Double.class)
                        .name("source")
                        .setParallelism(6);

        data.groupBy(0)
                .reduce(
                        new RichReduceFunction<Tuple2<String, Double>>() {
                            @Override
                            public Tuple2<String, Double> reduce(
                                    Tuple2<String, Double> value1, Tuple2<String, Double> value2) {
                                return null;
                            }
                        })
                .setCombineHint(CombineHint.NONE)
                .name("reducer")
                .output(new DiscardingOutputFormat<Tuple2<String, Double>>())
                .name("sink");

        Plan p = env.createProgramPlan();
        OptimizedPlan op = compileNoStats(p);

        OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(op);

        // get the original nodes
        SourcePlanNode sourceNode = resolver.getNode("source");
        SingleInputPlanNode reduceNode = resolver.getNode("reducer");
        SinkPlanNode sinkNode = resolver.getNode("sink");

        // check wiring
        assertThat(reduceNode.getInput().getSource()).isEqualTo(sourceNode);

        // check the strategies
        assertThat(reduceNode.getDriverStrategy()).isEqualTo(DriverStrategy.SORTED_REDUCE);

        // check the keys
        assertThat(reduceNode.getKeys(0)).isEqualTo(new FieldList(0));
        assertThat(reduceNode.getInput().getLocalStrategyKeys()).isEqualTo(new FieldList(0));

        // check parallelism
        assertThat(sourceNode.getParallelism()).isEqualTo(6);
        assertThat(reduceNode.getParallelism()).isEqualTo(8);
        assertThat(sinkNode.getParallelism()).isEqualTo(8);
    }
}
