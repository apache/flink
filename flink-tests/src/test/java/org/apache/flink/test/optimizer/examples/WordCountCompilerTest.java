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

package org.apache.flink.test.optimizer.examples;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/** Validate program compilation. */
public class WordCountCompilerTest extends CompilerTestBase {

    private static final long serialVersionUID = 8988304231385358228L;

    /** This method tests the simple word count. */
    @Test
    public void testWordCount() {
        checkWordCount(true);
        checkWordCount(false);
    }

    private void checkWordCount(boolean estimates) {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(DEFAULT_PARALLELISM);

        // get input data
        DataSet<String> lines = env.readTextFile(IN_FILE).name("Input Lines");

        lines
                // dummy map
                .map(
                        new MapFunction<String, Tuple2<String, Integer>>() {
                            private static final long serialVersionUID = -3952739820618875030L;

                            @Override
                            public Tuple2<String, Integer> map(String v) throws Exception {
                                return new Tuple2<>(v, 1);
                            }
                        })
                .name("Tokenize Lines")
                // count
                .groupBy(0)
                .sum(1)
                .name("Count Words")
                // discard
                .output(new DiscardingOutputFormat<Tuple2<String, Integer>>())
                .name("Word Counts");

        // get the plan and compile it
        Plan p = env.createProgramPlan();
        p.setExecutionConfig(new ExecutionConfig());

        OptimizedPlan plan;
        if (estimates) {
            GenericDataSourceBase<?, ?> source = getContractResolver(p).getNode("Input Lines");
            setSourceStatistics(source, 1024 * 1024 * 1024 * 1024L, 24f);
            plan = compileWithStats(p);
        } else {
            plan = compileNoStats(p);
        }

        // get the optimizer plan nodes
        OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(plan);
        SinkPlanNode sink = resolver.getNode("Word Counts");
        SingleInputPlanNode reducer = resolver.getNode("Count Words");
        SingleInputPlanNode mapper = resolver.getNode("Tokenize Lines");

        // verify the strategies
        Assert.assertEquals(ShipStrategyType.FORWARD, mapper.getInput().getShipStrategy());
        Assert.assertEquals(ShipStrategyType.PARTITION_HASH, reducer.getInput().getShipStrategy());
        Assert.assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());

        Channel c = reducer.getInput();
        Assert.assertEquals(LocalStrategy.COMBININGSORT, c.getLocalStrategy());
        FieldList l = new FieldList(0);
        Assert.assertEquals(l, c.getShipStrategyKeys());
        Assert.assertEquals(l, c.getLocalStrategyKeys());
        Assert.assertTrue(Arrays.equals(c.getLocalStrategySortOrder(), reducer.getSortOrders(0)));

        // check the combiner
        SingleInputPlanNode combiner = (SingleInputPlanNode) reducer.getPredecessor();
        Assert.assertEquals(DriverStrategy.SORTED_GROUP_COMBINE, combiner.getDriverStrategy());
        Assert.assertEquals(l, combiner.getKeys(0));
        Assert.assertEquals(ShipStrategyType.FORWARD, combiner.getInput().getShipStrategy());
    }
}
