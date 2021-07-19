/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.exec.processor.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.TestingBatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;

/** Tests for {@link InputPriorityConflictResolver}. */
public class InputPriorityConflictResolverTest {

    @Test
    public void testDetectAndResolve() {
        // P = InputProperty.DamBehavior.PIPELINED, E = InputProperty.DamBehavior.END_INPUT
        // P100 = PIPELINED + priority 100
        //
        // 0 --------(P0)----> 1 --(P0)-----------> 7
        //  \                    \-(P0)-> 2 -(P0)--/
        //   \-------(P0)----> 3 --(P1)-----------/
        //    \------(P0)----> 4 --(P10)---------/
        //     \              /                 /
        //      \    8 -(P0)-<                 /
        //       \            \               /
        //        \--(E0)----> 5 --(P10)-----/
        // 6 ---------(P100)----------------/
        TestingBatchExecNode[] nodes = new TestingBatchExecNode[9];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = new TestingBatchExecNode("TestingBatchExecNode" + i);
        }
        nodes[1].addInput(nodes[0], InputProperty.builder().priority(0).build());
        nodes[2].addInput(nodes[1], InputProperty.builder().priority(0).build());
        nodes[3].addInput(nodes[0], InputProperty.builder().priority(0).build());
        nodes[4].addInput(nodes[8], InputProperty.builder().priority(0).build());
        nodes[4].addInput(nodes[0], InputProperty.builder().priority(0).build());
        nodes[5].addInput(nodes[8], InputProperty.builder().priority(0).build());
        nodes[5].addInput(
                nodes[0],
                InputProperty.builder()
                        .damBehavior(InputProperty.DamBehavior.END_INPUT)
                        .priority(0)
                        .build());
        nodes[7].addInput(nodes[1], InputProperty.builder().priority(0).build());
        nodes[7].addInput(nodes[2], InputProperty.builder().priority(0).build());
        nodes[7].addInput(nodes[3], InputProperty.builder().priority(1).build());
        nodes[7].addInput(nodes[4], InputProperty.builder().priority(10).build());
        nodes[7].addInput(nodes[5], InputProperty.builder().priority(10).build());
        nodes[7].addInput(nodes[6], InputProperty.builder().priority(100).build());

        InputPriorityConflictResolver resolver =
                new InputPriorityConflictResolver(
                        Collections.singletonList(nodes[7]),
                        InputProperty.DamBehavior.END_INPUT,
                        StreamExchangeMode.BATCH,
                        new Configuration());
        resolver.detectAndResolve();
        Assert.assertEquals(nodes[1], nodes[7].getInputNodes().get(0));
        Assert.assertEquals(nodes[2], nodes[7].getInputNodes().get(1));
        Assert.assertTrue(nodes[7].getInputNodes().get(2) instanceof BatchExecExchange);
        Assert.assertEquals(
                Optional.of(StreamExchangeMode.BATCH),
                ((BatchExecExchange) nodes[7].getInputNodes().get(2)).getRequiredExchangeMode());
        Assert.assertEquals(
                nodes[3], nodes[7].getInputNodes().get(2).getInputEdges().get(0).getSource());
        Assert.assertTrue(nodes[7].getInputNodes().get(3) instanceof BatchExecExchange);
        Assert.assertEquals(
                Optional.of(StreamExchangeMode.BATCH),
                ((BatchExecExchange) nodes[7].getInputNodes().get(3)).getRequiredExchangeMode());
        Assert.assertEquals(
                nodes[4], nodes[7].getInputNodes().get(3).getInputEdges().get(0).getSource());
        Assert.assertEquals(nodes[5], nodes[7].getInputNodes().get(4));
        Assert.assertEquals(nodes[6], nodes[7].getInputNodes().get(5));
    }

    @Test
    public void testDeadlockCausedByExchange() {
        // P1 = PIPELINED + priority 1
        //
        // 0 -(P0)-> exchange --(P0)-> 1
        //                    \-(P1)-/
        TestingBatchExecNode[] nodes = new TestingBatchExecNode[2];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = new TestingBatchExecNode("TestingBatchExecNode" + i);
        }

        BatchExecExchange exchange =
                new BatchExecExchange(
                        InputProperty.builder()
                                .requiredDistribution(InputProperty.ANY_DISTRIBUTION)
                                .build(),
                        (RowType) nodes[0].getOutputType(),
                        "Exchange");
        exchange.setRequiredExchangeMode(StreamExchangeMode.BATCH);
        ExecEdge execEdge = ExecEdge.builder().source(nodes[0]).target(exchange).build();
        exchange.setInputEdges(Collections.singletonList(execEdge));

        nodes[1].addInput(exchange, InputProperty.builder().priority(0).build());
        nodes[1].addInput(exchange, InputProperty.builder().priority(1).build());

        InputPriorityConflictResolver resolver =
                new InputPriorityConflictResolver(
                        Collections.singletonList(nodes[1]),
                        InputProperty.DamBehavior.END_INPUT,
                        StreamExchangeMode.BATCH,
                        new Configuration());
        resolver.detectAndResolve();

        ExecNode<?> input0 = nodes[1].getInputNodes().get(0);
        ExecNode<?> input1 = nodes[1].getInputNodes().get(1);
        Assert.assertNotSame(input0, input1);

        Consumer<ExecNode<?>> checkExchange =
                execNode -> {
                    Assert.assertTrue(execNode instanceof BatchExecExchange);
                    BatchExecExchange e = (BatchExecExchange) execNode;
                    Assert.assertEquals(
                            Optional.of(StreamExchangeMode.BATCH), e.getRequiredExchangeMode());
                    Assert.assertEquals(nodes[0], e.getInputEdges().get(0).getSource());
                };
        checkExchange.accept(input0);
        checkExchange.accept(input1);
    }
}
