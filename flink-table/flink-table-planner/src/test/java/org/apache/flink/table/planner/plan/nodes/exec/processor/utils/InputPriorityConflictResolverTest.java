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
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecDynamicFilteringDataCollector;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(nodes[7].getInputNodes().get(0)).isEqualTo(nodes[1]);
        assertThat(nodes[7].getInputNodes().get(1)).isEqualTo(nodes[2]);
        assertThat(nodes[7].getInputNodes().get(2)).isInstanceOf(BatchExecExchange.class);
        assertThat(((BatchExecExchange) nodes[7].getInputNodes().get(2)).getRequiredExchangeMode())
                .isEqualTo(Optional.of(StreamExchangeMode.BATCH));
        assertThat(nodes[7].getInputNodes().get(2).getInputEdges().get(0).getSource())
                .isEqualTo(nodes[3]);
        assertThat(nodes[7].getInputNodes().get(3)).isInstanceOf(BatchExecExchange.class);
        assertThat(((BatchExecExchange) nodes[7].getInputNodes().get(3)).getRequiredExchangeMode())
                .isEqualTo(Optional.of(StreamExchangeMode.BATCH));
        assertThat(nodes[7].getInputNodes().get(3).getInputEdges().get(0).getSource())
                .isEqualTo(nodes[4]);
        assertThat(nodes[7].getInputNodes().get(4)).isEqualTo(nodes[5]);
        assertThat(nodes[7].getInputNodes().get(5)).isEqualTo(nodes[6]);
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
                        new Configuration(),
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
        assertThat(input1).isNotSameAs(input0);

        Consumer<ExecNode<?>> checkExchange =
                execNode -> {
                    assertThat(execNode).isInstanceOf(BatchExecExchange.class);
                    BatchExecExchange e = (BatchExecExchange) execNode;
                    assertThat(e.getRequiredExchangeMode())
                            .isEqualTo(Optional.of(StreamExchangeMode.BATCH));
                    assertThat(e.getInputEdges().get(0).getSource()).isEqualTo(nodes[0]);
                };
        checkExchange.accept(input0);
        checkExchange.accept(input1);
    }

    @Test
    public void testWithDynamicFilteringPlan() {
        // no conflicts for dpp pattern
        // 2 --------------------------------------(P1)--- 1 --(P0)--> 0
        //   \                                            /
        //   DynamicFilteringDataCollector               /
        //     \                                        /
        //    DynamicFilteringTableSourceScan --(P0) --/
        TestingBatchExecNode[] nodes = new TestingBatchExecNode[3];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = new TestingBatchExecNode("TestingBatchExecNode" + i);
        }

        BatchExecTableSourceScan scan =
                new BatchExecTableSourceScan(
                        new Configuration(),
                        new DynamicTableSourceSpec(null, null),
                        InputProperty.DEFAULT,
                        RowType.of(new IntType(), new IntType(), new IntType()),
                        "DynamicFilteringTableSourceScan");
        BatchExecDynamicFilteringDataCollector collector =
                new BatchExecDynamicFilteringDataCollector(
                        Collections.singletonList(1),
                        new Configuration(),
                        InputProperty.DEFAULT,
                        RowType.of(new IntType()),
                        "DynamicFilteringDataCollector");

        nodes[0].addInput(nodes[1], InputProperty.builder().priority(0).build());
        nodes[1].addInput(nodes[2], InputProperty.builder().priority(1).build());
        nodes[1].addInput(scan, InputProperty.builder().priority(0).build());
        ExecEdge collect2Scan = ExecEdge.builder().source(collector).target(scan).build();
        scan.setInputEdges(Collections.singletonList(collect2Scan));
        ExecEdge toCollector = ExecEdge.builder().source(nodes[2]).target(collector).build();
        collector.setInputEdges(Collections.singletonList(toCollector));

        InputPriorityConflictResolver resolver =
                new InputPriorityConflictResolver(
                        Collections.singletonList(nodes[1]),
                        InputProperty.DamBehavior.END_INPUT,
                        StreamExchangeMode.BATCH,
                        new Configuration());
        resolver.detectAndResolve();

        ExecNode<?> input0 = nodes[1].getInputNodes().get(0);
        ExecNode<?> input1 = nodes[1].getInputNodes().get(1);
        assertThat(input0).isSameAs(nodes[2]);
        assertThat(input1).isSameAs(scan);
    }
}
