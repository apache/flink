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

package org.apache.flink.table.planner.plan.nodes.exec.processor;

import org.apache.flink.api.common.BatchShuffleMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecDynamicFilteringDataCollector;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExecutionOrderEnforcer;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.AbstractExecNodeExactlyOnceVisitor;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecTableSourceScan.getDynamicFilteringDataCollector;

/**
 * This processor future checks each dynamic filter source to see if it is chained with a multiple
 * input operator. If so, we'll set the dependency flag.
 *
 * <p>NOTE: This processor can be only applied on {@link BatchExecNode} DAG.
 */
public class DynamicFilteringDependencyProcessor implements ExecNodeGraphProcessor {

    @Override
    public ExecNodeGraph process(ExecNodeGraph execGraph, ProcessorContext context) {
        ExecNodeGraph factSideProcessedGraph = addOrderEnforcer(execGraph, context);
        return enforceDimSideBlockingExchange(factSideProcessedGraph, context);
    }

    private ExecNodeGraph addOrderEnforcer(ExecNodeGraph execGraph, ProcessorContext context) {
        Map<BatchExecTableSourceScan, List<DescendantInfo>> dynamicFilteringScanDescendants =
                new HashMap<>();

        AbstractExecNodeExactlyOnceVisitor dynamicFilteringScanCollector =
                new AbstractExecNodeExactlyOnceVisitor() {
                    @Override
                    protected void visitNode(ExecNode<?> node) {
                        for (int i = 0; i < node.getInputEdges().size(); ++i) {
                            ExecEdge edge = node.getInputEdges().get(i);
                            ExecNode<?> input = edge.getSource();

                            // The character of the dynamic filter scan is that it
                            // has an input.
                            if (input instanceof BatchExecTableSourceScan
                                    && input.getInputEdges().size() > 0) {
                                dynamicFilteringScanDescendants
                                        .computeIfAbsent(
                                                (BatchExecTableSourceScan) input,
                                                ignored -> new ArrayList<>())
                                        .add(new DescendantInfo(node, i));
                            }
                        }

                        visitInputs(node);
                    }
                };
        execGraph.getRootNodes().forEach(node -> node.accept(dynamicFilteringScanCollector));

        for (Map.Entry<BatchExecTableSourceScan, List<DescendantInfo>> entry :
                dynamicFilteringScanDescendants.entrySet()) {
            BatchExecTableSourceScan oldTableSourceScan = entry.getKey();
            BatchExecDynamicFilteringDataCollector dynamicFilteringDataCollector =
                    getDynamicFilteringDataCollector(oldTableSourceScan);

            // we clear the input of tableSourceScan to avoid cycle in exec plan
            BatchExecTableSourceScan newTableSourceScan = oldTableSourceScan.copyAndRemoveInputs();

            // Add exchange between collector and enforcer
            BatchExecExchange exchange =
                    new BatchExecExchange(
                            context.getPlanner().getTableConfig(),
                            InputProperty.builder()
                                    .requiredDistribution(InputProperty.ANY_DISTRIBUTION)
                                    .damBehavior(InputProperty.DamBehavior.BLOCKING)
                                    .build(),
                            (RowType) dynamicFilteringDataCollector.getOutputType(),
                            "Exchange");
            exchange.setRequiredExchangeMode(StreamExchangeMode.BATCH);
            exchange.setInputEdges(
                    Collections.singletonList(
                            ExecEdge.builder()
                                    .source(dynamicFilteringDataCollector)
                                    .target(exchange)
                                    .build()));

            // set enforcer inputs
            BatchExecExecutionOrderEnforcer enforcer =
                    new BatchExecExecutionOrderEnforcer(
                            context.getPlanner().getTableConfig(),
                            Arrays.asList(
                                    exchange.getInputProperties().get(0), InputProperty.DEFAULT),
                            newTableSourceScan.getOutputType(),
                            "OrderEnforcer");
            ExecEdge edge1 = ExecEdge.builder().source(exchange).target(enforcer).build();
            ExecEdge edge2 = ExecEdge.builder().source(newTableSourceScan).target(enforcer).build();
            enforcer.setInputEdges(Arrays.asList(edge1, edge2));

            // set enforcer's output
            entry.getValue()
                    .forEach(
                            descendantInfo ->
                                    descendantInfo.descendant.replaceInputEdge(
                                            descendantInfo.inputId,
                                            ExecEdge.builder()
                                                    .source(enforcer)
                                                    .target(descendantInfo.descendant)
                                                    .build()));
        }

        return execGraph;
    }

    private ExecNodeGraph enforceDimSideBlockingExchange(
            ExecNodeGraph execGraph, ProcessorContext context) {
        if (context.getPlanner()
                        .getTableConfig()
                        .getConfiguration()
                        .get(ExecutionOptions.BATCH_SHUFFLE_MODE)
                == BatchShuffleMode.ALL_EXCHANGES_BLOCKING) {
            return execGraph;
        }

        Set<ExecNode<?>> nodesRequiredBlockingOutputs = new HashSet<>();
        // Find all the dynamic filter collector nodes and theirs inputs.
        AbstractExecNodeExactlyOnceVisitor nodesRequiredBlockingOutputsCollector =
                new AbstractExecNodeExactlyOnceVisitor() {
                    @Override
                    protected void visitNode(ExecNode<?> node) {
                        if (node instanceof BatchExecDynamicFilteringDataCollector) {
                            nodesRequiredBlockingOutputs.add(node);
                        }

                        // Here either it is added in the above lines or by its children nodes.
                        if (nodesRequiredBlockingOutputs.contains(node)) {
                            node.getInputEdges().stream()
                                    .map(ExecEdge::getSource)
                                    .forEach(nodesRequiredBlockingOutputs::add);
                        }

                        visitInputs(node);
                    }
                };
        execGraph
                .getRootNodes()
                .forEach(node -> node.accept(nodesRequiredBlockingOutputsCollector));

        // Now we make all the output edges in nodesRequiredBlockingOutputs to be blocking.
        AbstractExecNodeExactlyOnceVisitor blockingEnforcerVisitor =
                new AbstractExecNodeExactlyOnceVisitor() {
                    @Override
                    protected void visitNode(ExecNode<?> node) {
                        visitInputs(node);

                        // We only consider the edges that the source is in the set, but
                        // the target does not.
                        if (nodesRequiredBlockingOutputs.contains(node)) {
                            return;
                        }

                        for (int i = 0; i < node.getInputEdges().size(); ++i) {
                            ExecEdge edge = node.getInputEdges().get(i);
                            ExecNode<?> source = edge.getSource();

                            // We only consider the edges that the source is in the set, but
                            // the target does not.
                            if (!nodesRequiredBlockingOutputs.contains(source)) {
                                continue;
                            }

                            if (source instanceof BatchExecExchange) {
                                ((BatchExecExchange) source)
                                        .setRequiredExchangeMode(StreamExchangeMode.BATCH);
                            } else if (node instanceof BatchExecExchange) {
                                ((BatchExecExchange) node)
                                        .setRequiredExchangeMode(StreamExchangeMode.BATCH);
                            } else {
                                BatchExecExchange exchange =
                                        createExchange(
                                                source,
                                                node.getInputProperties().get(i),
                                                context.getPlanner().getTableConfig());
                                ExecEdge newEdge =
                                        ExecEdge.builder().source(exchange).target(node).build();
                                node.replaceInputEdge(i, newEdge);
                            }
                        }
                    }
                };
        execGraph.getRootNodes().forEach(node -> node.accept(blockingEnforcerVisitor));

        return execGraph;
    }

    private BatchExecExchange createExchange(
            ExecNode<?> source, InputProperty inputProperty, TableConfig tableConfig) {
        InputProperty newProperty =
                InputProperty.builder()
                        .requiredDistribution(
                                inputProperty.getRequiredDistribution()
                                                == InputProperty.UNKNOWN_DISTRIBUTION
                                        ? InputProperty.ANY_DISTRIBUTION
                                        : inputProperty.getRequiredDistribution())
                        .damBehavior(inputProperty.getDamBehavior())
                        .priority(inputProperty.getPriority())
                        .build();

        BatchExecExchange exchange =
                new BatchExecExchange(
                        tableConfig, newProperty, (RowType) source.getOutputType(), "Exchange");
        exchange.setRequiredExchangeMode(StreamExchangeMode.BATCH);
        ExecEdge execEdge = ExecEdge.builder().source(source).target(exchange).build();
        exchange.setInputEdges(Collections.singletonList(execEdge));

        return exchange;
    }

    private static class DescendantInfo {
        /** The DynamicFilteringScan is the {@code inputId}th input of current descendant . */
        final int inputId;

        final ExecNode<?> descendant;

        DescendantInfo(ExecNode<?> descendant, int inputId) {
            this.descendant = descendant;
            this.inputId = inputId;
        }
    }
}
