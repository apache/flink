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

package org.apache.flink.table.planner.plan.nodes.exec.processor;

import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.plan.nodes.exec.AdaptiveJoinExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty.DistributionType;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecAdaptiveJoin;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.AbstractExecNodeExactlyOnceVisitor;
import org.apache.flink.table.planner.plan.utils.OperatorType;
import org.apache.flink.table.planner.utils.TableConfigUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.plan.nodes.exec.InputProperty.DistributionType.KEEP_INPUT_AS_IS;

/**
 * A {@link ExecNodeGraphProcessor} which replace the qualified join nodes into adaptive join nodes.
 */
public class AdaptiveJoinProcessor implements ExecNodeGraphProcessor {

    @Override
    public ExecNodeGraph process(ExecNodeGraph execGraph, ProcessorContext context) {
        if (execGraph.getRootNodes().get(0) instanceof StreamExecNode) {
            throw new TableException("AdaptiveJoin does not support streaming jobs.");
        }
        if (!isAdaptiveJoinEnabled(context)) {
            return execGraph;
        }

        AbstractExecNodeExactlyOnceVisitor visitor =
                new AbstractExecNodeExactlyOnceVisitor() {
                    @Override
                    protected void visitNode(ExecNode<?> node) {
                        visitInputs(node);
                        // To avoid data correctness issues, we will refrain from transforming join
                        // nodes with a strict KEEP_INPUT_AS_IS constraint downstream. Meanwhile,
                        // deeper evaluations will be conducted in the corresponding
                        // StreamGraphOptimizationStrategy.
                        // And we don't need to check all downstream nodes of the join, because the
                        // KEEP_INPUT_AS_IS constraint will be bound to BatchExecExchange, which
                        // will be the direct downstream node of the join.
                        if (shouldStrictKeepInputAsIs(node.getInputProperties())) {
                            return;
                        }
                        for (int i = 0; i < node.getInputEdges().size(); ++i) {
                            ExecEdge edge = node.getInputEdges().get(i);
                            ExecNode<?> newNode = tryReplaceWithAdaptiveJoinNode(edge.getSource());
                            node.replaceInputEdge(
                                    i,
                                    ExecEdge.builder()
                                            .source(newNode)
                                            .target(node)
                                            .shuffle(edge.getShuffle())
                                            .exchangeMode(edge.getExchangeMode())
                                            .build());
                        }
                    }
                };

        List<ExecNode<?>> newRootNodes =
                execGraph.getRootNodes().stream()
                        .map(
                                node -> {
                                    node = tryReplaceWithAdaptiveJoinNode(node);
                                    node.accept(visitor);
                                    return node;
                                })
                        .collect(Collectors.toList());

        return new ExecNodeGraph(execGraph.getFlinkVersion(), newRootNodes);
    }

    private ExecNode<?> tryReplaceWithAdaptiveJoinNode(ExecNode<?> node) {
        // For AdaptiveJoin to be converted, its upstream input must ensure:
        // 1. Data distribution is by Hash.
        // 2. No upstream nodes require KEEP_INPUT_AS_IS (achieved by inserting BatchExecExchange).
        if (!(areAllInputsHashShuffle(node))
                || shouldKeepUpstreamExchangeInputAsIs(node.getInputEdges())) {
            return node;
        }
        ExecNode<?> newNode = node;
        if (node instanceof AdaptiveJoinExecNode
                && ((AdaptiveJoinExecNode) node).canBeTransformedToAdaptiveJoin()) {
            BatchExecAdaptiveJoin adaptiveJoin = ((AdaptiveJoinExecNode) node).toAdaptiveJoinNode();
            replaceInputEdge(adaptiveJoin, node);
            newNode = adaptiveJoin;
        }

        return newNode;
    }

    private boolean shouldStrictKeepInputAsIs(List<InputProperty> inputProperties) {
        return inputProperties.stream()
                .anyMatch(
                        inputProperty ->
                                (inputProperty.getRequiredDistribution().getType()
                                                == KEEP_INPUT_AS_IS)
                                        && ((InputProperty.KeepInputAsIsDistribution)
                                                        inputProperty.getRequiredDistribution())
                                                .isStrict());
    }

    private boolean shouldKeepInputAsIs(List<InputProperty> inputProperties) {
        return inputProperties.stream()
                .anyMatch(
                        inputProperty ->
                                inputProperty.getRequiredDistribution().getType()
                                        == KEEP_INPUT_AS_IS);
    }

    // If KEEP_INPUT_AS_IS constraint exists on an operator, it will always show on its upstream
    // BatchExecExchange.
    private boolean shouldKeepUpstreamExchangeInputAsIs(List<ExecEdge> inputEdges) {
        return inputEdges.stream()
                .filter(execEdge -> execEdge.getSource() instanceof BatchExecExchange)
                .map(execEdge -> (BatchExecExchange) execEdge.getSource())
                .anyMatch(exchange -> shouldKeepInputAsIs(exchange.getInputProperties()));
    }

    private boolean isAdaptiveJoinEnabled(ProcessorContext context) {
        TableConfig tableConfig = context.getPlanner().getTableConfig();
        boolean isAdaptiveJoinEnabled =
                tableConfig.get(
                                        OptimizerConfigOptions
                                                .TABLE_OPTIMIZER_ADAPTIVE_BROADCAST_JOIN_STRATEGY)
                                != OptimizerConfigOptions.AdaptiveBroadcastJoinStrategy.NONE
                        && !TableConfigUtils.isOperatorDisabled(
                                tableConfig, OperatorType.BroadcastHashJoin);
        isAdaptiveJoinEnabled |=
                tableConfig.get(
                                OptimizerConfigOptions
                                        .TABLE_OPTIMIZER_ADAPTIVE_SKEWED_JOIN_OPTIMIZATION_STRATEGY)
                        != OptimizerConfigOptions.AdaptiveSkewedJoinOptimizationStrategy.NONE;
        JobManagerOptions.SchedulerType schedulerType =
                context.getPlanner()
                        .getExecEnv()
                        .getConfig()
                        .getSchedulerType()
                        .orElse(JobManagerOptions.SchedulerType.AdaptiveBatch);
        boolean isAdaptiveBatchSchedulerEnabled =
                schedulerType == JobManagerOptions.SchedulerType.AdaptiveBatch;
        // Currently, adaptive join optimization and batch job progress recovery cannot be enabled
        // simultaneously so we should disable it here.
        // TODO: If job recovery for adaptive execution is supported in the future, this logic will
        //  need to be removed.
        boolean isJobRecoveryEnabled = tableConfig.get(BatchExecutionOptions.JOB_RECOVERY_ENABLED);
        return isAdaptiveJoinEnabled && isAdaptiveBatchSchedulerEnabled && !isJobRecoveryEnabled;
    }

    private boolean areAllInputsHashShuffle(ExecNode<?> node) {
        for (InputProperty inputProperty : node.getInputProperties()) {
            if (inputProperty.getRequiredDistribution().getType() != DistributionType.HASH) {
                return false;
            }
        }
        return true;
    }

    private void replaceInputEdge(ExecNode<?> newNode, ExecNode<?> originalNode) {
        List<ExecEdge> inputEdges = new ArrayList<>();
        for (int i = 0; i < originalNode.getInputEdges().size(); ++i) {
            ExecEdge edge = originalNode.getInputEdges().get(i);
            inputEdges.add(
                    ExecEdge.builder()
                            .source(edge.getSource())
                            .target(newNode)
                            .shuffle(edge.getShuffle())
                            .exchangeMode(edge.getExchangeMode())
                            .build());
        }
        newNode.setInputEdges(inputEdges);
    }
}
