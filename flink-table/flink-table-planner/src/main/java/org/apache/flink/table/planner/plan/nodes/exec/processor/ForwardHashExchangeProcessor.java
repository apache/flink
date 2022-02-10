/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.exec.processor;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecMultipleInput;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.AbstractExecNodeExactlyOnceVisitor;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitor;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A {@link ExecNodeGraphProcessor} which finds all {@link ExecNode}s that require hash
 * distribution, but its input has no hash Exchange node. Currently, the hash distribution is
 * satisfied by its none exchange inputs with FORWARD partitioner (the node and its input has the
 * same parallelism). Once the parallelism is changed, the FORWARD behavior will be broken, and the
 * result will be wrong.
 *
 * <p>In order to meet the needs of flexible parallelism changing by adaptive scheduler, a special
 * {@link BatchExecExchange} (with KEEP_INPUT_AS_IS distribution flag) will be added for the {@link
 * ExecNode} as its input. And then the StreamingJobGraphGenerator will decide which partitioner can
 * be used when dynamic-graph is enabled: FORWARD partitioner if nodes are chainable, else HASH
 * partitioner.
 *
 * <p>Its works only for batch job when dynamic-graph is enabled.
 */
public class ForwardHashExchangeProcessor implements ExecNodeGraphProcessor {

    @Override
    public ExecNodeGraph process(ExecNodeGraph execGraph, ProcessorContext context) {
        if (execGraph.getRootNodes().get(0) instanceof StreamExecNode) {
            throw new TableException("StreamExecNode is not supported yet");
        }
        if (!context.getPlanner().getExecEnv().getConfig().isDynamicGraph()) {
            return execGraph;
        }
        ExecNodeVisitor visitor =
                new AbstractExecNodeExactlyOnceVisitor() {
                    @Override
                    protected void visitNode(ExecNode<?> node) {
                        visitInputs(node);
                        if (node instanceof CommonExecExchange) {
                            return;
                        }
                        boolean changed = false;
                        List<ExecEdge> newEdges = new ArrayList<>(node.getInputEdges());
                        for (int i = 0; i < node.getInputProperties().size(); ++i) {
                            InputProperty inputProperty = node.getInputProperties().get(i);
                            InputProperty.RequiredDistribution requiredDistribution =
                                    inputProperty.getRequiredDistribution();
                            if (requiredDistribution.getType()
                                    != InputProperty.DistributionType.HASH) {
                                continue;
                            }
                            ExecEdge edge = node.getInputEdges().get(i);
                            if (!isExchangeInput(edge)) {
                                InputProperty newInputProperty =
                                        InputProperty.builder()
                                                .requiredDistribution(
                                                        InputProperty.keepInputAsIsDistribution(
                                                                requiredDistribution))
                                                .damBehavior(inputProperty.getDamBehavior())
                                                .priority(inputProperty.getPriority())
                                                .build();
                                BatchExecExchange newExchange =
                                        new BatchExecExchange(
                                                newInputProperty,
                                                (RowType) edge.getOutputType(),
                                                newInputProperty.toString());

                                ExecEdge newEdge1 =
                                        new ExecEdge(
                                                edge.getSource(),
                                                newExchange,
                                                edge.getShuffle(),
                                                edge.getExchangeMode());
                                newExchange.setInputEdges(Collections.singletonList(newEdge1));

                                ExecEdge newEdge2 =
                                        new ExecEdge(
                                                newExchange,
                                                edge.getTarget(),
                                                edge.getShuffle(),
                                                edge.getExchangeMode());

                                // update the edge
                                newEdges.set(i, newEdge2);
                                updateOriginalEdgeInMultipleInput(node, i, newExchange);
                                changed = true;
                            }
                        }
                        if (changed) {
                            node.setInputEdges(newEdges);
                        }
                    }
                };
        execGraph.getRootNodes().forEach(s -> s.accept(visitor));
        return execGraph;
    }

    private boolean isExchangeInput(ExecEdge edge) {
        return edge.getSource() instanceof CommonExecExchange;
    }

    private void updateOriginalEdgeInMultipleInput(
            ExecNode<?> node, int edgeIdx, BatchExecExchange newExchange) {
        if (node instanceof BatchExecMultipleInput) {
            updateOriginalEdgeInMultipleInput((BatchExecMultipleInput) node, edgeIdx, newExchange);
        }
    }

    /**
     * Add new exchange node between the input node and the target node for the given edge, and
     * reconnect the edges. So that the transformations can be connected correctly.
     */
    private void updateOriginalEdgeInMultipleInput(
            BatchExecMultipleInput multipleInput, int edgeIdx, BatchExecExchange newExchange) {
        ExecEdge originalEdge = multipleInput.getOriginalEdges().get(edgeIdx);
        ExecNode<?> inputNode = originalEdge.getSource();
        ExecNode<?> targetNode = originalEdge.getTarget();
        int edgeIdxInTargetNode = targetNode.getInputEdges().indexOf(originalEdge);
        checkArgument(edgeIdxInTargetNode >= 0);
        List<ExecEdge> newEdges = new ArrayList<>(targetNode.getInputEdges());

        // connect input node to new exchange node
        ExecEdge newEdge1 =
                new ExecEdge(
                        inputNode,
                        newExchange,
                        originalEdge.getShuffle(),
                        originalEdge.getExchangeMode());
        newExchange.setInputEdges(Collections.singletonList(newEdge1));

        // connect new exchange node to target node
        ExecEdge newEdge2 =
                new ExecEdge(
                        newExchange,
                        targetNode,
                        originalEdge.getShuffle(),
                        originalEdge.getExchangeMode());
        newEdges.set(edgeIdxInTargetNode, newEdge2);
        targetNode.setInputEdges(newEdges);
    }
}
