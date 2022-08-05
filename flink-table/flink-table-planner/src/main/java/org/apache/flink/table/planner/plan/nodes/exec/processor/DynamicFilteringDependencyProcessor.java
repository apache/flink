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

import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecMultipleInput;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.AbstractExecNodeExactlyOnceVisitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This processor future checks each dynamic filter source to see if it is chained with a multiple
 * input operator. If so, we'll set the dependency flag.
 *
 * <p>NOTE: This processor can be only applied on {@link BatchExecNode} DAG.
 */
public class DynamicFilteringDependencyProcessor implements ExecNodeGraphProcessor {

    @Override
    public ExecNodeGraph process(ExecNodeGraph execGraph, ProcessorContext context) {
        Map<BatchExecTableSourceScan, List<ExecNode<?>>> dynamicFilteringScanDescendants =
                new HashMap<>();

        AbstractExecNodeExactlyOnceVisitor dynamicFilteringScanCollector =
                new AbstractExecNodeExactlyOnceVisitor() {
                    @Override
                    protected void visitNode(ExecNode<?> node) {
                        node.getInputEdges().stream()
                                .map(ExecEdge::getSource)
                                .forEach(
                                        input -> {
                                            // The character of the dynamic filter scan is that it
                                            // has an input.
                                            if (input instanceof BatchExecTableSourceScan
                                                    && input.getInputEdges().size() > 0) {
                                                dynamicFilteringScanDescendants
                                                        .computeIfAbsent(
                                                                (BatchExecTableSourceScan) input,
                                                                ignored -> new ArrayList<>())
                                                        .add(node);
                                            }
                                        });

                        visitInputs(node);
                    }
                };
        execGraph.getRootNodes().forEach(node -> node.accept(dynamicFilteringScanCollector));

        for (Map.Entry<BatchExecTableSourceScan, List<ExecNode<?>>> entry :
                dynamicFilteringScanDescendants.entrySet()) {
            if (entry.getValue().size() == 1) {
                ExecNode<?> next = entry.getValue().get(0);
                if (next instanceof BatchExecMultipleInput) {
                    // the source can be chained with BatchExecMultipleInput
                    continue;
                }
            }
            // otherwise we need dependencies
            entry.getKey().setNeedDynamicFilteringDependency(true);
        }

        return execGraph;
    }
}
