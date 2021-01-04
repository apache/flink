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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitor;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for {@link ExecNode}.
 *
 * @param <T> The type of the elements that result from this node.
 */
public abstract class ExecNodeBase<T> implements ExecNode<T> {

    private final String description;
    private final List<ExecEdge> inputEdges;
    private final LogicalType outputType;
    // TODO remove this field once edge support `source` and `target`,
    //  and then we can get/set `inputNodes` through `inputEdges`.
    private List<ExecNode<?>> inputNodes;

    private transient Transformation<T> transformation;

    protected ExecNodeBase(List<ExecEdge> inputEdges, LogicalType outputType, String description) {
        this.inputEdges = new ArrayList<>(checkNotNull(inputEdges));
        this.outputType = checkNotNull(outputType);
        this.description = checkNotNull(description);
    }

    @Override
    public String getDesc() {
        return description;
    }

    @Override
    public LogicalType getOutputType() {
        return outputType;
    }

    @Override
    public List<ExecNode<?>> getInputNodes() {
        checkNotNull(inputNodes, "inputNodes should not be null, please call setInputNodes first.");
        return inputNodes;
    }

    @Override
    public List<ExecEdge> getInputEdges() {
        return checkNotNull(inputEdges, "inputEdges should not be null.");
    }

    // TODO remove this method once edge support `source` and `target`,
    //  and then we can get/set `inputNodes` through `inputEdges`.
    public void setInputNodes(List<ExecNode<?>> inputNodes) {
        checkArgument(checkNotNull(inputNodes).size() == checkNotNull(inputEdges).size());
        this.inputNodes = new ArrayList<>(inputNodes);
    }

    @Override
    public void replaceInputNode(int ordinalInParent, ExecNode<?> newInputNode) {
        checkArgument(ordinalInParent >= 0 && ordinalInParent < inputNodes.size());
        inputNodes.set(ordinalInParent, newInputNode);
    }

    @Override
    public void replaceInputEdge(int ordinalInParent, ExecEdge newInputEdge) {
        checkArgument(ordinalInParent >= 0 && ordinalInParent < inputEdges.size());
        inputEdges.set(ordinalInParent, newInputEdge);
    }

    public Transformation<T> translateToPlan(Planner planner) {
        if (transformation == null) {
            transformation = translateToPlanInternal((PlannerBase) planner);
        }
        return transformation;
    }

    /** Internal method, translates this node into a Flink operator. */
    protected abstract Transformation<T> translateToPlanInternal(PlannerBase planner);

    @Override
    public void accept(ExecNodeVisitor visitor) {
        visitor.visit(this);
    }

    /** Whether there is singleton exchange node as input. */
    protected boolean inputsContainSingleton() {
        return getInputNodes().stream()
                .anyMatch(
                        i ->
                                i instanceof CommonExecExchange
                                        && i.getInputEdges().get(0).getRequiredShuffle().getType()
                                                == ExecEdge.ShuffleType.SINGLETON);
    }
}
