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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for {@link ExecNode}.
 *
 * @param <T> The type of the elements that result from this node.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class ExecNodeBase<T> implements ExecNode<T> {

    /** The unique identifier for each ExecNode in the json plan. */
    @JsonIgnore private final int id;

    @JsonIgnore private final String description;

    @JsonIgnore private final LogicalType outputType;

    @JsonIgnore private final List<InputProperty> inputProperties;

    @JsonIgnore private List<ExecEdge> inputEdges;

    @JsonIgnore private transient Transformation<T> transformation;

    /** This is used to assign a unique ID to every ExecNode. */
    private static Integer idCounter = 0;

    /** Generate an unique ID for ExecNode. */
    public static int getNewNodeId() {
        idCounter++;
        return idCounter;
    }

    // used for json creator
    protected ExecNodeBase(
            int id,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description) {
        this.id = id;
        this.inputProperties = checkNotNull(inputProperties);
        this.outputType = checkNotNull(outputType);
        this.description = checkNotNull(description);
    }

    protected ExecNodeBase(
            List<InputProperty> inputProperties, LogicalType outputType, String description) {
        this(getNewNodeId(), inputProperties, outputType, description);
    }

    @Override
    public final int getId() {
        return id;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public LogicalType getOutputType() {
        return outputType;
    }

    @Override
    public List<InputProperty> getInputProperties() {
        return inputProperties;
    }

    @Override
    public List<ExecEdge> getInputEdges() {
        return checkNotNull(
                inputEdges,
                "inputEdges should not null, please call `setInputEdges(List<ExecEdge>)` first.");
    }

    @Override
    public void setInputEdges(List<ExecEdge> inputEdges) {
        checkNotNull(inputEdges, "inputEdges should not be null.");
        this.inputEdges = new ArrayList<>(inputEdges);
    }

    @Override
    public void replaceInputEdge(int index, ExecEdge newInputEdge) {
        List<ExecEdge> edges = getInputEdges();
        checkArgument(index >= 0 && index < edges.size());
        edges.set(index, newInputEdge);
    }

    @Override
    public Transformation<T> translateToPlan(Planner planner) {
        if (transformation == null) {
            transformation = translateToPlanInternal((PlannerBase) planner);
            if (this instanceof SingleTransformationTranslator) {
                if (inputsContainSingleton()) {
                    transformation.setParallelism(1);
                    transformation.setMaxParallelism(1);
                }
            }
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
        return getInputEdges().stream()
                .map(ExecEdge::getSource)
                .anyMatch(
                        i ->
                                i instanceof CommonExecExchange
                                        && i.getInputProperties()
                                                        .get(0)
                                                        .getRequiredDistribution()
                                                        .getType()
                                                == InputProperty.DistributionType.SINGLETON);
    }
}
