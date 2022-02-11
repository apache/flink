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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitor;
import org.apache.flink.table.planner.plan.utils.ExecNodeMetadataUtil;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

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

    private final String description;

    private final LogicalType outputType;

    private final List<InputProperty> inputProperties;

    private List<ExecEdge> inputEdges;

    private transient Transformation<T> transformation;

    /** Holds the context information (id, name, version) as deserialized from a JSON plan. */
    @JsonProperty(value = FIELD_NAME_TYPE, access = JsonProperty.Access.WRITE_ONLY)
    private final ExecNodeContext context;

    /**
     * Retrieves the default context from the {@link ExecNodeMetadata} annotation to be serialized
     * into the JSON plan.
     */
    @JsonProperty(value = FIELD_NAME_TYPE, access = JsonProperty.Access.READ_ONLY, index = 1)
    protected final ExecNodeContext getContextFromAnnotation() {
        return ExecNodeContext.newContext(this.getClass()).withId(getId());
    }

    protected ExecNodeBase(
            int id,
            ExecNodeContext context,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description) {
        this.context = checkNotNull(context).withId(id);
        this.inputProperties = checkNotNull(inputProperties);
        this.outputType = checkNotNull(outputType);
        this.description = checkNotNull(description);
    }

    @Override
    public final int getId() {
        return context.getId();
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

    @JsonIgnore
    protected String getSimplifiedName() {
        return getClass().getSimpleName().replace("StreamExec", "").replace("BatchExec", "");
    }

    protected String createTransformationUid(String operatorName) {
        return context.generateUid(operatorName);
    }

    protected String createTransformationName(TableConfig config) {
        return createTransformationName(config.getConfiguration());
    }

    protected String createTransformationName(ReadableConfig config) {
        return createFormattedTransformationName(getDescription(), getSimplifiedName(), config);
    }

    protected String createTransformationDescription(TableConfig config) {
        return createTransformationDescription(config.getConfiguration());
    }

    protected String createTransformationDescription(ReadableConfig config) {
        return createFormattedTransformationDescription(getDescription(), config);
    }

    protected TransformationMetadata createTransformationMeta(
            String operatorName, TableConfig config) {
        return createTransformationMeta(operatorName, config.getConfiguration());
    }

    protected TransformationMetadata createTransformationMeta(
            String operatorName, ReadableConfig config) {
        if (ExecNodeMetadataUtil.isUnsupported(this.getClass())) {
            return new TransformationMetadata(
                    createTransformationName(config), createTransformationDescription(config));
        } else {
            // Only classes supporting metadata util need to set the uid
            return new TransformationMetadata(
                    createTransformationUid(operatorName),
                    createTransformationName(config),
                    createTransformationDescription(config));
        }
    }

    protected TransformationMetadata createTransformationMeta(
            String operatorName, String detailName, String simplifiedName, ReadableConfig config) {
        final String name = createFormattedTransformationName(detailName, simplifiedName, config);
        final String desc = createFormattedTransformationDescription(detailName, config);
        if (ExecNodeMetadataUtil.isUnsupported(this.getClass())) {
            return new TransformationMetadata(name, desc);
        } else {
            // Only classes supporting metadata util need to set the uid
            return new TransformationMetadata(createTransformationUid(operatorName), name, desc);
        }
    }

    protected String createFormattedTransformationDescription(
            String description, ReadableConfig config) {
        if (config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_SIMPLIFY_OPERATOR_NAME_ENABLED)) {
            return String.format("[%d]:%s", getId(), description);
        }
        return description;
    }

    protected String createFormattedTransformationName(
            String detailName, String simplifiedName, ReadableConfig config) {
        if (config.get(OptimizerConfigOptions.TABLE_OPTIMIZER_SIMPLIFY_OPERATOR_NAME_ENABLED)) {
            return String.format("%s[%d]", simplifiedName, getId());
        }
        return detailName;
    }
}
