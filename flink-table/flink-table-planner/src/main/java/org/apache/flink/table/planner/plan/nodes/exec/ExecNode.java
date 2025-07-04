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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitor;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonTypeIdResolver;

import java.util.List;

import static org.apache.flink.table.planner.plan.nodes.exec.ExecNode.FIELD_NAME_TYPE;

/**
 * The representation of execution information for a {@link FlinkPhysicalRel}.
 *
 * @param <T> The type of the elements that result from this node.
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = FIELD_NAME_TYPE,
        visible = true)
@JsonTypeIdResolver(ExecNodeTypeIdResolver.class)
@Internal
public interface ExecNode<T> extends ExecNodeTranslator<T>, FusionCodegenExecNode {

    String FIELD_NAME_ID = "id";
    String FIELD_NAME_TYPE = "type";
    String FIELD_NAME_CONFIGURATION = "configuration";
    String FIELD_NAME_DESCRIPTION = "description";
    String FIELD_NAME_INPUT_PROPERTIES = "inputProperties";
    String FIELD_NAME_OUTPUT_TYPE = "outputType";
    String FIELD_NAME_STATE = "state";

    /** The unique ID of the node. */
    @JsonProperty(value = FIELD_NAME_ID, index = 0)
    int getId();

    /** Returns a string which describes this node. */
    @JsonProperty(value = FIELD_NAME_DESCRIPTION)
    String getDescription();

    /**
     * Returns the output {@link LogicalType} of this node, this type should be consistent with the
     * type parameter {@link T}.
     *
     * <p>Such as, if T is {@link RowData}, the output type should be {@link RowType}. please refer
     * to the JavaDoc of {@link RowData} for more info about mapping of logical types to internal
     * data structures.
     */
    @JsonProperty(value = FIELD_NAME_OUTPUT_TYPE)
    LogicalType getOutputType();

    /**
     * Returns a list of this node's input properties.
     *
     * <p>NOTE: If there are no inputs, returns an empty list, not null.
     *
     * @return List of this node's input properties.
     */
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonProperty(value = FIELD_NAME_INPUT_PROPERTIES)
    List<InputProperty> getInputProperties();

    /**
     * Returns a list of this node's input {@link ExecEdge}s.
     *
     * <p>NOTE: If there are no inputs, returns an empty list, not null.
     */
    @JsonIgnore
    List<ExecEdge> getInputEdges();

    /**
     * Sets the input {@link ExecEdge}s which connect these nodes and its input nodes.
     *
     * <p>NOTE: If there are no inputs, the given inputEdges should be empty, not null.
     *
     * @param inputEdges the input {@link ExecEdge}s.
     */
    @JsonIgnore
    void setInputEdges(List<ExecEdge> inputEdges);

    /**
     * Replaces the <code>ordinalInParent</code><sup>th</sup> input edge.
     *
     * @param index Position of the child input edge, 0 is the first.
     * @param newInputEdge New edge that should be put at position `index`.
     */
    void replaceInputEdge(int index, ExecEdge newInputEdge);

    /**
     * Accepts a visit from a {@link ExecNodeVisitor}.
     *
     * @param visitor ExecNodeVisitor.
     */
    void accept(ExecNodeVisitor visitor);

    /**
     * Declares whether the node has been created as part of a plan compilation. Some translation
     * properties might be impacted by this (e.g. UID generation for transformations).
     */
    void setCompiled(boolean isCompiled);
}
