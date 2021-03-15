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
import org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonDeserializer;
import org.apache.flink.table.planner.plan.nodes.exec.serde.LogicalTypeJsonSerializer;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitor;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.List;

/**
 * The representation of execution information for a {@link FlinkPhysicalRel}.
 *
 * @param <T> The type of the elements that result from this node.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
@Internal
public interface ExecNode<T> extends ExecNodeTranslator<T> {

    String FIELD_NAME_ID = "id";
    String FIELD_NAME_DESCRIPTION = "description";
    String FIELD_NAME_INPUT_PROPERTIES = "inputProperties";
    String FIELD_NAME_OUTPUT_TYPE = "outputType";

    /** Gets the ID of this node. */
    @JsonProperty(value = FIELD_NAME_ID)
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
    @JsonSerialize(using = LogicalTypeJsonSerializer.class)
    @JsonDeserialize(using = LogicalTypeJsonDeserializer.class)
    LogicalType getOutputType();

    /**
     * Returns a list of this node's input properties.
     *
     * <p>NOTE: If there are no inputs, returns an empty list, not null.
     *
     * @return List of this node's input properties.
     */
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
     * Sets the input {@link ExecEdge}s which connect this nodes and its input nodes.
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
}
