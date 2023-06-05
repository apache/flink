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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.delegation.Planner;
import org.apache.flink.table.planner.plan.fusion.OpFusionCodegenSpecGenerator;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitor;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/** {@link TestingBatchExecNode} for testing purpose. */
public class TestingBatchExecNode implements BatchExecNode<RowData> {

    private final String description;
    private final List<ExecEdge> inputEdges;
    private final List<InputProperty> inputProperties;

    public TestingBatchExecNode(String description) {
        this.description = description;
        this.inputEdges = new ArrayList<>();
        this.inputProperties = new ArrayList<>();
    }

    public void addInput(ExecNode<?> input) {
        addInput(input, InputProperty.DEFAULT);
    }

    public void addInput(ExecNode<?> input, InputProperty inputProperty) {
        inputEdges.add(ExecEdge.builder().source(input).target(this).build());
        inputProperties.add(inputProperty);
    }

    public List<ExecNode<?>> getInputNodes() {
        return inputEdges.stream().map(ExecEdge::getSource).collect(Collectors.toList());
    }

    @Override
    public int getId() {
        return 0;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public LogicalType getOutputType() {
        return RowType.of();
    }

    @Override
    public List<InputProperty> getInputProperties() {
        return inputProperties;
    }

    @Override
    public List<ExecEdge> getInputEdges() {
        return inputEdges;
    }

    @Override
    public void setInputEdges(List<ExecEdge> inputEdges) {
        this.inputEdges.clear();
        this.inputEdges.addAll(inputEdges);
    }

    @Override
    public void replaceInputEdge(int index, ExecEdge newInputEdge) {
        checkArgument(index >= 0 && index < inputEdges.size());
        inputEdges.set(index, newInputEdge);
    }

    @Override
    public Transformation<RowData> translateToPlan(Planner planner) {
        throw new TableException("Unsupported operation.");
    }

    @Override
    public void accept(ExecNodeVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public void setCompiled(boolean isCompiled) {
        throw new TableException("Unsupported operation.");
    }

    @Override
    public String toString() {
        return description;
    }

    @Override
    public boolean supportFusionCodegen() {
        return false;
    }

    @Override
    public OpFusionCodegenSpecGenerator translateToFusionCodegenSpec(Planner planner) {
        throw new UnsupportedOperationException();
    }
}
