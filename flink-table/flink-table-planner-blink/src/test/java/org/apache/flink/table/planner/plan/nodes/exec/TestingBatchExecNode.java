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
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.ExecNodeVisitor;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;

/** {@link TestingBatchExecNode} for testing purpose. */
public class TestingBatchExecNode implements BatchExecNode<RowData> {

    private final List<ExecNode<?>> inputNodes;
    private final List<InputProperty> inputProperties;

    public TestingBatchExecNode() {
        this.inputNodes = new ArrayList<>();
        this.inputProperties = new ArrayList<>();
    }

    public void addInput(ExecNode<?> input) {
        addInput(input, InputProperty.DEFAULT);
    }

    public void addInput(ExecNode<?> input, InputProperty inputProperty) {
        inputNodes.add(input);
        inputProperties.add(inputProperty);
    }

    @Override
    public String getDescription() {
        return "TestingBatchExecNode";
    }

    @Override
    public LogicalType getOutputType() {
        return RowType.of();
    }

    @Override
    public List<ExecNode<?>> getInputNodes() {
        return inputNodes;
    }

    @Override
    public List<InputProperty> getInputProperties() {
        return inputProperties;
    }

    @Override
    public void replaceInputNode(int ordinalInParent, ExecNode<?> newInputNode) {
        inputNodes.set(ordinalInParent, newInputNode);
    }

    @Override
    public Transformation<RowData> translateToPlan(Planner planner) {
        throw new TableException("Unsupported operation.");
    }

    @Override
    public void accept(ExecNodeVisitor visitor) {
        visitor.visit(this);
    }
}
