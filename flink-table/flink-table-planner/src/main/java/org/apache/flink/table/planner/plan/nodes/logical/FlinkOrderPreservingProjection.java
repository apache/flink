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

package org.apache.flink.table.planner.plan.nodes.logical;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

public class FlinkOrderPreservingProjection extends Project {

    private final @NonNull Project delegate;

    public FlinkOrderPreservingProjection(@NonNull Project delegate) {
        super(
                delegate.getCluster(),
                delegate.getTraitSet(),
                delegate.getHints(),
                delegate.getInput(),
                delegate.getProjects(),
                delegate.getRowType(),
                delegate.getVariablesSet());
        this.delegate = delegate;
    }

    public Project getDelegate() {
        return delegate;
    }

    @Override
    public Project copy(
            RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new FlinkOrderPreservingProjection(
                delegate.copy(traitSet, input, projects, rowType));
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return delegate.explainTerms(pw);
    }

    @Override
    public List<RelNode> getInputs() {
        return delegate.getInputs();
    }

    @Override
    public String getRelTypeName() {
        return delegate.getRelTypeName();
    }

    @Override
    public void replaceInput(int ordinalInParent, RelNode p) {
        delegate.replaceInput(ordinalInParent, p);
        this.input = delegate.getInput();
        recomputeDigest();
    }
}
