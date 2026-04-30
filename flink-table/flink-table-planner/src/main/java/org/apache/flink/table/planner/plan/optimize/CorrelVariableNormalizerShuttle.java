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

package org.apache.flink.table.planner.plan.optimize;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Normalizes correlation variable ids in a RelNode tree to make equivalent subplans digest-match.
 */
public final class CorrelVariableNormalizerShuttle extends RelShuttleImpl {

    private final Map<Integer, Integer> idMap = new LinkedHashMap<>();

    private final RexBuilder rexBuilder;
    private final RexShuttle rexCorrelNormalizer;

    public CorrelVariableNormalizerShuttle(RexBuilder rexBuilder) {
        this.rexBuilder = rexBuilder;
        rexCorrelNormalizer = new RexCorrelNormalizer();
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        var adjustedId = adjustCorrelationId(correlate.getCorrelationId());
        if (adjustedId.isPresent()) {
            var left = correlate.getLeft().accept(this);
            var right = correlate.getRight().accept(this);
            return correlate.copy(
                    correlate.getTraitSet(),
                    left,
                    right,
                    adjustedId.get(),
                    correlate.getRequiredColumns(),
                    correlate.getJoinType());
        }

        return super.visit(correlate);
    }

    @Override
    public RelNode visit(RelNode relNode) {
        if (relNode instanceof LogicalTableFunctionScan && relNode.getInputs().isEmpty()) {
            // visitChild applies the RexShuttle while walking RelNode inputs. A zero-input table
            // function scan is a leaf, but unlike a regular TableScan it can still contain RexNodes
            // (e.g., UNNEST over a correl variable), so rewrite it explicitly.
            return relNode.accept(rexCorrelNormalizer);
        }

        return super.visit(relNode);
    }

    @Override
    protected RelNode visitChild(RelNode parent, int i, RelNode child) {
        if (i == 0) {
            parent = parent.accept(rexCorrelNormalizer);
            parent = remapVariablesSet(parent);
        }

        return super.visitChild(parent, i, child);
    }

    /**
     * Filter, Project, and Join carry a {@link CorrelationId} set alongside their RexNodes. {@code
     * RelNode.accept(RexShuttle)} only rewrites the RexNodes and preserves the old {@code
     * variablesSet} via {@code copy()}, so ids we just adjusted in the condition/projects are still
     * advertised under their old names. To overcome that, we need to rebuild that variable with the
     * adjusted ids set as well.
     */
    private RelNode remapVariablesSet(RelNode relNode) {
        var oldSet = relNode.getVariablesSet();
        if (oldSet.isEmpty()) {
            return relNode;
        }

        var builder = ImmutableSet.<CorrelationId>builder();
        boolean changed = false;
        for (var id : oldSet) {
            var adjusted = adjustCorrelationId(id);
            if (adjusted.isPresent()) {
                builder.add(adjusted.get());
                changed = true;
            } else {
                builder.add(id);
            }
        }

        if (!changed) {
            return relNode;
        }

        var newSet = builder.build();
        if (relNode instanceof LogicalFilter) {
            var filter = (LogicalFilter) relNode;
            return new LogicalFilter(
                    filter.getCluster(),
                    filter.getTraitSet(),
                    filter.getHints(),
                    filter.getInput(),
                    filter.getCondition(),
                    newSet);
        }

        if (relNode instanceof LogicalProject) {
            var project = (LogicalProject) relNode;
            return new LogicalProject(
                    project.getCluster(),
                    project.getTraitSet(),
                    project.getHints(),
                    project.getInput(),
                    project.getProjects(),
                    project.getRowType(),
                    newSet);
        }

        if (relNode instanceof LogicalJoin) {
            var join = (LogicalJoin) relNode;
            return new LogicalJoin(
                    join.getCluster(),
                    join.getTraitSet(),
                    join.getHints(),
                    join.getLeft(),
                    join.getRight(),
                    join.getCondition(),
                    newSet,
                    join.getJoinType(),
                    join.isSemiJoinDone(),
                    ImmutableList.copyOf(join.getSystemFieldList()));
        }

        return relNode;
    }

    private Optional<CorrelationId> adjustCorrelationId(CorrelationId correlationId) {
        if (correlationId.getName().startsWith(CorrelationId.CORREL_PREFIX)) {
            int oldId = correlationId.getId();
            int newId = idMap.computeIfAbsent(oldId, k -> idMap.size() + 1);
            if (newId != oldId) {
                return Optional.of(new CorrelationId(newId));
            }
        }

        return Optional.empty();
    }

    private final class RexCorrelNormalizer extends RexShuttle {

        @Override
        public RexNode visitCorrelVariable(RexCorrelVariable variable) {
            var adjustedId = adjustCorrelationId(variable.id);
            if (adjustedId.isPresent()) {
                return rexBuilder.makeCorrel(variable.getType(), adjustedId.get());
            } else {
                return super.visitCorrelVariable(variable);
            }
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            // Let the base shuttle rewrite the RexSubQuery's operands first, so any
            // RexCorrelVariables they carry (e.g., the LHS of IN/SOME) are also adjusted.
            var withOperands = (RexSubQuery) super.visitSubQuery(subQuery);
            var rewritten = withOperands.rel.accept(CorrelVariableNormalizerShuttle.this);

            return rewritten == withOperands.rel ? withOperands : withOperands.clone(rewritten);
        }
    }
}
