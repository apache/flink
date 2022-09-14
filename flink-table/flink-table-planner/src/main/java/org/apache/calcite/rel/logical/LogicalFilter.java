/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Litmus;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Filter} not targeted at any particular engine or
 * calling convention.
 *
 * <p>Temporarily copy from calcite to cherry-pick [CALCITE-5107] and will be removed when upgrade
 * the latest calcite.
 */
public final class LogicalFilter extends Filter {
    private final com.google.common.collect.ImmutableSet<CorrelationId> variablesSet;

    // ~ Constructors -----------------------------------------------------------

    /**
     * Creates a LogicalFilter.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param hints Hints for this node
     * @param child Input relational expression
     * @param condition Boolean expression which determines whether a row is allowed to pass
     * @param variablesSet Correlation variables set by this relational expression to be used by
     *     nested expressions
     */
    public LogicalFilter(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode child,
            RexNode condition,
            com.google.common.collect.ImmutableSet<CorrelationId> variablesSet) {
        super(cluster, traitSet, hints, child, condition);
        this.variablesSet = Objects.requireNonNull(variablesSet);
        assert isValid(Litmus.THROW, null);
    }

    public LogicalFilter(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            RexNode condition,
            com.google.common.collect.ImmutableSet<CorrelationId> variablesSet) {
        this(
                cluster,
                traitSet,
                com.google.common.collect.ImmutableList.of(),
                child,
                condition,
                variablesSet);
    }

    @Deprecated // to be removed before 2.0
    public LogicalFilter(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode condition) {
        this(cluster, traitSet, child, condition, com.google.common.collect.ImmutableSet.of());
    }

    @Deprecated // to be removed before 2.0
    public LogicalFilter(RelOptCluster cluster, RelNode child, RexNode condition) {
        this(
                cluster,
                cluster.traitSetOf(Convention.NONE),
                child,
                condition,
                com.google.common.collect.ImmutableSet.of());
    }

    /** Creates a LogicalFilter by parsing serialized output. */
    public LogicalFilter(RelInput input) {
        super(input);
        this.variablesSet = com.google.common.collect.ImmutableSet.of();
    }

    /** Creates a LogicalFilter. */
    public static LogicalFilter create(final RelNode input, RexNode condition) {
        return create(input, condition, com.google.common.collect.ImmutableSet.of());
    }

    /** Creates a LogicalFilter. */
    public static LogicalFilter create(
            final RelNode input,
            RexNode condition,
            com.google.common.collect.ImmutableSet<CorrelationId> variablesSet) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet =
                cluster.traitSetOf(Convention.NONE)
                        .replaceIfs(
                                RelCollationTraitDef.INSTANCE,
                                () -> RelMdCollation.filter(mq, input))
                        .replaceIf(
                                RelDistributionTraitDef.INSTANCE,
                                () -> RelMdDistribution.filter(mq, input));
        return new LogicalFilter(cluster, traitSet, input, condition, variablesSet);
    }

    // ~ Methods ----------------------------------------------------------------

    @Override
    public Set<CorrelationId> getVariablesSet() {
        return variablesSet;
    }

    public LogicalFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new LogicalFilter(getCluster(), traitSet, hints, input, condition, variablesSet);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).itemIf("variablesSet", variablesSet, !variablesSet.isEmpty());
    }

    @Override
    public boolean deepEquals(Object obj) {
        return deepEquals0(obj) && variablesSet.equals(((LogicalFilter) obj).variablesSet);
    }

    @Override
    public int deepHashCode() {
        return Objects.hash(deepHashCode0(), variablesSet);
    }

    @Override
    public RelNode withHints(List<RelHint> hintList) {
        return new LogicalFilter(getCluster(), traitSet, hintList, input, condition, variablesSet);
    }
}
