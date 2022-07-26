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

import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;

import java.util.Collections;
import java.util.List;

/**
 * A relational operator that performs nested-loop joins.
 *
 * <p>Temporarily copy from calcite to cherry-pick [CALCITE-5107] and will be removed when upgrade
 * the latest calcite.
 *
 * <p>It behaves like a kind of {@link org.apache.calcite.rel.core.Join}, but works by setting
 * variables in its environment and restarting its right-hand input.
 *
 * <p>A LogicalCorrelate is used to represent a correlated query. One implementation strategy is to
 * de-correlate the expression.
 *
 * @see org.apache.calcite.rel.core.CorrelationId
 */
public final class LogicalCorrelate extends Correlate {
    // ~ Instance fields --------------------------------------------------------

    // ~ Constructors -----------------------------------------------------------

    /**
     * Creates a LogicalCorrelate.
     *
     * @param cluster cluster this relational expression belongs to
     * @param hints hints for this node
     * @param left left input relational expression
     * @param right right input relational expression
     * @param correlationId variable name for the row of left input
     * @param requiredColumns Required columns
     * @param joinType join type
     */
    public LogicalCorrelate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode left,
            RelNode right,
            CorrelationId correlationId,
            ImmutableBitSet requiredColumns,
            JoinRelType joinType) {
        super(cluster, traitSet, hints, left, right, correlationId, requiredColumns, joinType);
        assert !CalciteSystemProperty.DEBUG.value() || isValid(Litmus.THROW, null);
    }

    /**
     * Creates a LogicalCorrelate.
     *
     * @param cluster cluster this relational expression belongs to
     * @param left left input relational expression
     * @param right right input relational expression
     * @param correlationId variable name for the row of left input
     * @param requiredColumns Required columns
     * @param joinType join type
     */
    public LogicalCorrelate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            CorrelationId correlationId,
            ImmutableBitSet requiredColumns,
            JoinRelType joinType) {
        this(
                cluster,
                traitSet,
                Collections.emptyList(),
                left,
                right,
                correlationId,
                requiredColumns,
                joinType);
    }

    /** Creates a LogicalCorrelate by parsing serialized output. */
    public LogicalCorrelate(RelInput input) {
        this(
                input.getCluster(),
                input.getTraitSet(),
                input.getInputs().get(0),
                input.getInputs().get(1),
                new CorrelationId((Integer) input.get("correlation")),
                input.getBitSet("requiredColumns"),
                input.getEnum("joinType", JoinRelType.class));
    }

    /** Creates a LogicalCorrelate. */
    public static LogicalCorrelate create(
            RelNode left,
            RelNode right,
            List<RelHint> hints,
            CorrelationId correlationId,
            ImmutableBitSet requiredColumns,
            JoinRelType joinType) {
        final RelOptCluster cluster = left.getCluster();
        final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
        return new LogicalCorrelate(
                cluster, traitSet, hints, left, right, correlationId, requiredColumns, joinType);
    }

    /** Creates a LogicalCorrelate. */
    public static LogicalCorrelate create(
            RelNode left,
            RelNode right,
            CorrelationId correlationId,
            ImmutableBitSet requiredColumns,
            JoinRelType joinType) {
        final RelOptCluster cluster = left.getCluster();
        final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
        return new LogicalCorrelate(
                cluster, traitSet, left, right, correlationId, requiredColumns, joinType);
    }

    // ~ Methods ----------------------------------------------------------------

    @Override
    public LogicalCorrelate copy(
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            CorrelationId correlationId,
            ImmutableBitSet requiredColumns,
            JoinRelType joinType) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new LogicalCorrelate(
                getCluster(),
                traitSet,
                hints,
                left,
                right,
                correlationId,
                requiredColumns,
                joinType);
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelNode withHints(List<RelHint> hintList) {
        return new LogicalCorrelate(
                getCluster(),
                traitSet,
                hintList,
                left,
                right,
                correlationId,
                requiredColumns,
                joinType);
    }
}
