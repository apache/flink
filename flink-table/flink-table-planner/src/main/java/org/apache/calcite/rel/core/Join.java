/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.rel.core;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexChecker;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;
import org.apiguardian.api.API;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Relational expression that combines two relational expressions according to some condition.
 *
 * <p>Each output row has columns from the left and right inputs. The set of output rows is a subset
 * of the cartesian product of the two inputs; precisely which subset depends on the join condition.
 *
 * <p>The class is copied from Calcite because of 1.40.0 regression at CALCITE-6904, fixed in 1.42.0
 * (CALCITE-7327) Flink modification at lines: 105 ~ 107
 */
public abstract class Join extends BiRel implements Hintable {
    // ~ Instance fields --------------------------------------------------------

    protected final RexNode condition;
    protected final ImmutableSet<CorrelationId> variablesSet;
    protected final ImmutableList<RelHint> hints;

    /**
     * Values must be of enumeration {@link JoinRelType}, except that {@link JoinRelType#RIGHT} is
     * disallowed.
     */
    protected final JoinRelType joinType;

    protected final JoinInfo joinInfo;

    // ~ Constructors -----------------------------------------------------------

    /**
     * Creates a Join.
     *
     * @param cluster Cluster
     * @param traitSet Trait set
     * @param hints Hints
     * @param left Left input
     * @param right Right input
     * @param condition Join condition
     * @param joinType Join type
     * @param variablesSet variables that are set by the LHS and used by the RHS and are not
     *     available to nodes above this Join in the tree
     */
    protected Join(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
        super(cluster, traitSet, left, right);
        this.condition = requireNonNull(condition, "condition");
        this.variablesSet = ImmutableSet.copyOf(variablesSet);
        this.joinType = requireNonNull(joinType, "joinType");
        // FLINK MODIFICATION BEGIN
        this.joinInfo = JoinInfo.of(left, right, condition);
        // FLINK MODIFICATION END
        this.hints = ImmutableList.copyOf(hints);
    }

    @Deprecated // to be removed before 2.0
    protected Join(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
        this(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
    }

    @Deprecated // to be removed before 2.0
    protected Join(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType,
            Set<String> variablesStopped) {
        this(
                cluster,
                traitSet,
                ImmutableList.of(),
                left,
                right,
                condition,
                CorrelationId.setOf(variablesStopped),
                joinType);
    }

    // ~ Methods ----------------------------------------------------------------

    @Override
    public RelNode accept(RexShuttle shuttle) {
        RexNode condition = shuttle.apply(this.condition);
        if (this.condition == condition) {
            return this;
        }
        return copy(traitSet, condition, left, right, joinType, isSemiJoinDone());
    }

    public RexNode getCondition() {
        return condition;
    }

    public JoinRelType getJoinType() {
        return joinType;
    }

    @Override
    public boolean isValid(Litmus litmus, @Nullable Context context) {
        if (!super.isValid(litmus, context)) {
            return false;
        }
        if (getRowType().getFieldCount()
                != getSystemFieldList().size()
                        + left.getRowType().getFieldCount()
                        + (joinType.projectsRight() ? right.getRowType().getFieldCount() : 0)) {
            return litmus.fail("field count mismatch");
        }
        if (condition != null) {
            if (condition.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
                return litmus.fail("condition must be boolean: {}", condition.getType());
            }
            // The input to the condition is a row type consisting of system
            // fields, left fields, and right fields. Very similar to the
            // output row type, except that fields have not yet been made due
            // due to outer joins.
            RexChecker checker =
                    new RexChecker(
                            getCluster()
                                    .getTypeFactory()
                                    .builder()
                                    .addAll(getSystemFieldList())
                                    .addAll(getLeft().getRowType().getFieldList())
                                    .addAll(getRight().getRowType().getFieldList())
                                    .build(),
                            context,
                            litmus);
            condition.accept(checker);
            if (checker.getFailureCount() > 0) {
                return litmus.fail(
                        checker.getFailureCount() + " failures in condition " + condition);
            }
        }
        return litmus.succeed();
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // Maybe we should remove this for semi-join?
        if (isSemiJoin()) {
            // REVIEW jvs 9-Apr-2006:  Just for now...
            return planner.getCostFactory().makeTinyCost();
        }
        double rowCount = mq.getRowCount(this);
        return planner.getCostFactory().makeCost(rowCount, 0, 0);
    }

    // CHECKSTYLE: IGNORE 1
    /**
     * @deprecated Use {@link RelMdUtil#getJoinRowCount(RelMetadataQuery, Join, RexNode)}.
     */
    @Deprecated // to be removed before 2.0
    public static double estimateJoinedRows(Join joinRel, RexNode condition) {
        final RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();
        return Util.first(RelMdUtil.getJoinRowCount(mq, joinRel, condition), 1D);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return Util.first(RelMdUtil.getJoinRowCount(mq, this, condition), 1D);
    }

    @Override
    public Set<CorrelationId> getVariablesSet() {
        return variablesSet;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("condition", condition)
                .item("joinType", joinType.lowerName)
                .itemIf("variablesSet", variablesSet, !variablesSet.isEmpty())
                .itemIf("systemFields", getSystemFieldList(), !getSystemFieldList().isEmpty());
    }

    @API(since = "1.24", status = API.Status.INTERNAL)
    @EnsuresNonNullIf(expression = "#1", result = true)
    protected boolean deepEquals0(@Nullable Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Join o = (Join) obj;
        return traitSet.equals(o.traitSet)
                && left.deepEquals(o.left)
                && right.deepEquals(o.right)
                && condition.equals(o.condition)
                && joinType == o.joinType
                && hints.equals(o.hints)
                && getRowType().equalsSansFieldNames(o.getRowType());
    }

    @API(since = "1.24", status = API.Status.INTERNAL)
    protected int deepHashCode0() {
        return Objects.hash(
                traitSet, left.deepHashCode(), right.deepHashCode(), condition, joinType, hints);
    }

    @Override
    protected RelDataType deriveRowType() {
        return SqlValidatorUtil.deriveJoinRowType(
                left.getRowType(),
                right.getRowType(),
                joinType,
                getCluster().getTypeFactory(),
                null,
                getSystemFieldList());
    }

    /**
     * Returns whether this LogicalJoin has already spawned a {@code SemiJoin} via {@link
     * org.apache.calcite.rel.rules.JoinAddRedundantSemiJoinRule}.
     *
     * <p>The base implementation returns false.
     *
     * @return whether this join has already spawned a semi join
     */
    public boolean isSemiJoinDone() {
        return false;
    }

    /**
     * Returns whether this Join is a semijoin.
     *
     * @return true if this Join's join type is semi.
     */
    public boolean isSemiJoin() {
        return joinType == JoinRelType.SEMI;
    }

    /**
     * Returns a list of system fields that will be prefixed to output row type.
     *
     * @return list of system fields
     */
    public List<RelDataTypeField> getSystemFieldList() {
        return Collections.emptyList();
    }

    @Deprecated // to be removed before 2.0
    public static RelDataType deriveJoinRowType(
            RelDataType leftType,
            RelDataType rightType,
            JoinRelType joinType,
            RelDataTypeFactory typeFactory,
            @Nullable List<String> fieldNameList,
            List<RelDataTypeField> systemFieldList) {
        return SqlValidatorUtil.deriveJoinRowType(
                leftType, rightType, joinType, typeFactory, fieldNameList, systemFieldList);
    }

    @Deprecated // to be removed before 2.0
    public static RelDataType createJoinType(
            RelDataTypeFactory typeFactory,
            RelDataType leftType,
            RelDataType rightType,
            List<String> fieldNameList,
            List<RelDataTypeField> systemFieldList) {
        return SqlValidatorUtil.createJoinType(
                typeFactory, leftType, rightType, fieldNameList, systemFieldList);
    }

    @Override
    public Join copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.size() == 2;
        return copy(
                traitSet, getCondition(), inputs.get(0), inputs.get(1), joinType, isSemiJoinDone());
    }

    /**
     * Creates a copy of this join, overriding condition, system fields and inputs.
     *
     * <p>General contract as {@link RelNode#copy}.
     *
     * @param traitSet Traits
     * @param conditionExpr Condition
     * @param left Left input
     * @param right Right input
     * @param joinType Join type
     * @param semiJoinDone Whether this join has been translated to a semi-join
     * @return Copy of this join
     */
    public abstract Join copy(
            RelTraitSet traitSet,
            RexNode conditionExpr,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone);

    /**
     * Analyzes the join condition.
     *
     * @return Analyzed join condition
     */
    public JoinInfo analyzeCondition() {
        return joinInfo;
    }

    @Override
    public ImmutableList<RelHint> getHints() {
        return hints;
    }
}
