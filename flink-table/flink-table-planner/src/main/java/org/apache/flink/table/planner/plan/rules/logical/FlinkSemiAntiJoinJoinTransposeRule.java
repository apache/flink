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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * This rules is copied from Calcite's {@link
 * org.apache.calcite.rel.rules.SemiJoinJoinTransposeRule}. Modification: - Match Join with SEMI or
 * ANTI type instead of SemiJoin - Unsupported (NOT) EXISTS with uncorrelation. e.g. SELECT * FROM
 * x, y WHERE x.c = y.f AND EXISTS (SELECT * FROM z) - Unsupported keys in SEMI/ANTI Join condition
 * are from both Join's left and Join's right e.g. SELECT * FROM x, y WHERE x.c = y.f AND x.a IN
 * (SELECT z.i FROM z WHERE y.e = z.j)
 */

/**
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.SemiJoin} down in a tree past a
 * {@link org.apache.calcite.rel.core.Join} in order to trigger other rules that will convert {@code
 * SemiJoin}s.
 *
 * <ul>
 *   <li>SemiJoin(LogicalJoin(X, Y), Z) &rarr; LogicalJoin(SemiJoin(X, Z), Y)
 *   <li>SemiJoin(LogicalJoin(X, Y), Z) &rarr; LogicalJoin(X, SemiJoin(Y, Z))
 * </ul>
 *
 * <p>Whether this first or second conversion is applied depends on which operands actually
 * participate in the semi-join.
 */
public class FlinkSemiAntiJoinJoinTransposeRule extends RelOptRule {
    public static final FlinkSemiAntiJoinJoinTransposeRule INSTANCE =
            new FlinkSemiAntiJoinJoinTransposeRule();

    // ~ Constructors -----------------------------------------------------------

    /** Creates a FlinkSemiAntiJoinJoinTransposeRule. */
    private FlinkSemiAntiJoinJoinTransposeRule() {
        super(operand(LogicalJoin.class, some(operand(LogicalJoin.class, any()))));
    }

    // ~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin semiAntiJoin = call.rel(0);
        if (semiAntiJoin.getJoinType() != JoinRelType.SEMI
                && semiAntiJoin.getJoinType() != JoinRelType.ANTI) {
            return;
        }

        final Join join = call.rel(1);
        if (join.getJoinType() == JoinRelType.SEMI || join.getJoinType() == JoinRelType.ANTI) {
            return;
        }
        // TODO support other join type
        if (join.getJoinType() != JoinRelType.INNER) {
            return;
        }

        // unsupported cases:
        // 1. (NOT) EXISTS with uncorrelation
        // 2. keys in SemiJoin condition are from both Join's left and Join's right

        Pair<ImmutableBitSet, ImmutableBitSet> inputRefs =
                getSemiAntiJoinConditionInputRefs(semiAntiJoin);
        final ImmutableBitSet leftInputRefs = inputRefs.left;
        final ImmutableBitSet rightInputRefs = inputRefs.right;
        // unsupported case1. (NOT) EXISTS with uncorrelation
        // e.g. SELECT * FROM x, y WHERE x.c = y.f AND EXISTS (SELECT * FROM z)
        // SemiJoin may be push to both Join's left and Join's right.
        // TODO currently we does not handle this
        if (leftInputRefs.isEmpty() || rightInputRefs.isEmpty()) {
            return;
        }

        // X is the left child of the join below the semi-join
        // Y is the right child of the join below the semi-join
        // Z is the right child of the semi-join
        int nFieldsX = join.getLeft().getRowType().getFieldList().size();
        int nFieldsY = join.getRight().getRowType().getFieldList().size();
        int nFieldsZ = semiAntiJoin.getRight().getRowType().getFieldList().size();
        int nTotalFields = nFieldsX + nFieldsY + nFieldsZ;
        List<RelDataTypeField> fields = new ArrayList<RelDataTypeField>();

        // create a list of fields for the full join result; note that
        // we can't simply use the fields from the semi-join because the
        // row-type of a semi-join only includes the left hand side fields
        List<RelDataTypeField> joinFields = semiAntiJoin.getRowType().getFieldList();
        for (int i = 0; i < (nFieldsX + nFieldsY); i++) {
            fields.add(joinFields.get(i));
        }
        joinFields = semiAntiJoin.getRight().getRowType().getFieldList();
        for (int i = 0; i < nFieldsZ; i++) {
            fields.add(joinFields.get(i));
        }

        // determine which operands below the semi-join are the actual
        // Rels that participate in the semi-join
        int nKeysFromX = 0;
        int nKeysFromY = 0;
        for (int leftKey : leftInputRefs) {
            if (leftKey < nFieldsX) {
                nKeysFromX++;
            } else {
                nKeysFromY++;
            }
        }

        // unsupported case2. keys in SemiJoin condition are from both Join's left and Join's right
        // e.g. SELECT * FROM x, y WHERE x.c = y.f AND x.a IN (SELECT z.i FROM z WHERE y.e = z.j)
        if (nKeysFromX > 0 && nKeysFromY > 0) {
            return;
        }

        // the keys must all originate from either the left or right;
        // otherwise, a semi-join wouldn't have been created
        assert (nKeysFromX == 0) || (nKeysFromX == leftInputRefs.cardinality());
        assert (nKeysFromY == 0) || (nKeysFromY == leftInputRefs.cardinality());

        // need to convert the semi/anti join condition and possibly the keys
        RexNode newSemiAntiJoinFilter;
        int[] adjustments = new int[nTotalFields];
        if (nKeysFromX > 0) {
            // (X, Y, Z) --> (X, Z, Y)
            // semiJoin(X, Z)
            // pass 0 as Y's adjustment because there shouldn't be any
            // references to Y in the semi/anti join filter
            setJoinAdjustments(adjustments, nFieldsX, nFieldsY, nFieldsZ, 0, -nFieldsY);
            newSemiAntiJoinFilter =
                    semiAntiJoin
                            .getCondition()
                            .accept(
                                    new RelOptUtil.RexInputConverter(
                                            semiAntiJoin.getCluster().getRexBuilder(),
                                            fields,
                                            adjustments));
        } else {
            // (X, Y, Z) --> (X, Y, Z)
            // semiJoin(Y, Z)
            setJoinAdjustments(adjustments, nFieldsX, nFieldsY, nFieldsZ, -nFieldsX, -nFieldsX);
            newSemiAntiJoinFilter =
                    semiAntiJoin
                            .getCondition()
                            .accept(
                                    new RelOptUtil.RexInputConverter(
                                            semiAntiJoin.getCluster().getRexBuilder(),
                                            fields,
                                            adjustments));
        }

        // create the new join
        RelNode newSemiAntiJoinLeft;
        if (nKeysFromX > 0) {
            newSemiAntiJoinLeft = join.getLeft();
        } else {
            newSemiAntiJoinLeft = join.getRight();
        }
        Join newSemiAntiJoin =
                LogicalJoin.create(
                        newSemiAntiJoinLeft,
                        semiAntiJoin.getRight(),
                        join.getHints(),
                        newSemiAntiJoinFilter,
                        semiAntiJoin.getVariablesSet(),
                        semiAntiJoin.getJoinType());

        RelNode leftJoinRel;
        RelNode rightJoinRel;
        if (nKeysFromX > 0) {
            leftJoinRel = newSemiAntiJoin;
            rightJoinRel = join.getRight();
        } else {
            leftJoinRel = join.getLeft();
            rightJoinRel = newSemiAntiJoin;
        }

        RelNode newJoinRel =
                join.copy(
                        join.getTraitSet(),
                        join.getCondition(),
                        leftJoinRel,
                        rightJoinRel,
                        join.getJoinType(),
                        join.isSemiJoinDone());

        call.transformTo(newJoinRel);
    }

    /**
     * Sets an array to reflect how much each index corresponding to a field needs to be adjusted.
     * The array corresponds to fields in a 3-way join between (X, Y, and Z). X remains unchanged,
     * but Y and Z need to be adjusted by some fixed amount as determined by the input.
     *
     * @param adjustments array to be filled out
     * @param nFieldsX number of fields in X
     * @param nFieldsY number of fields in Y
     * @param nFieldsZ number of fields in Z
     * @param adjustY the amount to adjust Y by
     * @param adjustZ the amount to adjust Z by
     */
    private void setJoinAdjustments(
            int[] adjustments, int nFieldsX, int nFieldsY, int nFieldsZ, int adjustY, int adjustZ) {
        for (int i = 0; i < nFieldsX; i++) {
            adjustments[i] = 0;
        }
        for (int i = nFieldsX; i < (nFieldsX + nFieldsY); i++) {
            adjustments[i] = adjustY;
        }
        for (int i = nFieldsX + nFieldsY; i < (nFieldsX + nFieldsY + nFieldsZ); i++) {
            adjustments[i] = adjustZ;
        }
    }

    private Pair<ImmutableBitSet, ImmutableBitSet> getSemiAntiJoinConditionInputRefs(
            Join semiAntiJoin) {
        final int leftInputFieldCount = semiAntiJoin.getLeft().getRowType().getFieldCount();
        final ImmutableBitSet.Builder leftInputBitSet = ImmutableBitSet.builder();
        final ImmutableBitSet.Builder rightInputBitSet = ImmutableBitSet.builder();
        semiAntiJoin
                .getCondition()
                .accept(
                        new RexVisitorImpl<Void>(true) {
                            public Void visitInputRef(RexInputRef inputRef) {
                                int index = inputRef.getIndex();
                                if (index < leftInputFieldCount) {
                                    leftInputBitSet.set(index);
                                } else {
                                    rightInputBitSet.set(index);
                                }
                                return null;
                            }
                        });
        return new Pair<>(leftInputBitSet.build(), rightInputBitSet.build());
    }
}

// End FlinkSemiAntiJoinJoinTransposeRule.java
