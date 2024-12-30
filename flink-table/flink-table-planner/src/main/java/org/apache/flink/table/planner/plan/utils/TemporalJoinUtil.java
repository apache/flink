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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.spec.JoinSpec;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin;
import org.apache.flink.table.runtime.types.PlannerTypeUtils;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.mapping.IntPair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.planner.plan.utils.TemporalTableJoinUtil.isRowTimeTemporalTableJoinCondition;

/** Utilities for temporal join. */
public class TemporalJoinUtil {

    // ----------------------------------------------------------------------------------------
    //                          Temporal Join Condition Utilities
    // ----------------------------------------------------------------------------------------

    /**
     * {@link #TEMPORAL_JOIN_CONDITION} is a specific join condition which correctly defines
     * references to rightTimeAttribute, rightPrimaryKeyExpression and leftTimeAttribute. The
     * condition is used to mark this is a temporal table join and ensure columns these expressions
     * depends on will not be pruned.
     *
     * <p>The join key pair is necessary for temporal table join to ensure the condition will not be
     * pushed down.
     *
     * <p>The rightTimeAttribute, rightPrimaryKeyExpression and leftTimeAttribute will be extracted
     * from the condition in physical phase.
     */
    public static final SqlFunction TEMPORAL_JOIN_CONDITION =
            new SqlFunction(
                    "__TEMPORAL_JOIN_CONDITION",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    null,
                    OperandTypes.or(
                            /**
                             * ------------------------ Temporal table join condition
                             * ------------------------*
                             */
                            // right time attribute and primary key are required in event-time
                            // temporal table join,
                            OperandTypes.sequence(
                                    "'(LEFT_TIME_ATTRIBUTE, RIGHT_TIME_ATTRIBUTE, PRIMARY_KEY, LEFT_KEY, RIGHT_KEY)'",
                                    OperandTypes.DATETIME,
                                    OperandTypes.DATETIME,
                                    OperandTypes.ANY,
                                    OperandTypes.ANY,
                                    OperandTypes.ANY),
                            // right primary key is required for processing-time temporal table join
                            OperandTypes.sequence(
                                    "'(LEFT_TIME_ATTRIBUTE, PRIMARY_KEY, LEFT_KEY, RIGHT_KEY)'",
                                    OperandTypes.DATETIME,
                                    OperandTypes.ANY,
                                    OperandTypes.ANY,
                                    OperandTypes.ANY),
                            /**
                             * ------------------ Temporal table function join condition
                             * ---------------------*
                             */
                            // Event-time temporal function join condition
                            OperandTypes.sequence(
                                    "'(LEFT_TIME_ATTRIBUTE, RIGHT_TIME_ATTRIBUTE, PRIMARY_KEY)'",
                                    OperandTypes.DATETIME,
                                    OperandTypes.DATETIME,
                                    OperandTypes.ANY),
                            // Processing-time temporal function join condition
                            OperandTypes.sequence(
                                    "'(LEFT_TIME_ATTRIBUTE, PRIMARY_KEY)'",
                                    OperandTypes.DATETIME,
                                    OperandTypes.ANY)),
                    SqlFunctionCategory.SYSTEM);

    /**
     * Initial temporal condition used in rewrite phase of logical plan, this condition will be
     * replaced with {@link #TEMPORAL_JOIN_CONDITION} after the primary key inferred.
     */
    public static final SqlFunction INITIAL_TEMPORAL_JOIN_CONDITION =
            new SqlFunction(
                    "__INITIAL_TEMPORAL_JOIN_CONDITION",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    null,
                    OperandTypes.or(
                            // initial Event-time temporal table join condition, will fill
                            // PRIMARY_KEY later,
                            OperandTypes.sequence(
                                    "'(LEFT_TIME_ATTRIBUTE, RIGHT_TIME_ATTRIBUTE, LEFT_KEY, RIGHT_KEY)'",
                                    OperandTypes.DATETIME,
                                    OperandTypes.DATETIME,
                                    OperandTypes.ANY,
                                    OperandTypes.ANY),
                            // initial Processing-time temporal table join condition, will fill
                            // PRIMARY_KEY later,
                            OperandTypes.sequence(
                                    "'(LEFT_TIME_ATTRIBUTE, LEFT_KEY, RIGHT_KEY)'",
                                    OperandTypes.DATETIME,
                                    OperandTypes.ANY,
                                    OperandTypes.ANY)),
                    SqlFunctionCategory.SYSTEM);

    public static final SqlFunction TEMPORAL_JOIN_LEFT_KEY =
            new SqlFunction(
                    "__TEMPORAL_JOIN_LEFT_KEY",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    null,
                    OperandTypes.ARRAY,
                    SqlFunctionCategory.SYSTEM);

    public static final SqlFunction TEMPORAL_JOIN_RIGHT_KEY =
            new SqlFunction(
                    "__TEMPORAL_JOIN_RIGHT_KEY",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    null,
                    OperandTypes.ARRAY,
                    SqlFunctionCategory.SYSTEM);

    public static final SqlFunction TEMPORAL_JOIN_CONDITION_PRIMARY_KEY =
            new SqlFunction(
                    "__TEMPORAL_JOIN_CONDITION_PRIMARY_KEY",
                    SqlKind.OTHER_FUNCTION,
                    ReturnTypes.BOOLEAN_NOT_NULL,
                    null,
                    OperandTypes.ARRAY,
                    SqlFunctionCategory.SYSTEM);

    private static RexNode makePrimaryKeyCall(
            RexBuilder rexBuilder, List<RexNode> rightPrimaryKeyExpression) {
        return rexBuilder.makeCall(TEMPORAL_JOIN_CONDITION_PRIMARY_KEY, rightPrimaryKeyExpression);
    }

    private static RexNode makeLeftJoinKeyCall(RexBuilder rexBuilder, List<RexNode> keyExpression) {
        return rexBuilder.makeCall(TEMPORAL_JOIN_LEFT_KEY, keyExpression);
    }

    private static RexNode makeRightJoinKeyCall(
            RexBuilder rexBuilder, List<RexNode> keyExpression) {
        return rexBuilder.makeCall(TEMPORAL_JOIN_RIGHT_KEY, keyExpression);
    }

    public static RexNode makeProcTimeTemporalFunctionJoinConCall(
            RexBuilder rexBuilder, RexNode leftTimeAttribute, RexNode rightPrimaryKeyExpression) {
        return rexBuilder.makeCall(
                TEMPORAL_JOIN_CONDITION,
                leftTimeAttribute,
                makePrimaryKeyCall(rexBuilder, Arrays.asList(rightPrimaryKeyExpression)));
    }

    public static RexNode makeRowTimeTemporalFunctionJoinConCall(
            RexBuilder rexBuilder,
            RexNode leftTimeAttribute,
            RexNode rightTimeAttribute,
            RexNode rightPrimaryKeyExpression) {
        return rexBuilder.makeCall(
                TEMPORAL_JOIN_CONDITION,
                leftTimeAttribute,
                rightTimeAttribute,
                makePrimaryKeyCall(rexBuilder, Arrays.asList(rightPrimaryKeyExpression)));
    }

    public static RexNode makeInitialRowTimeTemporalTableJoinCondCall(
            RexBuilder rexBuilder,
            RexNode leftTimeAttribute,
            RexNode rightTimeAttribute,
            List<RexNode> leftJoinKeyExpression,
            List<RexNode> rightJoinKeyExpression) {
        return rexBuilder.makeCall(
                INITIAL_TEMPORAL_JOIN_CONDITION,
                leftTimeAttribute,
                rightTimeAttribute,
                makeLeftJoinKeyCall(rexBuilder, leftJoinKeyExpression),
                makeRightJoinKeyCall(rexBuilder, rightJoinKeyExpression));
    }

    public static RexNode makeRowTimeTemporalTableJoinConCall(
            RexBuilder rexBuilder,
            RexNode leftTimeAttribute,
            RexNode rightTimeAttribute,
            List<RexNode> rightPrimaryKeyExpression,
            List<RexNode> leftJoinKeyExpression,
            List<RexNode> rightJoinKeyExpression) {
        return rexBuilder.makeCall(
                TEMPORAL_JOIN_CONDITION,
                leftTimeAttribute,
                rightTimeAttribute,
                makePrimaryKeyCall(rexBuilder, rightPrimaryKeyExpression),
                makeLeftJoinKeyCall(rexBuilder, leftJoinKeyExpression),
                makeRightJoinKeyCall(rexBuilder, rightJoinKeyExpression));
    }

    public static RexNode makeInitialProcTimeTemporalTableJoinConCall(
            RexBuilder rexBuilder,
            RexNode leftTimeAttribute,
            List<RexNode> leftJoinKeyExpression,
            List<RexNode> rightJoinKeyExpression) {
        return rexBuilder.makeCall(
                INITIAL_TEMPORAL_JOIN_CONDITION,
                leftTimeAttribute,
                makeLeftJoinKeyCall(rexBuilder, leftJoinKeyExpression),
                makeRightJoinKeyCall(rexBuilder, rightJoinKeyExpression));
    }

    public static RexNode makeProcTimeTemporalTableJoinConCall(
            RexBuilder rexBuilder,
            RexNode leftTimeAttribute,
            List<RexNode> rightPrimaryKeyExpression,
            List<RexNode> leftJoinKeyExpression,
            List<RexNode> rightJoinKeyExpression) {
        return rexBuilder.makeCall(
                TEMPORAL_JOIN_CONDITION,
                leftTimeAttribute,
                makePrimaryKeyCall(rexBuilder, rightPrimaryKeyExpression),
                makeLeftJoinKeyCall(rexBuilder, leftJoinKeyExpression),
                makeRightJoinKeyCall(rexBuilder, rightJoinKeyExpression));
    }

    public static boolean isInitialRowTimeTemporalTableJoin(RexCall rexCall) {
        // (LEFT_TIME_ATTRIBUTE, RIGHT_TIME_ATTRIBUTE, LEFT_KEY, RIGHT_KEY)
        return rexCall.getOperator().equals(INITIAL_TEMPORAL_JOIN_CONDITION)
                && rexCall.getOperands().size() == 4;
    }

    public static boolean isInitialProcTimeTemporalTableJoin(RexCall rexCall) {
        // (LEFT_TIME_ATTRIBUTE, LEFT_KEY, RIGHT_KEY)
        return rexCall.getOperator().equals(INITIAL_TEMPORAL_JOIN_CONDITION)
                && rexCall.getOperands().size() == 3;
    }

    private static boolean containsTemporalJoinCondition(RexNode condition) {
        final boolean[] hasTemporalJoinCondition = {false};
        condition.accept(
                new RexVisitorImpl<Void>(true) {
                    @Override
                    public Void visitCall(RexCall call) {
                        if (!call.getOperator().equals(TEMPORAL_JOIN_CONDITION)
                                && !call.getOperator().equals(INITIAL_TEMPORAL_JOIN_CONDITION)) {
                            return super.visitCall(call);
                        } else {
                            hasTemporalJoinCondition[0] = true;
                            return null;
                        }
                    }
                });
        return hasTemporalJoinCondition[0];
    }

    public static boolean containsInitialTemporalJoinCondition(RexNode condition) {
        final boolean[] hasInitialTemporalJoinCondition = {false};
        condition.accept(
                new RexVisitorImpl<Void>(true) {
                    @Override
                    public Void visitCall(RexCall call) {
                        if (!call.getOperator().equals(INITIAL_TEMPORAL_JOIN_CONDITION)) {
                            return super.visitCall(call);
                        } else {
                            hasInitialTemporalJoinCondition[0] = true;
                            return null;
                        }
                    }
                });

        return hasInitialTemporalJoinCondition[0];
    }

    public static boolean isRowTimeJoin(JoinSpec joinSpec) {
        RexNode nonEquiJoinRex = joinSpec.getNonEquiCondition().orElse(null);
        RowTimeJoinVisitor visitor = new RowTimeJoinVisitor();
        if (nonEquiJoinRex != null) {
            nonEquiJoinRex.accept(visitor);
        }
        return visitor.isRowTimeJoin();
    }

    private static class RowTimeJoinVisitor extends RexVisitorImpl<Void> {
        private boolean isRowTimeJoin = false;

        protected RowTimeJoinVisitor() {
            super(true);
        }

        @Override
        public Void visitCall(RexCall call) {
            if (isRowTimeTemporalTableJoinCondition(call)
                    || isRowTimeTemporalFunctionJoinCon(call)) {
                isRowTimeJoin = true;
            } else {
                super.visitCall(call);
            }
            return null;
        }

        public boolean isRowTimeJoin() {
            return isRowTimeJoin;
        }
    }

    public static boolean isRowTimeTemporalFunctionJoinCon(RexCall rexCall) {
        // (LEFT_TIME_ATTRIBUTE, RIGHT_TIME_ATTRIBUTE, PRIMARY_KEY)
        return TEMPORAL_JOIN_CONDITION.equals(rexCall.getOperator())
                && rexCall.getOperands().size() == 3;
    }

    private static class TemporalFunctionJoinVisitor extends RexVisitorImpl<Void> {
        private boolean isTemporalFunctionJoin = false;

        protected TemporalFunctionJoinVisitor() {
            super(true);
        }

        @Override
        public Void visitCall(RexCall call) {
            if (isTemporalFunctionCon(call)) {
                isTemporalFunctionJoin = true;
            } else {
                super.visitCall(call);
            }
            return null;
        }

        public boolean isTemporalFunctionJoin() {
            return isTemporalFunctionJoin;
        }
    }

    public static boolean isTemporalFunctionJoin(RexBuilder rexBuilder, JoinInfo joinInfo) {
        RexNode nonEquiJoinRex = joinInfo.getRemaining(rexBuilder);
        TemporalFunctionJoinVisitor visitor = new TemporalFunctionJoinVisitor();
        if (nonEquiJoinRex != null) {
            nonEquiJoinRex.accept(visitor);
        }
        return visitor.isTemporalFunctionJoin();
    }

    public static boolean isTemporalFunctionCon(RexCall rexCall) {
        // (LEFT_TIME_ATTRIBUTE, PRIMARY_KEY)
        // (LEFT_TIME_ATTRIBUTE, RIGHT_TIME_ATTRIBUTE, PRIMARY_KEY)
        return rexCall.getOperator().equals(TEMPORAL_JOIN_CONDITION)
                && (rexCall.getOperands().size() == 2 || rexCall.getOperands().size() == 3);
    }

    public static void validateTemporalFunctionCondition(
            RexCall call,
            RexNode leftTimeAttribute,
            Optional<RexNode> rightTimeAttribute,
            Optional<RexNode[]> rightPrimaryKey,
            int rightKeysStartingOffset,
            JoinSpec joinSpec,
            String textualRepresentation) {

        if (isTemporalFunctionCon(call)) {
            validateTemporalFunctionPrimaryKey(
                    rightKeysStartingOffset, rightPrimaryKey, joinSpec, textualRepresentation);

            if (rightTimeAttribute.isPresent()
                    && !FlinkTypeFactory.isRowtimeIndicatorType(
                            rightTimeAttribute.get().getType())) {
                throw new ValidationException(
                        "Non rowtime timeAttribute ["
                                + rightTimeAttribute.get().getType()
                                + "] used to create TemporalTableFunction");
            }

            if (!FlinkTypeFactory.isRowtimeIndicatorType(leftTimeAttribute.getType())) {
                throw new ValidationException(
                        "Non rowtime timeAttribute ["
                                + leftTimeAttribute.getType()
                                + "] passed as the argument to TemporalTableFunction");
            }
        } else {
            validateTemporalFunctionPrimaryKey(
                    rightKeysStartingOffset, rightPrimaryKey, joinSpec, textualRepresentation);

            if (!FlinkTypeFactory.isProctimeIndicatorType(leftTimeAttribute.getType())) {
                throw new ValidationException(
                        "Non processing timeAttribute ["
                                + leftTimeAttribute.getType()
                                + "] passed as the argument to TemporalTableFunction");
            }
        }
    }

    private static void validateTemporalFunctionPrimaryKey(
            int rightKeysStartingOffset,
            Optional<RexNode[]> rightPrimaryKeyOpt,
            JoinSpec joinInfo,
            String textualRepresentation) {

        if (joinInfo.getRightKeys().length != 1) {
            throw new ValidationException(
                    "Only single column join key is supported. Found "
                            + joinInfo.getRightKeys()
                            + " in ["
                            + textualRepresentation
                            + "]");
        }

        if (!rightPrimaryKeyOpt.isPresent() || rightPrimaryKeyOpt.get().length != 1) {
            throw new ValidationException(
                    "Only single primary key is supported. Found "
                            + rightPrimaryKeyOpt
                            + " in ["
                            + textualRepresentation
                            + "]");
        }

        RexNode pk = rightPrimaryKeyOpt.get()[0];
        int rightJoinKeyInputReference = joinInfo.getRightKeys()[0] + rightKeysStartingOffset;
        int rightPrimaryKeyInputReference = extractInputRef(pk, textualRepresentation);

        if (rightPrimaryKeyInputReference != rightJoinKeyInputReference) {
            throw new ValidationException(
                    "Join key ["
                            + rightJoinKeyInputReference
                            + "] must be the same as "
                            + "temporal table's primary key ["
                            + pk
                            + "] "
                            + "in ["
                            + textualRepresentation
                            + "]");
        }
    }

    /**
     * Gets the join key pairs from left input field index to temporal table field index.
     *
     * @param joinInfo the join information of temporal table join
     * @param calcOnTemporalTable the calc programs on temporal table
     */
    public static IntPair[] getTemporalTableJoinKeyPairs(
            JoinInfo joinInfo, Optional<RexProgram> calcOnTemporalTable) {
        IntPair[] joinPairs = joinInfo.pairs().toArray(new IntPair[0]);
        if (calcOnTemporalTable.isPresent()) {
            // the target key of joinInfo is the calc output fields, we have to remapping to table
            // here
            List<IntPair> keyPairs = new ArrayList<>();
            Arrays.stream(joinPairs)
                    .forEach(
                            p -> {
                                int calcSrcIdx =
                                        getIdenticalSourceField(
                                                calcOnTemporalTable.get(), p.target);
                                if (calcSrcIdx != -1) {
                                    keyPairs.add(new IntPair(p.source, calcSrcIdx));
                                }
                            });
            return keyPairs.toArray(new IntPair[0]);
        } else {
            return joinPairs;
        }
    }

    // this is highly inspired by Calcite's RexProgram#getSourceField(int)
    private static int getIdenticalSourceField(RexProgram rexProgram, int outputOrdinal) {
        assert (outputOrdinal >= 0 && outputOrdinal < rexProgram.getProjectList().size());
        RexLocalRef project = rexProgram.getProjectList().get(outputOrdinal);
        int index = project.getIndex();
        while (true) {
            RexNode expr = rexProgram.getExprList().get(index);
            if (expr instanceof RexCall) {
                RexCall call = (RexCall) expr;
                if (call.getOperator() == SqlStdOperatorTable.IN_FENNEL) {
                    // drill through identity function
                    expr = call.getOperands().get(0);
                } else if (call.getOperator() == SqlStdOperatorTable.CAST) {
                    // drill through identity function
                    RelDataType outputType = call.getType();
                    RelDataType inputType = call.getOperands().get(0).getType();
                    boolean isCompatible =
                            PlannerTypeUtils.isInteroperable(
                                    FlinkTypeFactory.toLogicalType(outputType),
                                    FlinkTypeFactory.toLogicalType(inputType));
                    if (isCompatible) {
                        expr = call.getOperands().get(0);
                    }
                }
            }
            if (expr instanceof RexLocalRef) {
                RexLocalRef ref = (RexLocalRef) expr;
                index = ref.getIndex();
            } else if (expr instanceof RexInputRef) {
                RexInputRef ref = (RexInputRef) expr;
                return ref.getIndex();
            } else {
                return -1;
            }
        }
    }

    public static int extractInputRef(RexNode rexNode, String textualRepresentation) {
        InputRefVisitor inputReferenceVisitor = new InputRefVisitor();
        rexNode.accept(inputReferenceVisitor);
        Preconditions.checkState(
                inputReferenceVisitor.getFields().length == 1,
                "Failed to find input reference in [%s]",
                textualRepresentation);

        return inputReferenceVisitor.getFields()[0];
    }

    /**
     * Check whether input join node satisfy preconditions to convert into temporal join.
     *
     * @param join input join to analyze.
     * @return True if input join node satisfy preconditions to convert into temporal join, else
     *     false.
     */
    public static boolean satisfyTemporalJoin(FlinkLogicalJoin join) {
        return satisfyTemporalJoin(join, join.getLeft(), join.getRight());
    }

    public static boolean satisfyTemporalJoin(
            FlinkLogicalJoin join, RelNode newLeft, RelNode newRight) {
        if (!containsTemporalJoinCondition(join.getCondition())) {
            return false;
        }
        JoinInfo joinInfo = JoinInfo.of(newLeft, newRight, join.getCondition());
        if (isTemporalFunctionJoin(join.getCluster().getRexBuilder(), joinInfo)) {
            // Temporal table function join currently only supports INNER JOIN
            return join.getJoinType() == JoinRelType.INNER;
        } else {
            // Temporal table join currently only supports INNER JOIN and LEFT JOIN
            return join.getJoinType() == JoinRelType.INNER
                    || join.getJoinType() == JoinRelType.LEFT;
        }
    }
}
