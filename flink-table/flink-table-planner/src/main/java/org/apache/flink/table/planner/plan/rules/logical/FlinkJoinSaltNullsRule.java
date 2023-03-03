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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.fun.SqlNullifFunction;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.calcite.sql.fun.SqlNullifFunction;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 */
public class FlinkJoinSaltNullsRule extends RelRule<FlinkJoinSaltNullsRule.Config>
        implements TransformationRule {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkJoinSaltNullsRule.class);
    private static final String LEFT_SALT_NAME = "__rubisalt_left";
    private static final String RIGHT_SALT_NAME = "__rubisalt_right";

    public static final FlinkJoinSaltNullsRule INSTANCE =
            FlinkJoinSaltNullsRule.Config.DEFAULT.toRule();

    /** Creates a JoinToMultiJoinRule. */
    public FlinkJoinSaltNullsRule(Config config) {
        super(config);
    }

    @Deprecated // to be removed before 2.0
    public FlinkJoinSaltNullsRule(Class<? extends Join> clazz) {
        this(Config.DEFAULT.withOperandFor(clazz));
    }

    @Deprecated // to be removed before 2.0
    public FlinkJoinSaltNullsRule(
            Class<? extends Join> joinClass, RelBuilderFactory relBuilderFactory) {
        this(
                Config.DEFAULT
                        .withRelBuilderFactory(relBuilderFactory)
                        .as(Config.class)
                        .withOperandFor(joinClass));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Join join = call.rel(0);
        final JoinInfo joinInfo = join.analyzeCondition();

        // look for left equijoins that we have not transformed yet
        if (join.getJoinType() == JoinRelType.LEFT && joinInfo.isEqui()) {
            for (String fieldName : join.getRowType().getFieldNames()) {
                if (fieldName.equals(LEFT_SALT_NAME) || fieldName.equals(RIGHT_SALT_NAME)) {
                    // optimization already applied, skip
                    return false;
                }
            }
            return true;
        }

        return false;
    }

    private RexNode generateSaltHashExpr(
        RelBuilder relBuilder,
        RexBuilder rexBuilder,
        RelNode relNode) {
        final List<RelDataTypeField> fields = relNode.getRowType().getFieldList();
        final RelDataType intType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
        final List<RexNode> hashExprs = new ArrayList<>();

        for (RelDataTypeField field : fields) {
            BasicSqlType type = (BasicSqlType) field.getType();
            RexInputRef inputRef = new RexInputRef(field.getIndex(), field.getType());
            RexNode expr = relBuilder.call(FlinkSqlOperatorTable.HASH_CODE, inputRef);
            if (type.isNullable()) {
                expr = relBuilder.call(
                    FlinkSqlOperatorTable.IF,
                    relBuilder.isNull(inputRef),
                    rexBuilder.makeLiteral(0, intType, false),
                    expr);
            }
            hashExprs.add(expr);
        }

        if (hashExprs.size() > 1) {
            return relBuilder.call(FlinkSqlOperatorTable.PLUS, hashExprs);
        } else {
            return hashExprs.get(0);
        }
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Join origJoin = call.rel(0);
        final RelNode origLeft = call.rel(1);
        final RelNode origRight = call.rel(2);
        final RelBuilder relBuilder = call.builder();
        final RexBuilder rexBuilder = origJoin.getCluster().getRexBuilder();
        final RexNode origJoinCondition = origJoin.getCondition();
        final RelDataType intType = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);

        List<String> leftFieldNames = new ArrayList<>(origLeft.getRowType().getFieldNames());
        leftFieldNames.add(LEFT_SALT_NAME);

        List<RexNode> leftNullChecks = new ArrayList<>();
        origJoinCondition.accept(new RexShuttle() {
            public RexNode visitInputRef(RexInputRef node) {
                if (node.getIndex() < origLeft.getRowType().getFieldCount()) {
                    leftNullChecks.add(relBuilder.call(FlinkSqlOperatorTable.IS_NULL, node));
                }
                return node;
            }
        });

        RexNode leftSaltExpr =
            relBuilder.call(FlinkSqlOperatorTable.CASE,
                            relBuilder.or(leftNullChecks),
                            generateSaltHashExpr(relBuilder, rexBuilder, origLeft),
                            rexBuilder.makeLiteral(0, intType, false));

        RelNode leftSaltedProject =
            relBuilder
                .push(origLeft)
                .project(Iterables.concat(relBuilder.fields(), ImmutableList.of(leftSaltExpr)), leftFieldNames, true)
                .build();

        List<String> rightFieldNames = new ArrayList<>(origRight.getRowType().getFieldNames());
        rightFieldNames.add(RIGHT_SALT_NAME);

        List<RexNode> rightNullChecks = new ArrayList<>();
        origJoinCondition.accept(new RexShuttle() {
            public RexNode visitInputRef(RexInputRef node) {
                if (node.getIndex() >= origLeft.getRowType().getFieldCount()) {
                    rightNullChecks.add(relBuilder.call(FlinkSqlOperatorTable.IS_NULL, node));
                }
                return node;
            }
        });

        RexNode rightSaltExpr =
            relBuilder
                .call(FlinkSqlOperatorTable.CASE,
                        relBuilder.or(rightNullChecks),
                        generateSaltHashExpr(relBuilder, rexBuilder, origRight),
                        rexBuilder.makeLiteral(0, intType, false))
                .accept(new RexShuttle() {
                        public RexNode visitInputRef(RexInputRef node) {
                            if (node.getIndex() >= origLeft.getRowType().getFieldCount()) {
                                return new RexInputRef(node.getIndex() - origLeft.getRowType().getFieldCount(), node.getType());
                            } else {
                                return node;
                            }
                        }
                });

        RelNode rightSaltedProject =
            relBuilder
                .push(origRight)
                .project(Iterables.concat(relBuilder.fields(), ImmutableList.of(rightSaltExpr)), rightFieldNames, true)
                .build();

        // adjust right references by 1 to accomodate the salt field
        RexNode saltyJoinCondition = origJoinCondition.accept(new RexShuttle() {
            public RexNode visitInputRef(RexInputRef node) {
                if (node.getIndex() >= origLeft.getRowType().getFieldCount()) {
                    return new RexInputRef(node.getIndex() + 1, node.getType());
                } else {
                    return node;
                }
            }
        });

        RexNode leftSaltRef = relBuilder.push(leftSaltedProject).field(LEFT_SALT_NAME);
        RexNode rightSaltRef = relBuilder.push(rightSaltedProject).field(RIGHT_SALT_NAME);
        rightSaltRef = new RexInputRef(
            ((RexInputRef)rightSaltRef).getIndex() + leftSaltedProject.getRowType().getFieldCount(),
            rightSaltRef.getType());

        saltyJoinCondition = relBuilder.and(saltyJoinCondition, relBuilder.equals(leftSaltRef, rightSaltRef));

        final Join saltyJoin = origJoin.copy(
            origJoin.getTraitSet(),
            saltyJoinCondition,
            leftSaltedProject,
            rightSaltedProject,
            origJoin.getJoinType(),
            false);

        final RelNode saltyProject =
            relBuilder
                .push(saltyJoin)
                .projectExcept(relBuilder.fields(ImmutableList.of(LEFT_SALT_NAME, RIGHT_SALT_NAME)))
                .build();

        call.transformTo(saltyProject);
    }

    /** Rule configuration. */
    public interface Config extends RelRule.Config {
        Config DEFAULT = EMPTY.as(Config.class).withOperandFor(LogicalJoin.class);

        @Override
        default FlinkJoinSaltNullsRule toRule() {
            return new FlinkJoinSaltNullsRule(this);
        }

        /** Defines an operand tree for the given classes. */
        default Config withOperandFor(Class<? extends Join> joinClass) {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(joinClass)
                                            .inputs(
                                                    b1 -> b1.operand(RelNode.class).anyInputs(),
                                                    b2 -> b2.operand(RelNode.class).anyInputs()))
                    .as(Config.class);
        }
    }
}
