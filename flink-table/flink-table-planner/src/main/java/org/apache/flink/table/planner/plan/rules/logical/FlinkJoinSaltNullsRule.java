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
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * For outer joins, we run into data skew problems when the join condition is sparse,
 * creating a scenario where NULL keys are all distributed to a single task. It makes
 * the pipeline single-threaded and underutilized.
 *
 * The optimization strategy we use here is to generate a salt value that we add to
 * the join key. The salt value is 0 if the join keys are not null, but if any of the
 * keys are null, then we generate a salt value that's a hash of the other keys in
 * the input row.
 *
 * The strategy works because when any of the columns in an equijoin are NULL, the
 * join expression becomes NULL (i.e. not True), so it doesn't matter whether the
 * salt expressions on both sides evaluate to true or false. But we will use the
 * salt expression as part of the distribution key, fixing the work imbalance.
 *
 * The following example demostrates the effect of the optimization. Notice the
 * following changes to the plan:
 *   1. We compute __rubisalt_left and __rubisalt_right on the join inputs
 *   2. The Exchange into the Join node now includes the salt fields
 *   3. We test inputs for NULLness at runtime, conditionally generating a
 *      a salt value as a hash of other avaiable fields in the row
 *         CASE(OR(IS NULL(a), IS NULL(b)), HASH_CODE(id), 0) AS __rubisalt_left
 *         CASE(OR(IS NULL(c), IS NULL(a)), HASH_CODE(id), 0) AS __rubisalt_right
 *   4. The Join condition also gets a conjuction to compare salt values.
 *         AND(=(a, c), =(b, a0), =(__rubisalt_left, __rubisalt_right))
 *
 *   If none of the join keys are null, then the salt values as 0 and compare as true.
 *   If any of the join keys are null, then the join expression will always fail,
 *   the salt values will be non-zero, but it's OK that the salt values from left
 *   and right side don't line up -- we only need them for the distribution.
 *
 * Flink SQL> explain select f1.a, f2.a
 *       from source_1.foodelta f1
 *       left join source_1.foodelta f2 on f1.a = f2.c and f1.b = f2.a;
 *
 * == Abstract Syntax Tree ==
 * LogicalProject(a=[$1], a0=[$5])
 * +- LogicalJoin(condition=[AND(=($1, $7), =($2, $5))], joinType=[left])
 *    :- LogicalTableScan(table=[[default_catalog, source_1, foodelta]])
 *    +- LogicalTableScan(table=[[default_catalog, source_1, foodelta]])
 *
 * == Optimized Physical Plan ==
 * Calc(select=[a, a0])
 *  +- Join(joinType=[LeftOuterJoin],
 *           where=[AND(=(a, c), =(b, a0), =(__rubisalt_left, __rubisalt_right))],
 *           select=[a, b, __rubisalt_left, a0, c, __rubisalt_right],
 *           leftInputSpec=[NoUniqueKey],
 *           rightInputSpec=[NoUniqueKey])
 *     :- Exchange(distribution=[hash[a, b, __rubisalt_left]])
 *     :  +- Calc(select=[a, b, CASE(OR(IS NULL(a), IS NULL(b)), HASH_CODE(id), 0) AS __rubisalt_left])
 *     :     +- ChangelogNormalize(key=[id])
 *     :        +- Exchange(distribution=[hash[id]])
 *     :           +- DropUpdateBefore
 *     :              +- MiniBatchAssigner(interval=[3000ms], mode=[ProcTime])
 *     :                 +- TableSourceScan(table=[[default_catalog, source_1, foodelta]], fields=[id, a, b, c])
 *     +- Exchange(distribution=[hash[c, a, __rubisalt_right]])
 *        +- Calc(select=[a, c, CASE(OR(IS NULL(c), IS NULL(a)), HASH_CODE(id), 0) AS __rubisalt_right])
 *           +- ChangelogNormalize(key=[id])
 *              +- Exchange(distribution=[hash[id]])
 *                 +- DropUpdateBefore
 *                    +- MiniBatchAssigner(interval=[3000ms], mode=[ProcTime])
 *                       +- TableSourceScan(table=[[default_catalog, source_1, foodelta]], fields=[id, a, b, c])
 *
 * Optimization TODO:
 *  1. Do not include key columns in salt calculation -- they are already part of the distribution key,
 *     so the work to compute a possible hash value is wasted effort. In the example below, columns
 *     c and a are the join key, so there's no value in incorporating them into the salt.
 *       CASE((c IS NULL OR a IS NULL),
 *              (IF(a IS NULL, 0, HASH_CODE(a))
 *             + IF(b IS NULL, 0, HASH_CODE(b))
 *             + IF(c IS NULL, 0, HASH_CODE(c))), 0) AS __rubisalt_right]
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
        List<RexNode> hashExprs = new ArrayList<>();
        final List<RexNode> notNullHashExprs = new ArrayList<>();

        for (RelDataTypeField field : fields) {
            if (!(field.getType() instanceof BasicSqlType)) {
                // we only support basic SQL types for hasing, and more
                // complex types like arrays, maps, etc. are not going to
                // be supported by our hash function
                continue;
            }
            BasicSqlType type = (BasicSqlType) field.getType();
            RexInputRef inputRef = new RexInputRef(field.getIndex(), field.getType());
            RexNode expr = relBuilder.call(FlinkSqlOperatorTable.HASH_CODE, inputRef);
            if (type.isNullable()) {
                expr = relBuilder.call(
                    FlinkSqlOperatorTable.IF,
                    relBuilder.isNull(inputRef),
                    rexBuilder.makeLiteral(0, intType, false),
                    expr);
            } else {
                // maintain a list of exclusively non-nullable expressions that
                // we prefer to use for salt; if we don't have any non-null exprssions,
                // we will fall back to all expressions
                notNullHashExprs.add(expr);
            }
            hashExprs.add(expr);
        }

        if (notNullHashExprs.size() > 0) {
            // use not-null fields only, otherwise use all fields
            hashExprs = notNullHashExprs;
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
