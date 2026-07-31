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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexTableArgCall;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalLateralSnapshotJoin;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.plan.utils.LateralSnapshotJoinUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.inference.strategies.LateralSnapshotTypeStrategy;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites a {@link FlinkLogicalJoin} whose right side is a {@link FlinkLogicalTableFunctionScan}
 * backed by the built-in {@code SNAPSHOT} function into a dedicated {@link
 * FlinkLogicalLateralSnapshotJoin}. The right-side input becomes the actual TABLE argument of the
 * SNAPSHOT call. The SNAPSHOT-specific arguments (load_completed_condition, load_completed_time,
 * load_completed_idle_timeout, state_ttl) are carried as fields on the new node.
 *
 * <p>By the time this rule fires, Calcite's decorrelator has already converted the original {@code
 * LogicalCorrelate} into a {@code LogicalJoin} (because SNAPSHOT does not actually reference any
 * field of the outer input). The rule therefore matches the join shape directly.
 */
@Value.Enclosing
public class LogicalJoinToLateralSnapshotJoinRule
        extends RelRule<
                LogicalJoinToLateralSnapshotJoinRule.LogicalJoinToLateralSnapshotJoinRuleConfig> {

    public static final LogicalJoinToLateralSnapshotJoinRule INSTANCE =
            LogicalJoinToLateralSnapshotJoinRule.LogicalJoinToLateralSnapshotJoinRuleConfig.DEFAULT
                    .toRule();

    private LogicalJoinToLateralSnapshotJoinRule(
            LogicalJoinToLateralSnapshotJoinRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final FlinkLogicalJoin join = call.rel(0);
        // the rule replaces FlinkLogicalJoin, so it won't fire on its output.
        return findSnapshotScan(join.getRight()) != null;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final FlinkLogicalJoin join = call.rel(0);
        final RelNode leftNode = join.getLeft();
        final FlinkLogicalTableFunctionScan scan = findSnapshotScan(join.getRight());
        if (scan == null) {
            // matches() guarantees a SNAPSHOT scan on the right, so this cannot happen.
            throw new TableException(
                    "Could not find the SNAPSHOT scan on the build side of a LATERAL SNAPSHOT "
                            + "join. This is a bug, please file an issue.");
        }

        // SQL syntax already restricts LATERAL-side joins to INNER/LEFT, this is a defensive check.
        final JoinRelType joinType = join.getJoinType();
        if (joinType != JoinRelType.INNER && joinType != JoinRelType.LEFT) {
            throw new ValidationException(
                    String.format(
                            "LATERAL SNAPSHOT join only supports INNER JOIN and LEFT OUTER JOIN, but was %s JOIN.",
                            joinType));
        }

        // Require at least one equality predicate so the operator can hash-partition both inputs.
        final JoinInfo joinInfo = join.analyzeCondition();
        if (joinInfo.leftKeys.isEmpty()) {
            throw new ValidationException(
                    "LATERAL SNAPSHOT join requires at least one equality predicate.");
        }

        final RexCall snapshotCall = (RexCall) scan.getCall();

        // Resolve the raw build-side TABLE input the operator reads. A null result means the
        // SNAPSHOT call is malformed, which cannot happen for a plan that reached this rule.
        final RelNode rawTableInput = getSnapshotInputTable(scan);
        if (rawTableInput == null) {
            throw new TableException(
                    "Could not resolve the TABLE input of the SNAPSHOT scan on the build side of "
                            + "a LATERAL SNAPSHOT join. This is a bug, please file an issue.");
        }
        // The build-side row-time attribute drives the streaming operator's LOAD phase. In batch
        // all input is bounded and the join degrades to a regular join (see
        // BatchPhysicalLateralSnapshotJoinRule), so no watermark is required.
        if (!ShortcutUtils.unwrapContext(join).isBatchMode()) {
            // The build-side input must declare exactly one watermark, otherwise the operator
            // cannot determine when the LOAD phase is complete.
            final long rowtimeCount =
                    rawTableInput.getRowType().getFieldList().stream()
                            .filter(f -> FlinkTypeFactory.isRowtimeIndicatorType(f.getType()))
                            .count();
            if (rowtimeCount == 0) {
                throw new ValidationException(
                        "LATERAL SNAPSHOT requires a watermark on the build-side input.");
            }
            if (rowtimeCount > 1) {
                throw new ValidationException(
                        String.format(
                                "The build-side input of a LATERAL SNAPSHOT join must not have more than one "
                                        + "row-time attribute, but found %d.",
                                rowtimeCount));
            }
        }

        // Replace the SNAPSHOT TableFunctionScan with its input, preserving any FlinkLogicalCalc
        // nodes that the optimizer placed above the scan.
        final RelNode rightNode = replaceSnapshotScan(join.getRight());
        if (rightNode == null) {
            throw new TableException(
                    "Could not rewrite the build side of a LATERAL SNAPSHOT join by replacing the "
                            + "SNAPSHOT scan with its TABLE input. This is a bug, please file an "
                            + "issue.");
        }

        final List<RexNode> operands = snapshotCall.getOperands();
        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        final RexExecutor executor = join.getCluster().getPlanner().getExecutor();

        // All scalar SNAPSHOT arguments must be constant expressions, so we constant-fold each one
        // and reject anything that does not reduce to a literal. The 'input' TABLE argument
        // (index 0) is exempt.
        final RexLiteral conditionLiteral =
                foldToLiteral(
                        rexBuilder,
                        executor,
                        operands,
                        LateralSnapshotTypeStrategy.LOAD_COMPLETED_CONDITION_ARG_INDEX,
                        LateralSnapshotTypeStrategy.LOAD_COMPLETED_CONDITION_ARG_NAME);
        final RexLiteral loadCompletedTimeLiteral =
                foldToLiteral(
                        rexBuilder,
                        executor,
                        operands,
                        LateralSnapshotTypeStrategy.LOAD_COMPLETED_TIME_ARG_INDEX,
                        LateralSnapshotTypeStrategy.LOAD_COMPLETED_TIME_ARG_NAME);
        final RexLiteral idleTimeoutLiteral =
                foldToLiteral(
                        rexBuilder,
                        executor,
                        operands,
                        LateralSnapshotTypeStrategy.LOAD_COMPLETED_IDLE_TIMEOUT_ARG_INDEX,
                        LateralSnapshotTypeStrategy.LOAD_COMPLETED_IDLE_TIMEOUT_ARG_NAME);
        final RexLiteral stateTtlLiteral =
                foldToLiteral(
                        rexBuilder,
                        executor,
                        operands,
                        LateralSnapshotTypeStrategy.STATE_TTL_ARG_INDEX,
                        LateralSnapshotTypeStrategy.STATE_TTL_ARG_NAME);

        // Resolve load_completed_time according to load_completed_condition. The default
        // 'compile_time' uses the wall-clock time at planning; 'user_time' uses the user-provided
        // load_completed_time (which the type strategy guarantees is present for 'user_time').
        final String condition =
                conditionLiteral == null ? null : conditionLiteral.getValueAs(String.class);
        final Long loadCompletedTime;
        if (condition == null
                || LateralSnapshotTypeStrategy.LOAD_COMPLETED_CONDITION_COMPILE_TIME.equals(
                        condition)) {
            loadCompletedTime = System.currentTimeMillis();
        } else if (LateralSnapshotTypeStrategy.LOAD_COMPLETED_CONDITION_USER_TIME.equals(
                condition)) {
            loadCompletedTime =
                    loadCompletedTimeLiteral == null
                            ? null
                            : loadCompletedTimeLiteral.getValueAs(Long.class);
            if (loadCompletedTime == null) {
                throw new ValidationException(
                        "SNAPSHOT requires 'load_completed_time' when "
                                + "'load_completed_condition' is 'user_time'.");
            }
        } else {
            throw new ValidationException(
                    String.format("Unknown SNAPSHOT 'load_completed_condition': '%s'.", condition));
        }

        // The effective condition (defaulting to 'compile_time') is carried for explain output.
        final String loadCompletedCondition =
                condition == null
                        ? LateralSnapshotTypeStrategy.LOAD_COMPLETED_CONDITION_COMPILE_TIME
                        : condition;
        final Long loadCompletedIdleTimeoutMs =
                intervalMillis(
                        idleTimeoutLiteral,
                        LateralSnapshotTypeStrategy.LOAD_COMPLETED_IDLE_TIMEOUT_ARG_NAME);
        final Long stateTtlMs =
                intervalMillis(stateTtlLiteral, LateralSnapshotTypeStrategy.STATE_TTL_ARG_NAME);

        // The original join condition's field types were resolved against the SNAPSHOT scan's
        // materialized output, but rightNode (its raw TABLE input) still exposes the build-side
        // row-time attribute as an indicator (see replaceSnapshotScan). Retype the condition to
        // the actual left+right input types.
        final List<RelDataTypeField> leftRightFields = new ArrayList<>();
        leftRightFields.addAll(leftNode.getRowType().getFieldList());
        leftRightFields.addAll(rightNode.getRowType().getFieldList());
        final RexNode rebasedCondition =
                join.getCondition()
                        .accept(
                                new RexShuttle() {
                                    @Override
                                    public RexNode visitInputRef(RexInputRef inputRef) {
                                        return RexInputRef.of(inputRef.getIndex(), leftRightFields);
                                    }
                                });

        // Replace the join (over the materialized SNAPSHOT scan) with a dedicated
        // FlinkLogicalLateralSnapshotJoin taking the rewritten SNAPSHOT function input as
        // build-side input.
        final RelNode node =
                FlinkLogicalLateralSnapshotJoin.create(
                        leftNode,
                        rightNode,
                        rebasedCondition,
                        joinType,
                        loadCompletedCondition,
                        loadCompletedTime,
                        loadCompletedIdleTimeoutMs,
                        stateTtlMs);

        final int origRightCount = unwrap(join.getRight()).getRowType().getFieldCount();
        final int newRightCount = rightNode.getRowType().getFieldCount();
        final boolean isRowtimeFieldAdded = newRightCount > origRightCount;
        if (isRowtimeFieldAdded) {
            // If the build-side projection stripped the row-time attribute, replaceSnapshotScan
            // re-appended it as a trailing column so it reaches the operator. In that case the node
            // has extra trailing column(s) that a wrapper Calc projects away to restore the
            // original join's output type. Otherwise, the node's output type already matches the
            // original join.
            final RelDataType originalOutputType = join.getRowType();
            final List<RexNode> wrapperProjects = new ArrayList<>();
            for (int i = 0; i < originalOutputType.getFieldCount(); i++) {
                wrapperProjects.add(rexBuilder.makeInputRef(node, i));
            }
            final RexProgram wrapperProgram =
                    RexProgram.create(
                            node.getRowType(),
                            wrapperProjects,
                            null,
                            originalOutputType.getFieldNames(),
                            rexBuilder);
            call.transformTo(FlinkLogicalCalc.create(node, wrapperProgram));
        } else {
            call.transformTo(node);
        }
    }

    /**
     * Walks down a join's right input looking for a {@link FlinkLogicalTableFunctionScan} whose
     * call is the {@code SNAPSHOT} built-in. Walks past {@link FlinkLogicalCalc} nodes and breaks
     * on any other node type. Returns the {@link FlinkLogicalTableFunctionScan} if found, or null
     * if an unexpected node was observed, the subtree splits up (more than one input), or the tree
     * ends.
     */
    @Nullable
    private static FlinkLogicalTableFunctionScan findSnapshotScan(RelNode root) {
        RelNode current = unwrap(root);
        while (current != null) {
            if (current instanceof FlinkLogicalTableFunctionScan) {
                final FlinkLogicalTableFunctionScan scan = (FlinkLogicalTableFunctionScan) current;
                if (scan.getCall() instanceof RexCall
                        && LateralSnapshotJoinUtil.isSnapshotCall((RexCall) scan.getCall())) {
                    return scan;
                }
                return null;
            }
            // Walk through pass-through nodes (e.g. FlinkLogicalCalc inserted by the optimizer). A
            // Calc always has a single input; the size check is defensive.
            if (current instanceof FlinkLogicalCalc) {
                current = unwrap(current.getInput(0));
            } else {
                return null;
            }
        }
        return null;
    }

    private static RelNode unwrap(RelNode node) {
        return node instanceof HepRelVertex ? ((HepRelVertex) node).getCurrentRel() : node;
    }

    /**
     * Returns the raw (unwrapped) TABLE input of a {@code SNAPSHOT} scan, i.e. the build-side input
     * the operator reads. Returns {@code null} if the scan does not carry a SNAPSHOT call or its
     * TABLE argument cannot be resolved.
     */
    @Nullable
    private static RelNode getSnapshotInputTable(FlinkLogicalTableFunctionScan scan) {
        if (!(scan.getCall() instanceof RexCall)
                || !LateralSnapshotJoinUtil.isSnapshotCall((RexCall) scan.getCall())) {
            return null;
        }
        final RexCall snapshotCall = (RexCall) scan.getCall();
        final RexNode inputArg =
                snapshotCall.getOperands().get(LateralSnapshotTypeStrategy.INPUT_ARG_INDEX);
        if (!(inputArg instanceof RexTableArgCall)) {
            return null;
        }
        final RexTableArgCall tableArg = (RexTableArgCall) inputArg;
        if (tableArg.getInputIndex() < 0 || tableArg.getInputIndex() >= scan.getInputs().size()) {
            return null;
        }
        return unwrap(scan.getInputs().get(tableArg.getInputIndex()));
    }

    /**
     * Walks the right subtree replacing the {@link FlinkLogicalTableFunctionScan} (the SNAPSHOT
     * scan) with the scan's TABLE input, while preserving any {@link FlinkLogicalCalc} nodes
     * stacked above the scan. The SNAPSHOT type strategy materializes the build-side time
     * attributes, so the scan's output type differs from its input's (the build-side row-time
     * attribute is a plain timestamp on the scan output but a row-time indicator on the raw input).
     * Each preserved Calc was built against the materialized scan output, so its {@link RexProgram}
     * is rebased onto the raw (row-time-bearing) input type, which lets the row-time attribute flow
     * through to the operator.
     */
    @Nullable
    private static RelNode replaceSnapshotScan(RelNode node) {
        final RelNode current = unwrap(node);
        if (current instanceof FlinkLogicalTableFunctionScan) {
            // the top node is the TableFunctionScan, return its table input argument
            return getSnapshotInputTable((FlinkLogicalTableFunctionScan) current);
        }
        if (current instanceof FlinkLogicalCalc) {
            // the top node is a calc that needs to be rebased
            final FlinkLogicalCalc calc = (FlinkLogicalCalc) current;
            final RelNode rewrittenInput = replaceSnapshotScan(calc.getInput(0));
            if (rewrittenInput == null) {
                return null;
            }
            return rebaseCalc(calc, rewrittenInput);
        }
        return null;
    }

    /**
     * Rebuilds {@code calc}'s {@link RexProgram} so it reads from {@code newInput} (whose
     * build-side time attributes are still row-time indicators) instead of the materialized
     * SNAPSHOT scan output it was originally built against. Input references are retyped to the new
     * input's field types; the projection/condition expressions and output field names are
     * otherwise preserved.
     *
     * <p>If the projection dropped the build-side row-time attribute, it is re-appended as a
     * trailing column so it is available for the snapshot join operator.
     */
    private static RelNode rebaseCalc(FlinkLogicalCalc calc, RelNode newInput) {
        final RexProgram program = calc.getProgram();
        final RexBuilder rexBuilder = calc.getCluster().getRexBuilder();
        final List<RelDataTypeField> newInputFields = newInput.getRowType().getFieldList();
        final RexShuttle retyper =
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        return new RexInputRef(
                                inputRef.getIndex(),
                                newInputFields.get(inputRef.getIndex()).getType());
                    }
                };
        final List<RexNode> newProjects = new ArrayList<>();
        program.getProjectList().stream()
                .map(r -> program.expandLocalRef(r).accept(retyper))
                .forEach(newProjects::add);

        final RexNode newCondition =
                program.getCondition() == null
                        ? null
                        : program.expandLocalRef(program.getCondition()).accept(retyper);
        final List<String> fieldNames = new ArrayList<>(program.getOutputRowType().getFieldNames());

        // Re-append the build-side row-time attribute if this projection dropped it.
        final boolean exposesRowtime =
                newProjects.stream()
                        .anyMatch(p -> FlinkTypeFactory.isRowtimeIndicatorType(p.getType()));
        if (!exposesRowtime) {
            newInputFields.stream()
                    .filter(f -> FlinkTypeFactory.isRowtimeIndicatorType(f.getType()))
                    .findFirst()
                    .ifPresent(
                            f -> {
                                newProjects.add(new RexInputRef(f.getIndex(), f.getType()));
                                fieldNames.add(uniqueName(f.getName(), fieldNames));
                            });
        }

        final RexProgram newProgram =
                RexProgram.create(
                        newInput.getRowType(), newProjects, newCondition, fieldNames, rexBuilder);
        return calc.copy(calc.getTraitSet(), newInput, newProgram);
    }

    private static String uniqueName(String name, List<String> existing) {
        String candidate = name;
        int suffix = 0;
        while (existing.contains(candidate)) {
            candidate = name + "_" + suffix++;
        }
        return candidate;
    }

    /**
     * Returns the SNAPSHOT argument at {@code index} as a constant {@link RexLiteral}, or {@code
     * null} if the argument is absent (omitted optional arguments are carried as a {@code
     * DEFAULT()} call) or explicitly NULL. The argument may still be carried as a cast or a
     * deterministic function call at this point, so it is constant-folded first. Throws a {@link
     * ValidationException} if the expression cannot be constant-folded.
     */
    @Nullable
    private static RexLiteral foldToLiteral(
            RexBuilder rexBuilder,
            RexExecutor executor,
            List<RexNode> operands,
            int index,
            String argName) {
        if (index >= operands.size()) {
            return null;
        }
        RexNode operand = operands.get(index);
        if (operand.isA(SqlKind.DEFAULT)) {
            // Optional argument not provided by the user.
            return null;
        }
        if (!(operand instanceof RexLiteral)) {
            operand = FlinkRexUtil.simplify(rexBuilder, operand, executor);
        }
        if (!(operand instanceof RexLiteral)) {
            throw new ValidationException(
                    String.format(
                            "Argument '%s' of SNAPSHOT must be a constant expression that can be evaluated at plan time.",
                            argName));
        }
        final RexLiteral literal = (RexLiteral) operand;
        return literal.isNull() ? null : literal;
    }

    /**
     * Returns the value of a folded {@code INTERVAL} literal in milliseconds, or {@code null} if
     * the literal is absent. The function signature guarantees a day-time interval, so {@link
     * RexLiteral#getValueAs} yields milliseconds; a negative duration is not meaningful for these
     * arguments and is rejected.
     */
    @Nullable
    private static Long intervalMillis(@Nullable RexLiteral literal, String argName) {
        if (literal == null) {
            return null;
        }
        final Long millis = literal.getValueAs(Long.class);
        if (millis != null && millis < 0) {
            throw new ValidationException(
                    String.format("Argument '%s' of SNAPSHOT must not be negative.", argName));
        }
        return millis;
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface LogicalJoinToLateralSnapshotJoinRuleConfig extends RelRule.Config {

        LogicalJoinToLateralSnapshotJoinRule.LogicalJoinToLateralSnapshotJoinRuleConfig DEFAULT =
                ImmutableLogicalJoinToLateralSnapshotJoinRule
                        .LogicalJoinToLateralSnapshotJoinRuleConfig.builder()
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(FlinkLogicalJoin.class).anyInputs())
                        .withDescription("LogicalJoinToLateralSnapshotJoinRule");

        @Override
        default LogicalJoinToLateralSnapshotJoinRule toRule() {
            return new LogicalJoinToLateralSnapshotJoinRule(this);
        }
    }
}
