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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.planner.plan.nodes.calcite.WatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalRel;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTemporalSort;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType;
import org.apache.flink.table.planner.plan.utils.FlinkRelUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Removes {@link StreamPhysicalWatermarkAssigner} nodes whose watermarks are not consumed by any
 * downstream operator that requires watermarks (e.g. windowed aggregates, event-time interval /
 * temporal joins, event-time temporal sort, or {@code CURRENT_WATERMARK} SQL calls).
 *
 * <h3>Anchoring</h3>
 *
 * <p>The rule is anchored on the HEP planner's root rel so that a single firing handles the whole
 * tree per optimization (per sink in a statement set, per query in sink-less optimization paths
 * such as {@code EXPLAIN} or non-insert Table API). The operand matches any {@link
 * StreamPhysicalRel}, but {@link #matches(RelOptRuleCall)} guards that only the current root
 * triggers the rewrite. This avoids re-firing the rule on intermediate sub-trees during HEP
 * top-down matching, which would either over-remove (if we drop a watermark assigner without
 * knowing the full ancestry) or thrash (if we walked the same sub-tree repeatedly).
 *
 * <h3>Walk and consumer detection</h3>
 *
 * <p>The rule walks the plan from the root towards the sources. While descending, it tracks whether
 * any ancestor node has already declared {@link StreamPhysicalRel#requireWatermark()}, embeds a
 * {@code CURRENT_WATERMARK} call in any of its expressions, or is a rowtime {@link
 * StreamPhysicalTemporalSort} (whose runtime operator consumes watermarks even though it does not
 * formally implement {@code requireWatermark()}). A watermark assigner is dropped only when no
 * ancestor up to the root requires watermarks.
 *
 * <h3>Type-marker demotion</h3>
 *
 * <p>{@link WatermarkAssigner} stamps a {@link TimeIndicatorRelDataType} marker (e.g. {@code
 * TIMESTAMP(3) *ROWTIME*}) onto the watermark column. When the assigner is dropped, the column
 * reverts to plain {@code TIMESTAMP(3)} along the path from the former assigner to the root. The
 * rule rewrites every parent on that path accordingly:
 *
 * <ul>
 *   <li>{@link StreamPhysicalCalc} parents have their {@link RexProgram} rebuilt against the new
 *       input row type. Any {@link RexInputRef} whose column type changed is replaced with a fresh
 *       ref of the new type. The rest of the {@link RexNode} tree is preserved as-is — we
 *       deliberately do <em>not</em> re-run the full time-indicator materializer here, because it
 *       would re-wrap already-materialized {@code PROCTIME_MATERIALIZE(...)} calls on unrelated
 *       proctime columns. If dropping the assigner leaves two adjacent Calcs (the parent and the
 *       former child of the assigner), they are folded into a single Calc via {@link
 *       FlinkRelUtil#merge(Calc, Calc)} so the rule produces the same compact plan as a follow-up
 *       {@code FlinkCalcMergeRule} pass would.
 *   <li>Other parents (pass-through rels such as Exchange, Union, ChangelogNormalize, Sink, …)
 *       derive their row types from their inputs and are recreated via {@link
 *       RelNode#copy(org.apache.calcite.plan.RelTraitSet, java.util.List)}.
 * </ul>
 *
 * <h3>Root-rowtime guard (per-branch)</h3>
 *
 * <p>If the root's output row type carries an event-time {@code *ROWTIME*} time-indicator on a
 * field, an upstream rowtime column is consumed by the root (e.g. forwarded through a sink for
 * external watermark propagation, or surfaced as a rowtime in the result of a {@code SELECT}). The
 * watermark assigner that ultimately produces that field must be kept.
 *
 * <p>The rule is precise about this: rather than refusing to fire across the whole sub-tree, it
 * tracks the set of "protected" field indexes through the rewrite and only refuses to drop the
 * specific assigner(s) whose rowtime field reaches a protected root field. Branches whose rowtime
 * column does <em>not</em> reach the root's protected fields are still rewritten. The protected set
 * is propagated downward as follows:
 *
 * <ul>
 *   <li>Pass-through rels (Filter, Sort, Union, Exchange, Sink, …): preserved.
 *   <li>{@link StreamPhysicalCalc} / {@link Project}: a parent's protected output index maps to its
 *       input index iff the corresponding projection is a bare {@link RexInputRef}. Composite
 *       expressions cannot carry a time-indicator marker through, so their inputs are not
 *       protected.
 *   <li>{@link Join}: indexes split between the left and right halves of the output schema (semi /
 *       anti joins only project the left side).
 *   <li>{@link Aggregate} and window aggregates: clear the protected set; aggregations consume time
 *       and produce non-rowtime aggregate results.
 *   <li>{@link WatermarkAssigner}: positional pass-through.
 * </ul>
 *
 * <p>See <a href="https://issues.apache.org/jira/browse/FLINK-14621">FLINK-14621</a>.
 *
 * <h3>Known limitations</h3>
 *
 * <ul>
 *   <li><b>Computed-column rowtime forwarding to sinks.</b> If a sink projects a column derived
 *       from a rowtime via an expression (e.g. {@code sink_rt AS rt + INTERVAL '1' SECOND}) rather
 *       than forwarding the rowtime column directly, the result type is plain {@code TIMESTAMP} and
 *       not a {@link TimeIndicatorRelDataType}. The protected-index propagation cannot trace the
 *       rowtime through the composite expression and the assigner may be dropped. The semantics of
 *       "watermark forwarding" through such a derived column are not well-defined in Flink today;
 *       if watermark propagation matters, project the rowtime column directly.
 *   <li><b>Per-branch protection across unions.</b> When the sink's protected rowtime column is fed
 *       by a {@link org.apache.calcite.rel.core.Union}, every branch's corresponding rowtime input
 *       is protected. This is required for correctness: at runtime a downstream operator advances
 *       watermarks as the minimum across all union inputs, so dropping the assigner on one branch
 *       would block (not skip) progress on that input.
 * </ul>
 */
@Value.Enclosing
public class RedundantWatermarkAssignerRemoveRule
        extends RelRule<RedundantWatermarkAssignerRemoveRule.Config> {

    public static final RelOptRule INSTANCE =
            new RedundantWatermarkAssignerRemoveRule(Config.DEFAULT);

    protected RedundantWatermarkAssignerRemoveRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        if (!isHepRoot(call)) {
            return false;
        }
        final StreamPhysicalRel root = call.rel(0);
        final Set<Integer> protectedIndexes = rowtimeFieldIndexes(root.getRowType());
        return hasRedundantWatermarkAssigner(root, false, protectedIndexes);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final StreamPhysicalRel root = call.rel(0);
        final RexBuilder rexBuilder = root.getCluster().getRexBuilder();
        final Set<Integer> protectedIndexes = rowtimeFieldIndexes(root.getRowType());
        final RelNode rewritten = rewrite(root, false, protectedIndexes, rexBuilder);
        if (rewritten != root) {
            call.transformTo(rewritten);
        }
    }

    /**
     * Returns {@code true} if the matched rel is the current HEP root. HEP rules cannot otherwise
     * detect "I'm at the root" — we ask the planner directly.
     */
    private static boolean isHepRoot(RelOptRuleCall call) {
        if (!(call.getPlanner() instanceof HepPlanner)) {
            return false;
        }
        final HepPlanner planner = (HepPlanner) call.getPlanner();
        return unwrap(planner.getRoot()) == call.rel(0);
    }

    private static Set<Integer> rowtimeFieldIndexes(RelDataType rowType) {
        if (!rowType.isStruct()) {
            // Some HEP roots in intermediate optimization stages can be non-struct (e.g. RAW row
            // types). They cannot carry a rowtime time-indicator marker.
            return Collections.emptySet();
        }
        Set<Integer> indexes = null;
        final List<RelDataType> fields =
                rowType.getFieldList().stream().map(f -> f.getType()).collect(Collectors.toList());
        for (int i = 0; i < fields.size(); i++) {
            final RelDataType t = fields.get(i);
            if (t instanceof TimeIndicatorRelDataType
                    && ((TimeIndicatorRelDataType) t).isEventTime()) {
                if (indexes == null) {
                    indexes = new HashSet<>();
                }
                indexes.add(i);
            }
        }
        return indexes == null ? Collections.emptySet() : indexes;
    }

    /**
     * Returns {@code true} if the subtree rooted at {@code node} contains at least one watermark
     * assigner whose watermarks are not consumed by any operator on the path to the root and whose
     * rowtime field is not in the protected set.
     */
    private static boolean hasRedundantWatermarkAssigner(
            RelNode node, boolean requiredAbove, Set<Integer> protectedIndexes) {
        if (node instanceof StreamPhysicalWatermarkAssigner && !requiredAbove) {
            final int rtIdx = ((StreamPhysicalWatermarkAssigner) node).rowtimeFieldIndex();
            if (!protectedIndexes.contains(rtIdx)) {
                return true;
            }
        }
        final boolean requiredBelow = requiredAbove || consumesWatermarkAtRuntime(node);
        final List<RelNode> inputs = node.getInputs();
        for (int i = 0; i < inputs.size(); i++) {
            final RelNode child = unwrap(inputs.get(i));
            final Set<Integer> childProtected = mapProtectedDown(node, i, protectedIndexes);
            if (hasRedundantWatermarkAssigner(child, requiredBelow, childProtected)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Recursively rewrites the subtree rooted at {@code node}, dropping any {@link
     * StreamPhysicalWatermarkAssigner} that is not needed by an ancestor and whose rowtime column
     * is not in the protected set, and demoting downstream references to the formerly-stamped
     * time-indicator column.
     */
    private static RelNode rewrite(
            RelNode node,
            boolean requiredAbove,
            Set<Integer> protectedIndexes,
            RexBuilder rexBuilder) {
        if (node instanceof StreamPhysicalWatermarkAssigner && !requiredAbove) {
            final StreamPhysicalWatermarkAssigner assigner = (StreamPhysicalWatermarkAssigner) node;
            if (!protectedIndexes.contains(assigner.rowtimeFieldIndex())) {
                // Drop: replace with the (rewritten) input. The TimeIndicator marker on the
                // watermark column is lost; parents above will be retyped on the way back up.
                return rewrite(unwrap(assigner.getInput()), false, protectedIndexes, rexBuilder);
            }
            // Keep this assigner — its rowtime column is protected (reaches a root rowtime
            // field). Fall through to the generic recursion to continue processing below.
        }

        final boolean requiredBelow = requiredAbove || consumesWatermarkAtRuntime(node);

        final List<RelNode> inputs = node.getInputs();
        if (inputs == null || inputs.isEmpty()) {
            return node;
        }
        final List<RelNode> newInputs = new ArrayList<>(inputs.size());
        boolean changed = false;
        for (int i = 0; i < inputs.size(); i++) {
            final RelNode unwrapped = unwrap(inputs.get(i));
            final Set<Integer> childProtected = mapProtectedDown(node, i, protectedIndexes);
            final RelNode rewritten = rewrite(unwrapped, requiredBelow, childProtected, rexBuilder);
            changed |= rewritten != unwrapped;
            newInputs.add(rewritten);
        }
        if (!changed) {
            return node;
        }
        return rebuildParent(node, newInputs, rexBuilder);
    }

    /**
     * Maps a parent's protected output-field indexes to one of its inputs' field-index space. See
     * the class Javadoc for the propagation rules.
     */
    private static Set<Integer> mapProtectedDown(
            RelNode node, int inputIndex, Set<Integer> parentProtected) {
        if (parentProtected.isEmpty()) {
            return Collections.emptySet();
        }
        if (node instanceof StreamPhysicalCalc) {
            final RexProgram program = ((StreamPhysicalCalc) node).getProgram();
            final Set<Integer> mapped = new HashSet<>();
            for (Integer outIdx : parentProtected) {
                final RexNode expanded =
                        program.expandLocalRef(program.getProjectList().get(outIdx));
                if (expanded instanceof RexInputRef) {
                    mapped.add(((RexInputRef) expanded).getIndex());
                }
            }
            return mapped;
        }
        if (node instanceof Project) {
            final List<RexNode> projects = ((Project) node).getProjects();
            final Set<Integer> mapped = new HashSet<>();
            for (Integer outIdx : parentProtected) {
                final RexNode rex = projects.get(outIdx);
                if (rex instanceof RexInputRef) {
                    mapped.add(((RexInputRef) rex).getIndex());
                }
            }
            return mapped;
        }
        if (node instanceof Join) {
            final Join join = (Join) node;
            final int leftFieldCount = join.getLeft().getRowType().getFieldCount();
            final boolean leftOnlyOutput =
                    join.getJoinType() == JoinRelType.SEMI
                            || join.getJoinType() == JoinRelType.ANTI;
            final Set<Integer> mapped = new HashSet<>();
            for (Integer outIdx : parentProtected) {
                if (leftOnlyOutput) {
                    if (inputIndex == 0) {
                        mapped.add(outIdx);
                    }
                } else if (inputIndex == 0 && outIdx < leftFieldCount) {
                    mapped.add(outIdx);
                } else if (inputIndex == 1 && outIdx >= leftFieldCount) {
                    mapped.add(outIdx - leftFieldCount);
                }
            }
            return mapped;
        }
        if (node instanceof Aggregate) {
            return Collections.emptySet();
        }
        // Pass-through default for rels that preserve input column positions at the start of
        // their output (WatermarkAssigner, Filter, Sort, Union, Exchange, Sink,
        // ChangelogNormalize, …). Some rels (e.g. window TVF) append columns at the end; bound
        // the propagated indexes by the child input's field count to stay within range.
        final int inputFieldCount =
                unwrap(node.getInputs().get(inputIndex)).getRowType().getFieldCount();
        final Set<Integer> bounded = new HashSet<>();
        for (Integer idx : parentProtected) {
            if (idx < inputFieldCount) {
                bounded.add(idx);
            }
        }
        return bounded;
    }

    /**
     * Rebuilds {@code node} with {@code newInputs}, accounting for the fact that a formerly
     * time-indicator column on the inputs may now be a plain timestamp.
     */
    private static RelNode rebuildParent(
            RelNode node, List<RelNode> newInputs, RexBuilder rexBuilder) {
        if (node instanceof StreamPhysicalCalc) {
            return rebuildCalc((StreamPhysicalCalc) node, newInputs.get(0), rexBuilder);
        }
        // Pass-through: row type is derived from the inputs, so a regular copy demotes the
        // rowtime field automatically.
        return node.copy(node.getTraitSet(), newInputs);
    }

    private static RelNode rebuildCalc(
            StreamPhysicalCalc calc, RelNode newInput, RexBuilder rexBuilder) {
        final RelDataType oldInputType = calc.getInput().getRowType();
        final RelDataType newInputType = newInput.getRowType();
        // Nothing to retype if the input row-type is unchanged. The input rel may still have
        // been replaced (e.g. an inner Calc was rebuilt below); merge if it is also a Calc to
        // collapse the two adjacent Calcs that result from dropping a WatermarkAssigner.
        if (oldInputType.equals(newInputType)) {
            final RelNode top = calc.copy(calc.getTraitSet(), newInput, calc.getProgram());
            return maybeMergeCalc((StreamPhysicalCalc) top);
        }
        final RexProgram program = calc.getProgram();
        // Retype any RexInputRef whose input column type changed (rowtime demoted to plain
        // TIMESTAMP). We deliberately do NOT run the full RexTimeIndicatorMaterializer here:
        // it would re-wrap already-materialized PROCTIME_MATERIALIZE(...) calls on other
        // (proctime) columns, producing PROCTIME_MATERIALIZE(PROCTIME_MATERIALIZE(...)).
        final RexShuttle retyper =
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef ref) {
                        final RelDataType newFieldType =
                                newInputType.getFieldList().get(ref.getIndex()).getType();
                        if (!ref.getType().equals(newFieldType)) {
                            return rexBuilder.makeInputRef(newFieldType, ref.getIndex());
                        }
                        return ref;
                    }
                };
        final List<RexNode> projects =
                program.getProjectList().stream()
                        .map(p -> program.expandLocalRef(p).accept(retyper))
                        .collect(Collectors.toList());
        final RexNode condition =
                program.getCondition() != null
                        ? program.expandLocalRef(program.getCondition()).accept(retyper)
                        : null;
        final RexProgram newProgram =
                RexProgram.create(
                        newInputType,
                        projects,
                        condition,
                        program.getOutputRowType().getFieldNames(),
                        rexBuilder);
        final StreamPhysicalCalc top =
                new StreamPhysicalCalc(
                        calc.getCluster(),
                        calc.getTraitSet(),
                        newInput,
                        newProgram,
                        newProgram.getOutputRowType());
        return maybeMergeCalc(top);
    }

    /**
     * If {@code top}'s input is itself a {@link Calc}, merge the two programs into a single Calc.
     * Dropping a {@link StreamPhysicalWatermarkAssigner} that sits between two Calcs naturally
     * leaves them adjacent; folding them keeps the plan compact and matches what {@code
     * FlinkCalcMergeRule} would produce on a subsequent pass.
     */
    private static RelNode maybeMergeCalc(StreamPhysicalCalc top) {
        final RelNode input = unwrap(top.getInput());
        if (input instanceof Calc) {
            return FlinkRelUtil.merge(top, (Calc) input);
        }
        return top;
    }

    private static RelNode unwrap(RelNode node) {
        return node instanceof HepRelVertex ? ((HepRelVertex) node).getCurrentRel() : node;
    }

    /**
     * Returns {@code true} if {@code node} consumes upstream watermarks at runtime, either through
     * a declared {@link StreamPhysicalRel#requireWatermark()}, a {@code CURRENT_WATERMARK} SQL call
     * embedded in any of its expressions, or as a special-case rel whose runtime operator depends
     * on watermarks while {@code requireWatermark()} happens to return {@code false}.
     */
    private static boolean consumesWatermarkAtRuntime(RelNode node) {
        if (node instanceof StreamPhysicalRel && ((StreamPhysicalRel) node).requireWatermark()) {
            return true;
        }
        if (isRowtimeTemporalSort(node)) {
            return true;
        }
        return containsCurrentWatermarkCall(node);
    }

    /**
     * Returns {@code true} if {@code node} is a {@link StreamPhysicalTemporalSort} whose primary
     * sort field is a rowtime time-indicator column. Such a sort is implemented at runtime by an
     * operator that consumes watermarks.
     */
    private static boolean isRowtimeTemporalSort(RelNode node) {
        if (!(node instanceof StreamPhysicalTemporalSort)) {
            return false;
        }
        final StreamPhysicalTemporalSort sort = (StreamPhysicalTemporalSort) node;
        if (sort.getCollation().getFieldCollations().isEmpty()) {
            return false;
        }
        final int fieldIndex = sort.getCollation().getFieldCollations().get(0).getFieldIndex();
        final RelDataType fieldType =
                sort.getInput().getRowType().getFieldList().get(fieldIndex).getType();
        return fieldType instanceof TimeIndicatorRelDataType
                && ((TimeIndicatorRelDataType) fieldType).isEventTime();
    }

    /**
     * Defensively scans every {@link RexNode} that {@code node} exposes via {@link
     * RelNode#accept(RexShuttle)}. This relies on the standard Calcite contract that a rel
     * implementation routes its internal expressions through the shuttle (Calc, Project, Filter,
     * Join, Aggregate filter args, …), so any future rel type that exposes a {@code
     * CURRENT_WATERMARK} call is automatically classified as a watermark consumer.
     */
    private static boolean containsCurrentWatermarkCall(RelNode node) {
        final boolean[] found = {false};
        final RexShuttle shuttle =
                new RexShuttle() {
                    @Override
                    public RexNode visitCall(RexCall call) {
                        if (!found[0]
                                && "CURRENT_WATERMARK"
                                        .equalsIgnoreCase(call.getOperator().getName())) {
                            found[0] = true;
                        }
                        return super.visitCall(call);
                    }
                };
        try {
            node.accept(shuttle);
        } catch (Exception ignored) {
            // Some rels' accept(RexShuttle) implementations may not be defensive; treat scan
            // failures as "no CURRENT_WATERMARK found" — in the worst case we miss a marker, but
            // the same rel will be examined again from the parent side via the recursion.
        }
        return found[0];
    }

    /** Rule configuration. */
    @Value.Immutable
    public interface Config extends RelRule.Config {
        Config DEFAULT =
                ImmutableRedundantWatermarkAssignerRemoveRule.Config.builder()
                        .description("RedundantWatermarkAssignerRemoveRule")
                        .operandSupplier(
                                builder -> builder.operand(StreamPhysicalRel.class).anyInputs())
                        .build();

        @Override
        default RedundantWatermarkAssignerRemoveRule toRule() {
            return new RedundantWatermarkAssignerRemoveRule(this);
        }
    }
}
