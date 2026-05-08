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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Pushes a {@link LogicalProject} down through a {@link LogicalCorrelate} produced by Flink's
 * UNNEST rewrite, pruning unused left-side columns before the cross-product expansion.
 *
 * <p>This is a custom replacement for Calcite's {@link
 * org.apache.calcite.rel.rules.ProjectCorrelateTransposeRule}, which has two bugs that cause
 * runtime failures on UNNEST:
 *
 * <ol>
 *   <li><b>Bug 1.</b> {@code RelShuttleImpl.visit(TableFunctionScan)} only walks {@code
 *       visitChildren(scan)} — it never applies the {@link RexShuttle} to the scan's {@code
 *       rexCall}, and {@code LogicalTableFunctionScan} doesn't override {@code accept(RelShuttle)},
 *       so dispatch routes through {@code visit(RelNode)} instead. The result: {@code
 *       RexFieldAccess($cor0.X)} indices inside the unnest call are never re-numbered when the left
 *       input is pruned, and runtime codegen reads from a stale field index. We fix this here by
 *       walking the right tree explicitly and applying our shuttle to every {@code
 *       TableFunctionScan} we find.
 *   <li><b>Bug 2.</b> Calcite's {@code PushProjector} mishandles renumbering when the right side is
 *       {@code LogicalProject(named_fields...) over LogicalTableFunctionScan} and the wrapper
 *       Project has more than one output field — pruning produces a plan whose remaining wrapper
 *       output silently resolves to the wrong source field (e.g. {@code UNNEST(MAP)} returns keys
 *       where values were expected). We sidestep this entirely by NOT pruning the right side at
 *       all. Only the left input is pruned; the right side (TFS plus any wrapper Project) is passed
 *       through unchanged except for the {@code $cor0.X} index rewrite from Bug 1.
 * </ol>
 *
 * <p>The trade-off vs. a fully-general project pushdown: when a query reads only a subset of an
 * UNNEST's output columns (e.g. only {@code v} from {@code UNNEST(MAP) AS f(k, v)}), we still pass
 * both columns out of the Correlate. The unused column gets trimmed by downstream Calc-merging.
 * Pruning the (typically wide) source-table left input is the bigger win and is what this rule
 * delivers safely.
 */
@Value.Enclosing
public class FlinkProjectCorrelateUnnestTransposeRule
        extends RelRule<FlinkProjectCorrelateUnnestTransposeRule.Config>
        implements TransformationRule {

    public static final FlinkProjectCorrelateUnnestTransposeRule INSTANCE =
            FlinkProjectCorrelateUnnestTransposeRule.Config.DEFAULT.toRule();

    protected FlinkProjectCorrelateUnnestTransposeRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Correlate correlate = call.rel(1);
        return UnnestRuleUtil.isUnnestCorrelate(correlate);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalProject topProject = call.rel(0);
        final LogicalCorrelate correlate = call.rel(1);
        final RelNode left = correlate.getLeft();
        final int leftFieldCount = left.getRowType().getFieldCount();

        // Determine which left columns the top project references, plus the columns required by
        // the right side via the correlation variable.
        ImmutableBitSet projectInputs = RelOptUtil.InputFinder.bits(topProject.getProjects(), null);
        ImmutableBitSet projectLeftRefs =
                projectInputs.intersect(ImmutableBitSet.range(0, leftFieldCount));
        ImmutableBitSet usedLeftCols = projectLeftRefs.union(correlate.getRequiredColumns());

        // Nothing to prune: rule is a no-op. Also defensively bail if no left columns survive,
        // which would produce a zero-column Project that some Calcite paths reject.
        if (usedLeftCols.cardinality() == leftFieldCount || usedLeftCols.cardinality() == 0) {
            return;
        }

        // Build mapping oldLeftIndex -> newLeftIndex for kept columns.
        final Map<Integer, Integer> leftMapping = new HashMap<>();
        int newIdx = 0;
        for (Integer oldIdx : usedLeftCols) {
            leftMapping.put(oldIdx, newIdx++);
        }
        final int newLeftFieldCount = newIdx;

        final RelBuilder builder = call.builder();
        final RexBuilder rexBuilder = builder.getRexBuilder();

        // Build new left input as a Project that keeps only the used columns. RelBuilder.project
        // derives field names from the input refs.
        builder.push(left);
        final List<RexNode> leftRefs = new ArrayList<>();
        for (Integer oldIdx : usedLeftCols) {
            leftRefs.add(builder.field(oldIdx));
        }
        final RelNode newLeft = builder.project(leftRefs).build();

        // Allocate a fresh correlation id bound to the new left's narrower row type, and rewrite
        // every $cor0.X reference inside the right side using the index mapping. This is the
        // explicit Bug 1 workaround.
        final CorrelationId newCorId = correlate.getCluster().createCorrel();
        final RexCorrelVariable newCorVar =
                (RexCorrelVariable) rexBuilder.makeCorrel(newLeft.getRowType(), newCorId);
        final CorrelationFieldAccessRebinder rebinder =
                new CorrelationFieldAccessRebinder(
                        correlate.getCorrelationId(), newCorVar, leftMapping, rexBuilder);
        final RelNode newRight = rewriteCorrelationRefs(correlate.getRight(), rebinder);

        // Re-map requiredColumns to the new left's index space.
        final ImmutableBitSet.Builder newReqColsBuilder = ImmutableBitSet.builder();
        for (Integer oldCol : correlate.getRequiredColumns()) {
            newReqColsBuilder.set(leftMapping.get(oldCol));
        }
        final ImmutableBitSet newReqCols = newReqColsBuilder.build();

        // Build the new Correlate.
        final RelNode newCorrelate =
                correlate.copy(
                        correlate.getTraitSet(),
                        newLeft,
                        newRight,
                        newCorId,
                        newReqCols,
                        correlate.getJoinType());

        // Rewrite the top Project's input refs:
        //   left side  ([0, leftFieldCount))            -> via leftMapping
        //   right side ([leftFieldCount, total))        -> shifted by (newLeftFieldCount -
        // leftFieldCount)
        // Right-side row width is unchanged, so a uniform shift is sufficient.
        final InputRefRemapper remapper =
                new InputRefRemapper(leftMapping, leftFieldCount, newLeftFieldCount);
        final List<RexNode> newProjects =
                topProject.getProjects().stream()
                        .map(rex -> rex.accept(remapper))
                        .collect(Collectors.toList());

        final RelNode result =
                builder.push(newCorrelate)
                        .project(newProjects, topProject.getRowType().getFieldNames())
                        .build();

        call.transformTo(result);
    }

    /**
     * Looks up {@code oldIdx} in {@code mapping} and returns the new index, throwing a clear
     * exception if the index is missing. Centralizes the consistency check used by both the
     * correlation-variable rewrite and the top-project input-ref remap.
     */
    private static int requireMapped(
            Map<Integer, Integer> mapping, int oldIdx, String description) {
        Integer newIdx = mapping.get(oldIdx);
        if (newIdx == null) {
            throw new IllegalStateException(
                    description + " " + oldIdx + " missing from mapping " + mapping);
        }
        return newIdx;
    }

    /**
     * Walks the right subtree applying {@code rexShuttle} to every node's expressions. We do this
     * manually rather than going through {@code RelShuttleImpl} because the stock implementation
     * skips leaf {@link TableFunctionScan} nodes' RexCalls (Bug 1 in this rule's javadoc).
     *
     * <p>The traversal:
     *
     * <ul>
     *   <li>Recursively visits each input.
     *   <li>Re-builds the parent if any input changed.
     *   <li>Calls {@code accept(rexShuttle)} on every node so that {@code TableFunctionScan}'s
     *       {@code rexCall}, {@code LogicalProject}'s projections, etc. are all rewritten.
     * </ul>
     */
    private static RelNode rewriteCorrelationRefs(RelNode rel, RexShuttle rexShuttle) {
        RelNode unwrapped = UnnestRuleUtil.unwrap(rel);
        final List<RelNode> originalInputs = unwrapped.getInputs();
        final List<RelNode> newInputs = new ArrayList<>(originalInputs.size());
        boolean changed = false;
        for (RelNode input : originalInputs) {
            RelNode newInput = rewriteCorrelationRefs(input, rexShuttle);
            newInputs.add(newInput);
            if (newInput != input) {
                changed = true;
            }
        }
        RelNode current = changed ? unwrapped.copy(unwrapped.getTraitSet(), newInputs) : unwrapped;
        return current.accept(rexShuttle);
    }

    /**
     * {@link RexShuttle} that swaps an old {@link RexCorrelVariable} for a new one and re-numbers
     * the field index of any {@link RexFieldAccess} on it using the supplied mapping.
     *
     * <p>Mirrors Calcite's {@code ProjectCorrelateTransposeRule.RexFieldAccessReplacer}, but lives
     * here so the rule is self-contained.
     */
    private static class CorrelationFieldAccessRebinder extends RexShuttle {
        private final CorrelationId oldCorId;
        private final CorrelationId newCorId;
        private final RexCorrelVariable newCorVar;
        private final Map<Integer, Integer> indexMapping;
        private final RexBuilder rexBuilder;

        CorrelationFieldAccessRebinder(
                CorrelationId oldCorId,
                RexCorrelVariable newCorVar,
                Map<Integer, Integer> indexMapping,
                RexBuilder rexBuilder) {
            this.oldCorId = oldCorId;
            this.newCorId = newCorVar.id;
            this.newCorVar = newCorVar;
            this.indexMapping = indexMapping;
            this.rexBuilder = rexBuilder;
        }

        @Override
        public RexNode visitCorrelVariable(RexCorrelVariable variable) {
            return variable.id.equals(oldCorId) ? newCorVar : variable;
        }

        @Override
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            RexNode refExpr = fieldAccess.getReferenceExpr().accept(this);
            // Semantic id check (rather than reference equality on `newCorVar`) so the rebinder
            // remains correct if Calcite ever interns/normalizes RexCorrelVariable instances.
            if (refExpr instanceof RexCorrelVariable
                    && ((RexCorrelVariable) refExpr).id.equals(newCorId)) {
                int newFieldIdx =
                        requireMapped(
                                indexMapping,
                                fieldAccess.getField().getIndex(),
                                "Required column index");
                return rexBuilder.makeFieldAccess(refExpr, newFieldIdx);
            }
            return super.visitFieldAccess(fieldAccess);
        }
    }

    /**
     * {@link RexShuttle} that re-numbers {@link RexInputRef}s in the top project after the left
     * input has been pruned. Left-side refs use the index mapping; right-side refs are shifted by
     * the change in left-field count (right-side row width is unchanged).
     */
    private static class InputRefRemapper extends RexShuttle {
        private final Map<Integer, Integer> leftMapping;
        private final int oldLeftFieldCount;
        private final int newLeftFieldCount;

        InputRefRemapper(
                Map<Integer, Integer> leftMapping, int oldLeftFieldCount, int newLeftFieldCount) {
            this.leftMapping = leftMapping;
            this.oldLeftFieldCount = oldLeftFieldCount;
            this.newLeftFieldCount = newLeftFieldCount;
        }

        @Override
        public RexNode visitInputRef(RexInputRef ref) {
            int oldIdx = ref.getIndex();
            if (oldIdx < oldLeftFieldCount) {
                int newIdx =
                        requireMapped(
                                leftMapping, oldIdx, "Top project references pruned left column");
                return new RexInputRef(newIdx, ref.getType());
            }
            int rightOffset = oldIdx - oldLeftFieldCount;
            return new RexInputRef(newLeftFieldCount + rightOffset, ref.getType());
        }
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        Config DEFAULT =
                ImmutableFlinkProjectCorrelateUnnestTransposeRule.Config.builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(LogicalProject.class)
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(LogicalCorrelate.class)
                                                                        .anyInputs()))
                        .withDescription("FlinkProjectCorrelateUnnestTransposeRule");

        @Override
        default FlinkProjectCorrelateUnnestTransposeRule toRule() {
            return new FlinkProjectCorrelateUnnestTransposeRule(this);
        }
    }
}
