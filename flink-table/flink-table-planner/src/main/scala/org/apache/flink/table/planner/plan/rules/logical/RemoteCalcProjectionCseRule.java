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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Rule that deduplicates identical remote (e.g. Python) calls repeated in the projection of a {@link
 * FlinkLogicalCalc}.
 *
 * <p>The underlying {@link RexProgram} already shares structurally identical expressions through
 * {@link org.apache.calcite.rex.RexLocalRef}s. However, the remote calc translation expands those
 * local refs into independent expression trees, which re-introduces the duplication and makes the
 * same UDF be shipped to the remote worker once per occurrence. This rule makes the sharing explicit
 * in the plan by splitting the calc into two:
 *
 * <pre>
 * Calc(projection=[pyFunc(a, b), pyFunc(a, b)])
 * </pre>
 *
 * <p>becomes
 *
 * <pre>
 * TopCalc(projection=[$0, $0])
 *   BottomCalc(projection=[pyFunc(a, b) AS f0])
 * </pre>
 *
 * <p>The bottom calc keeps one occurrence of every distinct remote call, and the top calc is a pure
 * {@link RexInputRef} projection restoring the original output schema.
 *
 * <p>Only deterministic calls are deduplicated; a non-deterministic call must be evaluated
 * independently for each occurrence.
 */
@Value.Enclosing
public class RemoteCalcProjectionCseRule extends RelRule<RemoteCalcProjectionCseRule.Config> {

    protected RemoteCalcProjectionCseRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalCalc calc = call.rel(0);
        RexProgram program = calc.getProgram();

        // Conditions are pushed away by RemoteCalcPushConditionRule beforehand, and duplicates
        // shared with a condition are handled by RemoteCalcConditionProjectionCseRule.
        if (program.getCondition() != null) {
            return false;
        }

        List<RexNode> projects = RemoteCalcCseUtil.expandProjects(calc);
        RemoteCallFinder callFinder = config.remoteCallFinder();

        // Only a projection already normalized by RemoteCalcRewriteProjectionRule is handled, i.e.
        // it consists of plain input refs and top-level remote calls only.
        if (projects.stream().noneMatch(callFinder::isRemoteCall)) {
            return false;
        }
        if (!projects.stream()
                .allMatch(p -> p instanceof RexInputRef || callFinder.isRemoteCall(p))) {
            return false;
        }

        return findDuplicates(projects, callFinder) != null;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalCalc calc = call.rel(0);
        RelNode input = calc.getInput();
        RexBuilder rexBuilder = call.builder().getRexBuilder();
        RemoteCallFinder callFinder = config.remoteCallFinder();

        List<RexNode> projects = RemoteCalcCseUtil.expandProjects(calc);
        int[] originalToUnique = findDuplicates(projects, callFinder);
        if (originalToUnique == null) {
            return;
        }

        // The bottom calc keeps the first occurrence of every distinct expression.
        List<RexNode> bottomProjects = new ArrayList<>();
        Map<Integer, Integer> uniqueIndexToBottomIndex = new LinkedHashMap<>();
        for (int i = 0; i < projects.size(); i++) {
            if (originalToUnique[i] == i) {
                uniqueIndexToBottomIndex.put(i, bottomProjects.size());
                bottomProjects.add(projects.get(i));
            }
        }

        List<String> bottomFieldNames =
                SqlValidatorUtil.uniquify(
                        java.util.stream.IntStream.range(0, bottomProjects.size())
                                .mapToObj(i -> "f" + i)
                                .collect(Collectors.toList()),
                        rexBuilder.getTypeFactory().getTypeSystem().isSchemaCaseSensitive());

        FlinkLogicalCalc bottomCalc =
                new FlinkLogicalCalc(
                        calc.getCluster(),
                        calc.getTraitSet(),
                        input,
                        RexProgram.create(
                                input.getRowType(),
                                bottomProjects,
                                null,
                                bottomFieldNames,
                                rexBuilder));

        // The top calc only forwards the shared results back to their original positions.
        RelDataType bottomRowType = bottomCalc.getRowType();
        List<RexNode> topProjects = new ArrayList<>();
        for (int i = 0; i < projects.size(); i++) {
            int bottomIndex = uniqueIndexToBottomIndex.get(originalToUnique[i]);
            topProjects.add(
                    new RexInputRef(
                            bottomIndex, bottomRowType.getFieldList().get(bottomIndex).getType()));
        }

        call.transformTo(
                calc.copy(
                        calc.getTraitSet(),
                        bottomCalc,
                        RexProgram.create(
                                bottomRowType,
                                topProjects,
                                null,
                                calc.getRowType(),
                                rexBuilder)));
    }

    /**
     * Maps every projection index to the index of the first projection computing the same value.
     *
     * @return the mapping, or {@code null} when there is nothing to deduplicate
     */
    private int[] findDuplicates(List<RexNode> projects, RemoteCallFinder callFinder) {
        Map<RexNode, Integer> firstOccurrence = new LinkedHashMap<>();
        int[] originalToUnique = new int[projects.size()];
        boolean hasDuplicate = false;

        for (int i = 0; i < projects.size(); i++) {
            RexNode project = projects.get(i);
            // Forwarded fields are cheap and already shared, so only remote calls are considered.
            boolean canReuse = RemoteCalcCseUtil.isReusableRemoteCall(project, callFinder);
            Integer existing = canReuse ? firstOccurrence.get(project) : null;
            if (existing != null) {
                originalToUnique[i] = existing;
                hasDuplicate = true;
            } else {
                if (canReuse) {
                    firstOccurrence.put(project, i);
                }
                originalToUnique[i] = i;
            }
        }

        return hasDuplicate ? originalToUnique : null;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RemoteCalcProjectionCseRule)) {
            return false;
        }
        RemoteCalcProjectionCseRule other = (RemoteCalcProjectionCseRule) obj;
        return super.equals(other)
                && config.remoteCallFinder()
                        .getClass()
                        .equals(other.config.remoteCallFinder().getClass());
    }

    @Override
    public int hashCode() {
        return super.hashCode() * 31 + config.remoteCallFinder().getClass().hashCode();
    }

    // -------------------------------------------------------------------------

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        Config DEFAULT =
                ImmutableRemoteCalcProjectionCseRule.Config.builder()
                        .operandSupplier(b0 -> b0.operand(FlinkLogicalCalc.class).anyInputs())
                        .description("RemoteCalcProjectionCseRule")
                        .build();

        @Value.Default
        default RemoteCallFinder remoteCallFinder() {
            return new PythonRemoteCallFinder();
        }

        /** Sets {@link #remoteCallFinder()}. */
        Config withRemoteCallFinder(RemoteCallFinder callFinder);

        @Override
        default RemoteCalcProjectionCseRule toRule() {
            return new RemoteCalcProjectionCseRule(this);
        }
    }
}
