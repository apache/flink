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
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRel;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSnapshot;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType;
import org.apache.flink.table.planner.plan.utils.TemporalTableJoinUtil;

import org.apache.flink.shaded.curator5.org.apache.curator.shaded.com.google.common.collect.Lists;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.immutables.value.Value;

/**
 * Traverses an event time temporal table join {@link RelNode} tree and update the right child to
 * set {@link FlinkLogicalTableSourceScan}'s eventTimeSnapshot property to true which will prevent
 * it generating a new StreamPhysicalChangelogNormalize later.
 *
 * <p>the match patterns are as following(8 variants, the three `Calc` nodes are all optional):
 *
 * <pre>{@code
 *    Join (event time temporal)
 *      /       \
 * RelNode     [Calc]
 *               \
 *             Snapshot
 *                \
 *              [Calc]
 *                 \
 *             WatermarkAssigner
 *                  \
 *                [Calc]
 *                   \
 *                TableScan
 * }</pre>
 *
 * <p>Note: This rule can only be used in a separate {@link org.apache.calcite.plan.hep.HepProgram}
 * after `LOGICAL_REWRITE` rule sets are applied for now.
 */
@Value.Enclosing
public class EventTimeTemporalJoinRewriteRule
        extends RelRule<EventTimeTemporalJoinRewriteRule.Config> {

    public static final RuleSet EVENT_TIME_TEMPORAL_JOIN_REWRITE_RULES =
            RuleSets.ofList(
                    Config.JOIN_CALC_SNAPSHOT_CALC_WMA_CALC_TS.toRule(),
                    Config.JOIN_CALC_SNAPSHOT_CALC_WMA_TS.toRule(),
                    Config.JOIN_CALC_SNAPSHOT_WMA_CALC_TS.toRule(),
                    Config.JOIN_CALC_SNAPSHOT_WMA_TS.toRule(),
                    Config.JOIN_SNAPSHOT_CALC_WMA_CALC_TS.toRule(),
                    Config.JOIN_SNAPSHOT_CALC_WMA_TS.toRule(),
                    Config.JOIN_SNAPSHOT_WMA_CALC_TS.toRule(),
                    Config.JOIN_SNAPSHOT_WMA_TS.toRule());

    public EventTimeTemporalJoinRewriteRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalJoin join = call.rel(0);
        RexNode joinCondition = join.getCondition();
        // only matches event time temporal join
        return joinCondition != null
                && TemporalTableJoinUtil.isEventTimeTemporalJoin(joinCondition);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalJoin join = call.rel(0);
        FlinkLogicalRel joinRightChild = call.rel(2);
        RelNode newRight = transmitSnapshotRequirement(joinRightChild);
        call.transformTo(
                join.copy(join.getTraitSet(), Lists.newArrayList(join.getLeft(), newRight)));
    }

    private RelNode transmitSnapshotRequirement(RelNode node) {
        if (node instanceof FlinkLogicalCalc) {
            final FlinkLogicalCalc calc = (FlinkLogicalCalc) node;
            // filter is not allowed because it will corrupt the version table
            if (null != calc.getProgram().getCondition()) {
                throw new TableException(
                        "Filter is not allowed for right changelog input of event time temporal join,"
                                + " it will corrupt the versioning of data. Please consider removing the filter before joining.");
            }

            final RelNode child = calc.getInput();
            final RelNode newChild = transmitSnapshotRequirement(child);
            if (newChild != child) {
                return calc.copy(calc.getTraitSet(), newChild, calc.getProgram());
            }
            return calc;
        }
        if (node instanceof FlinkLogicalSnapshot) {
            final FlinkLogicalSnapshot snapshot = (FlinkLogicalSnapshot) node;
            assert isEventTime(snapshot.getPeriod().getType());
            final RelNode child = snapshot.getInput();
            final RelNode newChild = transmitSnapshotRequirement(child);
            if (newChild != child) {
                return snapshot.copy(snapshot.getTraitSet(), newChild, snapshot.getPeriod());
            }
            return snapshot;
        }
        if (node instanceof HepRelVertex) {
            return transmitSnapshotRequirement(((HepRelVertex) node).getCurrentRel());
        }
        if (node instanceof FlinkLogicalWatermarkAssigner) {
            final FlinkLogicalWatermarkAssigner wma = (FlinkLogicalWatermarkAssigner) node;
            final RelNode child = wma.getInput();
            final RelNode newChild = transmitSnapshotRequirement(child);
            if (newChild != child) {
                return wma.copy(
                        wma.getTraitSet(), newChild, wma.rowtimeFieldIndex(), wma.watermarkExpr());
            }
            return wma;
        }
        if (node instanceof FlinkLogicalTableSourceScan) {
            final FlinkLogicalTableSourceScan ts = (FlinkLogicalTableSourceScan) node;
            // update eventTimeSnapshotRequired to true
            return ts.copy(ts.getTraitSet(), ts.relOptTable(), true);
        }
        return node;
    }

    private boolean isEventTime(RelDataType period) {
        if (period instanceof TimeIndicatorRelDataType) {
            return ((TimeIndicatorRelDataType) period).isEventTime();
        }
        return false;
    }

    /**
     * Configuration for {@link EventTimeTemporalJoinRewriteRule}.
     *
     * <p>Operator tree:
     *
     * <pre>{@code
     *    Join (event time temporal)
     *      /       \
     * RelNode     [Calc]
     *               \
     *             Snapshot
     *                \
     *              [Calc]
     *                 \
     *             WatermarkAssigner
     *                  \
     *                [Calc]
     *                   \
     *                TableScan
     * }</pre>
     *
     * <p>8 variants:
     *
     * <ul>
     *   <li>JOIN_CALC_SNAPSHOT_CALC_WMA_CALC_TS
     *   <li>JOIN_CALC_SNAPSHOT_CALC_WMA_TS
     *   <li>JOIN_CALC_SNAPSHOT_WMA_CALC_TS
     *   <li>JOIN_CALC_SNAPSHOT_WMA_TS
     *   <li>JOIN_SNAPSHOT_CALC_WMA_CALC_TS
     *   <li>JOIN_SNAPSHOT_CALC_WMA_TS
     *   <li>JOIN_SNAPSHOT_WMA_CALC_TS
     *   <li>JOIN_SNAPSHOT_WMA_TS
     * </ul>
     */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        RelRule.Config JOIN_CALC_SNAPSHOT_CALC_WMA_CALC_TS =
                ImmutableEventTimeTemporalJoinRewriteRule.Config.builder()
                        .build()
                        .withDescription(
                                "EventTimeTemporalJoinRewriteRule_CALC_SNAPSHOT_CALC_WMA_CALC")
                        .as(Config.class)
                        .withOperandSupplier(
                                joinTransform ->
                                        joinTransform
                                                .operand(FlinkLogicalJoin.class)
                                                .inputs(
                                                        left ->
                                                                left.operand(FlinkLogicalRel.class)
                                                                        .anyInputs(),
                                                        right ->
                                                                right.operand(
                                                                                FlinkLogicalCalc
                                                                                        .class)
                                                                        .oneInput(
                                                                                r1 ->
                                                                                        r1.operand(
                                                                                                        FlinkLogicalSnapshot
                                                                                                                .class)
                                                                                                .oneInput(
                                                                                                        r2 ->
                                                                                                                r2.operand(
                                                                                                                                FlinkLogicalCalc
                                                                                                                                        .class)
                                                                                                                        .oneInput(
                                                                                                                                r3 ->
                                                                                                                                        r3.operand(
                                                                                                                                                        FlinkLogicalWatermarkAssigner
                                                                                                                                                                .class)
                                                                                                                                                .oneInput(
                                                                                                                                                        r4 ->
                                                                                                                                                                r4.operand(
                                                                                                                                                                                FlinkLogicalCalc
                                                                                                                                                                                        .class)
                                                                                                                                                                        .oneInput(
                                                                                                                                                                                r5 ->
                                                                                                                                                                                        r5.operand(
                                                                                                                                                                                                        FlinkLogicalTableSourceScan
                                                                                                                                                                                                                .class)
                                                                                                                                                                                                .noInputs())))))));
        RelRule.Config JOIN_CALC_SNAPSHOT_CALC_WMA_TS =
                ImmutableEventTimeTemporalJoinRewriteRule.Config.builder()
                        .build()
                        .withDescription("EventTimeTemporalJoinRewriteRule_CALC_SNAPSHOT_CALC_WMA")
                        .as(Config.class)
                        .withOperandSupplier(
                                joinTransform ->
                                        joinTransform
                                                .operand(FlinkLogicalJoin.class)
                                                .inputs(
                                                        left ->
                                                                left.operand(FlinkLogicalRel.class)
                                                                        .anyInputs(),
                                                        right ->
                                                                right.operand(
                                                                                FlinkLogicalCalc
                                                                                        .class)
                                                                        .oneInput(
                                                                                r1 ->
                                                                                        r1.operand(
                                                                                                        FlinkLogicalSnapshot
                                                                                                                .class)
                                                                                                .oneInput(
                                                                                                        r2 ->
                                                                                                                r2.operand(
                                                                                                                                FlinkLogicalCalc
                                                                                                                                        .class)
                                                                                                                        .oneInput(
                                                                                                                                r3 ->
                                                                                                                                        r3.operand(
                                                                                                                                                        FlinkLogicalWatermarkAssigner
                                                                                                                                                                .class)
                                                                                                                                                .oneInput(
                                                                                                                                                        r4 ->
                                                                                                                                                                r4.operand(
                                                                                                                                                                                FlinkLogicalTableSourceScan
                                                                                                                                                                                        .class)
                                                                                                                                                                        .noInputs()))))));

        RelRule.Config JOIN_CALC_SNAPSHOT_WMA_CALC_TS =
                ImmutableEventTimeTemporalJoinRewriteRule.Config.builder()
                        .build()
                        .withDescription("EventTimeTemporalJoinRewriteRule_CALC_SNAPSHOT_WMA_CALC")
                        .as(Config.class)
                        .withOperandSupplier(
                                joinTransform ->
                                        joinTransform
                                                .operand(FlinkLogicalJoin.class)
                                                .inputs(
                                                        left ->
                                                                left.operand(FlinkLogicalRel.class)
                                                                        .anyInputs(),
                                                        right ->
                                                                right.operand(
                                                                                FlinkLogicalCalc
                                                                                        .class)
                                                                        .oneInput(
                                                                                r1 ->
                                                                                        r1.operand(
                                                                                                        FlinkLogicalSnapshot
                                                                                                                .class)
                                                                                                .oneInput(
                                                                                                        r2 ->
                                                                                                                r2.operand(
                                                                                                                                FlinkLogicalWatermarkAssigner
                                                                                                                                        .class)
                                                                                                                        .oneInput(
                                                                                                                                r3 ->
                                                                                                                                        r3.operand(
                                                                                                                                                        FlinkLogicalCalc
                                                                                                                                                                .class)
                                                                                                                                                .oneInput(
                                                                                                                                                        r4 ->
                                                                                                                                                                r4.operand(
                                                                                                                                                                                FlinkLogicalTableSourceScan
                                                                                                                                                                                        .class)
                                                                                                                                                                        .noInputs()))))));
        RelRule.Config JOIN_CALC_SNAPSHOT_WMA_TS =
                ImmutableEventTimeTemporalJoinRewriteRule.Config.builder()
                        .build()
                        .withDescription("EventTimeTemporalJoinRewriteRule_CALC_SNAPSHOT_WMA")
                        .as(Config.class)
                        .withOperandSupplier(
                                joinTransform ->
                                        joinTransform
                                                .operand(FlinkLogicalJoin.class)
                                                .inputs(
                                                        left ->
                                                                left.operand(FlinkLogicalRel.class)
                                                                        .anyInputs(),
                                                        right ->
                                                                right.operand(
                                                                                FlinkLogicalCalc
                                                                                        .class)
                                                                        .oneInput(
                                                                                r1 ->
                                                                                        r1.operand(
                                                                                                        FlinkLogicalSnapshot
                                                                                                                .class)
                                                                                                .oneInput(
                                                                                                        r2 ->
                                                                                                                r2.operand(
                                                                                                                                FlinkLogicalWatermarkAssigner
                                                                                                                                        .class)
                                                                                                                        .oneInput(
                                                                                                                                r3 ->
                                                                                                                                        r3.operand(
                                                                                                                                                        FlinkLogicalTableSourceScan
                                                                                                                                                                .class)
                                                                                                                                                .noInputs())))));

        RelRule.Config JOIN_SNAPSHOT_CALC_WMA_CALC_TS =
                ImmutableEventTimeTemporalJoinRewriteRule.Config.builder()
                        .build()
                        .withDescription("EventTimeTemporalJoinRewriteRule_SNAPSHOT_CALC_WMA_CALC")
                        .as(Config.class)
                        .withOperandSupplier(
                                joinTransform ->
                                        joinTransform
                                                .operand(FlinkLogicalJoin.class)
                                                .inputs(
                                                        left ->
                                                                left.operand(FlinkLogicalRel.class)
                                                                        .anyInputs(),
                                                        right ->
                                                                right.operand(
                                                                                FlinkLogicalSnapshot
                                                                                        .class)
                                                                        .oneInput(
                                                                                r1 ->
                                                                                        r1.operand(
                                                                                                        FlinkLogicalCalc
                                                                                                                .class)
                                                                                                .oneInput(
                                                                                                        r2 ->
                                                                                                                r2.operand(
                                                                                                                                FlinkLogicalWatermarkAssigner
                                                                                                                                        .class)
                                                                                                                        .oneInput(
                                                                                                                                r3 ->
                                                                                                                                        r3.operand(
                                                                                                                                                        FlinkLogicalCalc
                                                                                                                                                                .class)
                                                                                                                                                .oneInput(
                                                                                                                                                        r4 ->
                                                                                                                                                                r4.operand(
                                                                                                                                                                                FlinkLogicalTableSourceScan
                                                                                                                                                                                        .class)
                                                                                                                                                                        .noInputs()))))));
        RelRule.Config JOIN_SNAPSHOT_CALC_WMA_TS =
                ImmutableEventTimeTemporalJoinRewriteRule.Config.builder()
                        .build()
                        .withDescription("EventTimeTemporalJoinRewriteRule_SNAPSHOT_CALC_WMA")
                        .as(Config.class)
                        .withOperandSupplier(
                                joinTransform ->
                                        joinTransform
                                                .operand(FlinkLogicalJoin.class)
                                                .inputs(
                                                        left ->
                                                                left.operand(FlinkLogicalRel.class)
                                                                        .anyInputs(),
                                                        right ->
                                                                right.operand(
                                                                                FlinkLogicalSnapshot
                                                                                        .class)
                                                                        .oneInput(
                                                                                r1 ->
                                                                                        r1.operand(
                                                                                                        FlinkLogicalCalc
                                                                                                                .class)
                                                                                                .oneInput(
                                                                                                        r2 ->
                                                                                                                r2.operand(
                                                                                                                                FlinkLogicalWatermarkAssigner
                                                                                                                                        .class)
                                                                                                                        .oneInput(
                                                                                                                                r3 ->
                                                                                                                                        r3.operand(
                                                                                                                                                        FlinkLogicalTableSourceScan
                                                                                                                                                                .class)
                                                                                                                                                .noInputs())))));

        RelRule.Config JOIN_SNAPSHOT_WMA_CALC_TS =
                ImmutableEventTimeTemporalJoinRewriteRule.Config.builder()
                        .build()
                        .withDescription("EventTimeTemporalJoinRewriteRule_SNAPSHOT_WMA_CALC")
                        .as(Config.class)
                        .withOperandSupplier(
                                joinTransform ->
                                        joinTransform
                                                .operand(FlinkLogicalJoin.class)
                                                .inputs(
                                                        left ->
                                                                left.operand(FlinkLogicalRel.class)
                                                                        .anyInputs(),
                                                        right ->
                                                                right.operand(
                                                                                FlinkLogicalSnapshot
                                                                                        .class)
                                                                        .oneInput(
                                                                                r1 ->
                                                                                        r1.operand(
                                                                                                        FlinkLogicalWatermarkAssigner
                                                                                                                .class)
                                                                                                .oneInput(
                                                                                                        r2 ->
                                                                                                                r2.operand(
                                                                                                                                FlinkLogicalCalc
                                                                                                                                        .class)
                                                                                                                        .oneInput(
                                                                                                                                r3 ->
                                                                                                                                        r3.operand(
                                                                                                                                                        FlinkLogicalTableSourceScan
                                                                                                                                                                .class)
                                                                                                                                                .noInputs())))));

        RelRule.Config JOIN_SNAPSHOT_WMA_TS =
                ImmutableEventTimeTemporalJoinRewriteRule.Config.builder()
                        .build()
                        .withDescription("EventTimeTemporalJoinRewriteRule_SNAPSHOT_WMA")
                        .as(Config.class)
                        .withOperandSupplier(
                                joinTransform ->
                                        joinTransform
                                                .operand(FlinkLogicalJoin.class)
                                                .inputs(
                                                        left ->
                                                                left.operand(FlinkLogicalRel.class)
                                                                        .anyInputs(),
                                                        right ->
                                                                right.operand(
                                                                                FlinkLogicalSnapshot
                                                                                        .class)
                                                                        .oneInput(
                                                                                r1 ->
                                                                                        r1.operand(
                                                                                                        FlinkLogicalWatermarkAssigner
                                                                                                                .class)
                                                                                                .oneInput(
                                                                                                        r2 ->
                                                                                                                r2.operand(
                                                                                                                                FlinkLogicalTableSourceScan
                                                                                                                                        .class)
                                                                                                                        .noInputs()))));

        @Override
        default RelOptRule toRule() {
            return new EventTimeTemporalJoinRewriteRule(this);
        }
    }
}
