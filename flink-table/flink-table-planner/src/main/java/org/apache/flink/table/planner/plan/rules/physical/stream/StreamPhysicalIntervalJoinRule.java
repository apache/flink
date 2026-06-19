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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode;
import org.apache.flink.table.planner.plan.nodes.exec.spec.IntervalJoinSpec;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalIntervalJoin;
import org.apache.flink.table.planner.plan.utils.IntervalJoinUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;

import java.util.Collection;
import java.util.function.Function;
import java.util.stream.Collectors;

import scala.Option;

/**
 * Rule that converts non-SEMI/ANTI {@link FlinkLogicalJoin} with window bounds in join condition to
 * {@link StreamPhysicalIntervalJoin}.
 */
@Value.Enclosing
public class StreamPhysicalIntervalJoinRule
        extends StreamPhysicalJoinRuleBase<
                StreamPhysicalIntervalJoinRule.StreamPhysicalIntervalJoinRuleConfig> {
    public static final RelOptRule INSTANCE = StreamPhysicalIntervalJoinRuleConfig.DEFAULT.toRule();

    public StreamPhysicalIntervalJoinRule(StreamPhysicalIntervalJoinRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalJoin join = call.rel(0);

        if (!IntervalJoinUtil.satisfyIntervalJoin(join)) {
            return false;
        }

        // validate the join
        IntervalJoinSpec.WindowBounds windowBounds = extractWindowBounds(join).f0.get();

        if (windowBounds.isEventTime()) {
            RelDataType leftTimeAttributeType =
                    join.getLeft()
                            .getRowType()
                            .getFieldList()
                            .get(windowBounds.getLeftTimeIdx())
                            .getType();
            RelDataType rightTimeAttributeType =
                    join.getRight()
                            .getRowType()
                            .getFieldList()
                            .get(windowBounds.getRightTimeIdx())
                            .getType();
            if (leftTimeAttributeType.getSqlTypeName() != rightTimeAttributeType.getSqlTypeName()) {
                throw new ValidationException(
                        String.format(
                                "Interval join with rowtime attribute requires same rowtime types,"
                                        + " but the types are %s and %s.",
                                leftTimeAttributeType, rightTimeAttributeType));
            }
        } else {
            // Check that no event-time attributes are in the input because the processing time
            // window
            // join does not correctly hold back watermarks.
            // We rely on projection pushdown to remove unused attributes before the join.
            RelDataType joinRowType = join.getRowType();
            boolean containsRowTime =
                    joinRowType.getFieldList().stream()
                            .anyMatch(f -> FlinkTypeFactory.isRowtimeIndicatorType(f.getType()));
            if (containsRowTime) {
                throw new TableException(
                        "Interval join with proctime attribute requires no event-time attributes are in the "
                                + "join inputs.");
            }
        }
        return true;
    }

    @Override
    public Collection<Integer> computeJoinLeftKeys(FlinkLogicalJoin join) {
        Tuple2<Option<IntervalJoinSpec.WindowBounds>, Option<RexNode>> tuple2 =
                extractWindowBounds(join);
        return join.analyzeCondition().leftKeys.stream()
                .filter(k -> tuple2.f0.get().getLeftTimeIdx() != k)
                .collect(Collectors.toList());
    }

    @Override
    public Collection<Integer> computeJoinRightKeys(FlinkLogicalJoin join) {
        Tuple2<Option<IntervalJoinSpec.WindowBounds>, Option<RexNode>> tuple2 =
                extractWindowBounds(join);
        return join.analyzeCondition().rightKeys.stream()
                .filter(k -> tuple2.f0.get().getRightTimeIdx() != k)
                .collect(Collectors.toList());
    }

    @Override
    public FlinkRelNode transform(
            FlinkLogicalJoin join,
            FlinkRelNode leftInput,
            Function<RelNode, RelNode> leftConversion,
            FlinkRelNode rightInput,
            Function<RelNode, RelNode> rightConversion,
            RelTraitSet providedTraitSet) {
        Tuple2<Option<IntervalJoinSpec.WindowBounds>, Option<RexNode>> tuple2 =
                extractWindowBounds(join);
        return new StreamPhysicalIntervalJoin(
                join.getCluster(),
                providedTraitSet,
                leftConversion.apply(leftInput),
                rightConversion.apply(rightInput),
                join.getJoinType(),
                join.getCondition(),
                tuple2.f1.getOrElse(() -> join.getCluster().getRexBuilder().makeLiteral(true)),
                tuple2.f0.get());
    }

    /** Configuration for {@link StreamPhysicalIntervalJoinRule}. */
    @Value.Immutable
    public interface StreamPhysicalIntervalJoinRuleConfig
            extends StreamPhysicalJoinRuleBaseRuleConfig {
        StreamPhysicalIntervalJoinRule.StreamPhysicalIntervalJoinRuleConfig DEFAULT =
                ImmutableStreamPhysicalIntervalJoinRule.StreamPhysicalIntervalJoinRuleConfig
                        .builder()
                        .build()
                        .withOperandSupplier(StreamPhysicalJoinRuleBaseRuleConfig.OPERAND_TRANSFORM)
                        .withDescription("StreamPhysicalJoinRuleBase")
                        .as(
                                StreamPhysicalIntervalJoinRule.StreamPhysicalIntervalJoinRuleConfig
                                        .class);

        @Override
        default StreamPhysicalIntervalJoinRule toRule() {
            return new StreamPhysicalIntervalJoinRule(this);
        }
    }
}
