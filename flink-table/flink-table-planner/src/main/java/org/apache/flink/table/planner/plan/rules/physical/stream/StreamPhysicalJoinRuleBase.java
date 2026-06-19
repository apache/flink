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
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode;
import org.apache.flink.table.planner.plan.nodes.exec.spec.IntervalJoinSpec;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRel;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.plan.utils.IntervalJoinUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;

import java.util.Collection;
import java.util.function.Function;

import scala.Option;

/**
 * Base implementation for rules match stream-stream join, including regular stream join, interval
 * join and temporal join.
 */
@Value.Enclosing
public abstract class StreamPhysicalJoinRuleBase<
                T extends StreamPhysicalJoinRuleBase.StreamPhysicalJoinRuleBaseRuleConfig>
        extends RelRule<StreamPhysicalJoinRuleBase.StreamPhysicalJoinRuleBaseRuleConfig> {

    protected StreamPhysicalJoinRuleBase(T config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalJoin join = call.rel(0);
        FlinkLogicalRel left = call.rel(1);
        FlinkLogicalRel right = call.rel(2);
        FlinkRelNode newJoin =
                transform(
                        join,
                        left,
                        leftInput -> convertInput(leftInput, computeJoinLeftKeys(join)),
                        right,
                        rightInput -> convertInput(rightInput, computeJoinRightKeys(join)),
                        join.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL()));
        call.transformTo(newJoin);
    }

    protected Tuple2<Option<IntervalJoinSpec.WindowBounds>, Option<RexNode>> extractWindowBounds(
            FlinkLogicalJoin join) {
        TableConfig tableConfig = ShortcutUtils.unwrapTableConfig(join);
        return JavaScalaConversionUtil.toJava(
                IntervalJoinUtil.extractWindowBoundsFromPredicate(
                        join.getCondition(),
                        join.getLeft().getRowType().getFieldCount(),
                        join.getRowType(),
                        join.getCluster().getRexBuilder(),
                        tableConfig,
                        ShortcutUtils.unwrapClassLoader(join)));
    }

    private RelNode convertInput(RelNode input, Collection<Integer> columns) {
        RelTraitSet requiredTraitSet = toHashTraitByColumns(columns, input.getTraitSet());
        return RelOptRule.convert(input, requiredTraitSet);
    }

    private RelTraitSet toHashTraitByColumns(
            Collection<Integer> columns, RelTraitSet inputTraitSet) {
        FlinkRelDistribution distribution =
                columns.isEmpty()
                        ? FlinkRelDistribution.SINGLETON()
                        : FlinkRelDistribution.hash(
                                columns.stream().mapToInt(Integer::intValue).toArray(), true);
        return inputTraitSet.replace(FlinkConventions.STREAM_PHYSICAL()).replace(distribution);
    }

    public Collection<Integer> computeJoinLeftKeys(FlinkLogicalJoin join) {
        return join.analyzeCondition().leftKeys;
    }

    public Collection<Integer> computeJoinRightKeys(FlinkLogicalJoin join) {
        return join.analyzeCondition().rightKeys;
    }

    public abstract FlinkRelNode transform(
            FlinkLogicalJoin join,
            FlinkRelNode leftInput,
            Function<RelNode, RelNode> leftConversion,
            FlinkRelNode rightInput,
            Function<RelNode, RelNode> rightConversion,
            RelTraitSet providedTraitSet);

    /** Configuration for {@link StreamPhysicalConstantTableFunctionScanRule}. */
    @Value.Immutable
    public interface StreamPhysicalJoinRuleBaseRuleConfig extends RelRule.Config {
        RelRule.OperandTransform OPERAND_TRANSFORM =
                b0 ->
                        b0.operand(FlinkLogicalJoin.class)
                                .inputs(
                                        b1 -> b1.operand(FlinkLogicalRel.class).anyInputs(),
                                        b2 -> b2.operand(FlinkLogicalRel.class).anyInputs());
        StreamPhysicalJoinRuleBase.StreamPhysicalJoinRuleBaseRuleConfig DEFAULT =
                ImmutableStreamPhysicalJoinRuleBase.StreamPhysicalJoinRuleBaseRuleConfig.builder()
                        .build()
                        .withOperandSupplier(OPERAND_TRANSFORM)
                        .withDescription("StreamPhysicalJoinRuleBase")
                        .as(StreamPhysicalJoinRuleBase.StreamPhysicalJoinRuleBaseRuleConfig.class);

        @Override
        default StreamPhysicalJoinRuleBase toRule() {
            throw new RuntimeException();
        }
    }
}
