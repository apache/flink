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

package org.apache.flink.table.planner.plan.nodes.logical;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlRankFunction;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Sub-class of {@link Window} that is a relational expression which represents a set of over window
 * aggregates in Flink.
 */
public class FlinkLogicalOverAggregate extends Window implements FlinkLogicalRel {
    public static final ConverterRule CONVERTER =
            new FlinkLogicalOverAggregateConverter(
                    ConverterRule.Config.INSTANCE.withConversion(
                            LogicalWindow.class,
                            Convention.NONE,
                            FlinkConventions.LOGICAL(),
                            "FlinkLogicalOverAggregateConverter"));

    protected FlinkLogicalOverAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode input,
            List<RexLiteral> constants,
            RelDataType rowType,
            List<Group> groups) {
        super(cluster, traitSet, hints, input, constants, rowType, groups);
    }

    public FlinkLogicalOverAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            List<RexLiteral> constants,
            RelDataType rowType,
            List<Group> groups) {
        super(cluster, traitSet, input, constants, rowType, groups);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return copy(traitSet, inputs, rowType, groups);
    }

    public RelNode copy(
            RelTraitSet traitSet,
            List<RelNode> inputs,
            RelDataType rowType,
            List<Window.Group> groups) {
        return new FlinkLogicalOverAggregate(
                getCluster(), traitSet, inputs.get(0), constants, rowType, groups);
    }

    private static class FlinkLogicalOverAggregateConverter extends ConverterRule {

        protected FlinkLogicalOverAggregateConverter(Config config) {
            super(config);
        }

        @Override
        public @Nullable RelNode convert(RelNode rel) {
            LogicalWindow window = (LogicalWindow) rel;
            RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
            RelTraitSet traitSet =
                    rel.getCluster()
                            .traitSetOf(FlinkConventions.LOGICAL())
                            .replaceIfs(
                                    RelCollationTraitDef.INSTANCE,
                                    () ->
                                            RelMdCollation.window(
                                                    mq, window.getInput(), window.groups))
                            .simplify();
            RelNode newInput = RelOptRule.convert(window.getInput(), FlinkConventions.LOGICAL());

            window.groups.forEach(
                    group -> {
                        final int orderKeySize = group.orderKeys.getFieldCollations().size();
                        group.aggCalls.forEach(
                                winAggCall -> {
                                    if (orderKeySize == 0
                                            && winAggCall.op instanceof SqlRankFunction) {
                                        throw new ValidationException(
                                                "Over Agg: The window rank function requires order by clause with non-constant fields. "
                                                        + "please re-check the over window statement.");
                                    }
                                });
                    });

            return new FlinkLogicalOverAggregate(
                    rel.getCluster(),
                    traitSet,
                    newInput,
                    window.constants,
                    window.getRowType(),
                    window.groups);
        }
    }
}
