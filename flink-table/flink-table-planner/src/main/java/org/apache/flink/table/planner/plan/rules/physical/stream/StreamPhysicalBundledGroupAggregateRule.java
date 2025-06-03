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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalBundledGroupAggregate;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.planner.plan.utils.BundledAggUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Aggregate;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Rule that converts {@link FlinkLogicalAggregate} to {@link StreamPhysicalBundledGroupAggregate}.
 */
public class StreamPhysicalBundledGroupAggregateRule extends ConverterRule {

    public static final RelOptRule INSTANCE =
            new StreamPhysicalBundledGroupAggregateRule(
                    Config.INSTANCE.withConversion(
                            FlinkLogicalAggregate.class,
                            FlinkConventions.LOGICAL(),
                            FlinkConventions.STREAM_PHYSICAL(),
                            "StreamPhysicalBundledGroupAggregateRule"));

    public StreamPhysicalBundledGroupAggregateRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalAggregate agg = call.rel(0);

        if (agg.getGroupType() != Aggregate.Group.SIMPLE) {
            throw new TableException("GROUPING SETS are currently not supported.");
        }

        return agg.getAggCallList().stream().anyMatch(BundledAggUtil::containsBatchAggCall);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        FlinkLogicalAggregate agg = (FlinkLogicalAggregate) rel;
        FlinkRelDistribution requiredDistribution;
        if (agg.getGroupCount() != 0) {
            requiredDistribution = FlinkRelDistribution.hash(agg.getGroupSet().asList(), true);
        } else {
            requiredDistribution = FlinkRelDistribution.SINGLETON();
        }
        RelTraitSet requiredTraitSet =
                rel.getCluster()
                        .getPlanner()
                        .emptyTraitSet()
                        .replace(requiredDistribution)
                        .replace(FlinkConventions.STREAM_PHYSICAL());
        RelTraitSet providedTraitSet =
                rel.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());
        RelNode newInput = RelOptRule.convert(agg.getInput(), requiredTraitSet);

        return new StreamPhysicalBundledGroupAggregate(
                rel.getCluster(),
                providedTraitSet,
                newInput,
                rel.getRowType(),
                agg.getGroupSet().toArray(),
                agg.getAggCallList(),
                agg.partialFinalType(),
                agg.getHints());
    }
}
