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

import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSort;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalLimit;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Rule that matches {@link FlinkLogicalSort} with empty sort fields, and converts it to {@link
 * StreamPhysicalLimit}.
 */
public class StreamPhysicalLimitRule extends ConverterRule {

    public static final RelOptRule INSTANCE =
            new StreamPhysicalLimitRule(
                    Config.INSTANCE.withConversion(
                            FlinkLogicalSort.class,
                            FlinkConventions.LOGICAL(),
                            FlinkConventions.STREAM_PHYSICAL(),
                            "StreamPhysicalLimitRule"));

    protected StreamPhysicalLimitRule(final Config config) {
        super(config);
    }

    @Override
    public boolean matches(final RelOptRuleCall call) {
        final FlinkLogicalSort sort = call.rel(0);
        return sort.getCollation().getFieldCollations().isEmpty();
    }

    @Override
    public @Nullable RelNode convert(final RelNode rel) {
        final FlinkLogicalSort sort = (FlinkLogicalSort) rel;
        final RelNode input = sort.getInput();

        // require SINGLETON exchange
        final RelTraitSet newTraitSet =
                input.getTraitSet()
                        .replace(FlinkConventions.STREAM_PHYSICAL())
                        .replace(FlinkRelDistribution.SINGLETON());
        final RelNode newInput = RelOptRule.convert(input, newTraitSet);

        // create StreamPhysicalLimit
        return new StreamPhysicalLimit(
                rel.getCluster(), newTraitSet, newInput, sort.offset, sort.fetch);
    }
}
