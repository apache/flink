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

package org.apache.flink.table.planner.plan.rules.physical.batch;

import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalUnion;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalUnion;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import java.util.List;
import java.util.stream.Collectors;

/** Rule that converts {@link FlinkLogicalUnion} to @{@link BatchPhysicalUnion}. */
public class BatchPhysicalUnionRule extends ConverterRule {

    public static final RelOptRule INSTANCE =
            new BatchPhysicalUnionRule(
                    Config.INSTANCE
                            .withConversion(
                                    FlinkLogicalUnion.class,
                                    FlinkConventions.LOGICAL(),
                                    FlinkConventions.BATCH_PHYSICAL(),
                                    "BatchPhysicalUnionRule")
                            .withRuleFactory(BatchPhysicalUnionRule::new));

    protected BatchPhysicalUnionRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return ((FlinkLogicalUnion) call.rel(0)).all;
    }

    @Override
    public RelNode convert(RelNode rel) {
        FlinkLogicalUnion union = (FlinkLogicalUnion) rel;
        RelTraitSet traitSet = rel.getTraitSet().replace(FlinkConventions.BATCH_PHYSICAL());
        List<RelNode> newInputs =
                union.getInputs().stream()
                        .map(input -> RelOptRule.convert(input, FlinkConventions.BATCH_PHYSICAL()))
                        .collect(Collectors.toList());

        return new BatchPhysicalUnion(
                rel.getCluster(), traitSet, newInputs, union.all, rel.getRowType());
    }
}
