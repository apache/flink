/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalMultiJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMultiJoin;
import org.apache.flink.table.planner.plan.trait.FlinkRelDistribution;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.AttributeBasedJoinKeyExtractor.ConditionAttributeRef;
import org.apache.flink.table.runtime.operators.join.stream.keyselector.JoinKeyExtractor;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.plan.utils.MultiJoinUtil.createJoinAttributeMap;

/** Rule that converts {@link FlinkLogicalMultiJoin} to {@link StreamPhysicalMultiJoin}. */
public class StreamPhysicalMultiJoinRule extends ConverterRule {
    public static final RelOptRule INSTANCE = new StreamPhysicalMultiJoinRule();

    private StreamPhysicalMultiJoinRule() {
        super(
                Config.INSTANCE.withConversion(
                        FlinkLogicalMultiJoin.class,
                        FlinkConventions.LOGICAL(),
                        FlinkConventions.STREAM_PHYSICAL(),
                        "StreamPhysicalMultiJoinRule"));
    }

    @Override
    public RelNode convert(final RelNode rel) {
        final FlinkLogicalMultiJoin multiJoin = (FlinkLogicalMultiJoin) rel;
        final Map<Integer, List<ConditionAttributeRef>> joinAttributeMap =
                createJoinAttributeMap(multiJoin.getInputs(), multiJoin.getJoinConditions());
        final List<RowType> inputRowTypes =
                multiJoin.getInputs().stream()
                        .map(i -> FlinkTypeFactory.toLogicalRowType(i.getRowType()))
                        .collect(Collectors.toList());
        final JoinKeyExtractor keyExtractor =
                new AttributeBasedJoinKeyExtractor(joinAttributeMap, inputRowTypes);
        // Apply hash distribution traits to all inputs based on the commonJoinKeys
        final List<RelNode> newInputs =
                createHashDistributedInputs(multiJoin.getInputs(), keyExtractor);
        final RelTraitSet traitSet = rel.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());

        return new StreamPhysicalMultiJoin(
                multiJoin.getCluster(),
                traitSet,
                newInputs,
                multiJoin.getJoinFilter(),
                multiJoin.getRowType(),
                multiJoin.getJoinConditions(),
                multiJoin.getJoinTypes(),
                joinAttributeMap,
                multiJoin.getPostJoinFilter(),
                multiJoin.getHints(),
                keyExtractor);
    }

    private List<RelNode> createHashDistributedInputs(
            final List<RelNode> inputs, final JoinKeyExtractor keyExtractor) {
        final List<RelNode> newInputs = new ArrayList<>();

        for (int i = 0; i < inputs.size(); i++) {
            final RelNode input = inputs.get(i);
            final RelTraitSet inputTraitSet = createInputTraitSet(input, keyExtractor, i);
            final RelNode convertedInput = RelOptRule.convert(input, inputTraitSet.simplify());
            newInputs.add(convertedInput);
        }

        return newInputs;
    }

    private RelTraitSet createInputTraitSet(
            final RelNode input, final JoinKeyExtractor keyExtractor, final int inputIndex) {
        final int[] commonJoinKeyIndices = keyExtractor.getCommonJoinKeyIndices(inputIndex);

        RelTraitSet inputTraitSet = input.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());

        if (commonJoinKeyIndices.length > 0) {
            final FlinkRelDistribution hashDistribution =
                    FlinkRelDistribution.hash(commonJoinKeyIndices, true);
            inputTraitSet = inputTraitSet.replace(hashDistribution);
        } else {
            inputTraitSet = inputTraitSet.replace(FlinkRelDistribution.SINGLETON());
        }

        return inputTraitSet;
    }
}
