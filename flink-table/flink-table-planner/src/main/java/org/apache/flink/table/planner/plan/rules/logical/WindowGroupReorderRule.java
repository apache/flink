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
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window.Group;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Planner rule that makes the over window groups which have the same shuffle keys and order keys
 * together.
 */
@Value.Enclosing
public class WindowGroupReorderRule
        extends RelRule<WindowGroupReorderRule.WindowGroupReorderRuleConfig> {

    public static final WindowGroupReorderRule INSTANCE =
            WindowGroupReorderRule.WindowGroupReorderRuleConfig.DEFAULT.toRule();

    private WindowGroupReorderRule(WindowGroupReorderRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalWindow window = call.rel(0);
        return window.groups.size() > 1;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalWindow window = call.rel(0);
        RelNode input = call.rel(1);
        List<Group> oldGroups = new ArrayList<>(window.groups);
        List<Group> sequenceGroups = new ArrayList<>(window.groups);

        sequenceGroups.sort(
                (o1, o2) -> {
                    int keyComp = o1.keys.compareTo(o2.keys);
                    if (keyComp == 0) {
                        return compareRelCollation(o1.orderKeys, o2.orderKeys);
                    } else {
                        return keyComp;
                    }
                });

        List<Group> reverseSequenceGroups = new ArrayList<>(window.groups);
        Collections.reverse(reverseSequenceGroups);
        if (!sequenceGroups.equals(oldGroups) && !reverseSequenceGroups.equals(oldGroups)) {
            int offset = input.getRowType().getFieldCount();
            List<int[]> aggTypeIndexes = new ArrayList<>();
            for (Group group : oldGroups) {
                int aggCount = group.aggCalls.size();
                int[] typeIndexes = new int[aggCount];
                for (int i = 0; i < aggCount; i++) {
                    typeIndexes[i] = offset + i;
                }
                offset += aggCount;
                aggTypeIndexes.add(typeIndexes);
            }

            offset = input.getRowType().getFieldCount();
            List<Integer> mapToOldTypeIndexes =
                    IntStream.range(0, offset).boxed().collect(Collectors.toList());
            for (Group newGroup : sequenceGroups) {
                int aggCount = newGroup.aggCalls.size();
                int oldIndex = oldGroups.indexOf(newGroup);
                offset += aggCount;
                for (int aggIndex = 0; aggIndex < aggCount; aggIndex++) {
                    mapToOldTypeIndexes.add(aggTypeIndexes.get(oldIndex)[aggIndex]);
                }
            }

            List<Map.Entry<String, RelDataType>> newFieldList =
                    mapToOldTypeIndexes.stream()
                            .map(index -> window.getRowType().getFieldList().get(index))
                            .collect(Collectors.toList());
            RelDataType intermediateRowType =
                    window.getCluster().getTypeFactory().createStructType(newFieldList);
            LogicalWindow newLogicalWindow =
                    LogicalWindow.create(
                            window.getCluster().getPlanner().emptyTraitSet(),
                            input,
                            window.constants,
                            intermediateRowType,
                            sequenceGroups);

            List<Integer> sortedIndices =
                    IntStream.range(0, mapToOldTypeIndexes.size())
                            .boxed()
                            .sorted(Comparator.comparingInt(mapToOldTypeIndexes::get))
                            .collect(Collectors.toList());

            List<RexInputRef> projects =
                    sortedIndices.stream()
                            .map(
                                    index ->
                                            new RexInputRef(
                                                    index, newFieldList.get(index).getValue()))
                            .collect(Collectors.toList());

            LogicalProject project =
                    LogicalProject.create(
                            newLogicalWindow,
                            Collections.emptyList(),
                            projects,
                            window.getRowType());
            call.transformTo(project);
        }
    }

    private int compareRelCollation(RelCollation o1, RelCollation o2) {
        int comp = o1.compareTo(o2);
        if (comp == 0) {
            List<RelFieldCollation> collations1 = o1.getFieldCollations();
            List<RelFieldCollation> collations2 = o2.getFieldCollations();
            for (int index = 0; index < collations1.size(); index++) {
                RelFieldCollation collation1 = collations1.get(index);
                RelFieldCollation collation2 = collations2.get(index);
                int direction =
                        collation1
                                .getDirection()
                                .shortString
                                .compareTo(collation2.getDirection().shortString);
                if (direction == 0) {
                    int nullDirection =
                            Integer.compare(
                                    collation1.nullDirection.nullComparison,
                                    collation2.nullDirection.nullComparison);
                    if (nullDirection != 0) {
                        return nullDirection;
                    }
                } else {
                    return direction;
                }
            }
        }
        return comp;
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface WindowGroupReorderRuleConfig extends RelRule.Config {
        WindowGroupReorderRule.WindowGroupReorderRuleConfig DEFAULT =
                ImmutableWindowGroupReorderRule.WindowGroupReorderRuleConfig.builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(LogicalWindow.class)
                                                .inputs(
                                                        b1 ->
                                                                b1.operand(RelNode.class)
                                                                        .anyInputs()))
                        .withDescription("ExchangeWindowGroupRule");

        @Override
        default WindowGroupReorderRule toRule() {
            return new WindowGroupReorderRule(this);
        }
    }
}
