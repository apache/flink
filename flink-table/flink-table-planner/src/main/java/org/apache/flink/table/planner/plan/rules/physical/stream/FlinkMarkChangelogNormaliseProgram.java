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

import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.optimize.program.FlinkOptimizeProgram;
import org.apache.flink.table.planner.plan.optimize.program.StreamOptimizeContext;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link FlinkOptimizeProgram} that marks changelog normalize nodes using the same source and
 * determines common filters if any.
 */
public class FlinkMarkChangelogNormaliseProgram
        implements FlinkOptimizeProgram<StreamOptimizeContext> {
    @Override
    public RelNode optimize(RelNode root, StreamOptimizeContext context) {
        final Map<TableScan, List<ChangelogNormalizeContext>> tableScansToChangelogNormalize =
                new HashMap<>();
        final FlinkRexBuilder rexBuilder =
                new FlinkRexBuilder(context.getFlinkRelBuilder().getTypeFactory());
        for (RelNode relNode : root.getInputs()) {
            gatherTableScanToChangelogNormalizeMap(
                    relNode, tableScansToChangelogNormalize, rexBuilder);
        }

        for (Map.Entry<TableScan, List<ChangelogNormalizeContext>> entry :
                tableScansToChangelogNormalize.entrySet()) {
            if (entry.getValue().size() <= 1) {
                continue;
            }

            List<ChangelogNormalizeContext> list = entry.getValue();
            list.sort(Comparator.comparingInt(o -> o.rexNodes.size()));
            Set<RexNode> common = new HashSet<>(list.get(0).rexNodes);

            for (int i = 1; i < list.size() && !common.isEmpty(); i++) {
                common.retainAll(list.get(i).rexNodes);
            }

            for (ChangelogNormalizeContext ctx : entry.getValue()) {
                ctx.getChangelogNormalize().markSourceReuse();
                if (!common.isEmpty()) {
                    ctx.getChangelogNormalize().setCommonFilter(common.toArray(new RexNode[0]));
                }
            }
        }
        return root;
    }

    private void gatherTableScanToChangelogNormalizeMap(
            RelNode curRelNode,
            Map<TableScan, List<ChangelogNormalizeContext>> map,
            FlinkRexBuilder rexBuilder) {
        for (RelNode input : curRelNode.getInputs()) {
            if (input instanceof StreamPhysicalChangelogNormalize) {
                StreamPhysicalChangelogNormalize changelogNormalize =
                        (StreamPhysicalChangelogNormalize) input;
                if (curRelNode instanceof StreamPhysicalCalc) {
                    StreamPhysicalCalc calc = (StreamPhysicalCalc) curRelNode;
                    final List<RexNode> list = getConditions(calc, rexBuilder);
                    gatherTableScanToChangelogNormalizeMap(
                            input, ChangelogNormalizeContext.of(changelogNormalize, list), map);
                }
            } else {
                gatherTableScanToChangelogNormalizeMap(input, map, rexBuilder);
            }
        }
    }

    private List<RexNode> getConditions(StreamPhysicalCalc calc, RexBuilder rexBuilder) {
        final RexProgram program = calc.getProgram();
        if (program.getCondition() != null) {
            final RexNode rexNode =
                    RexUtil.toCnf(
                            rexBuilder,
                            FlinkRexUtil.expandSearch(
                                    rexBuilder, program.expandLocalRef(program.getCondition())));
            return RelOptUtil.conjunctions(rexNode);
        }
        return List.of();
    }

    private void gatherTableScanToChangelogNormalizeMap(
            RelNode cur,
            ChangelogNormalizeContext context,
            Map<TableScan, List<ChangelogNormalizeContext>> currentMap) {
        if (cur instanceof TableScan) {
            currentMap.computeIfAbsent((TableScan) cur, (k) -> new ArrayList<>()).add(context);
        } else {
            for (RelNode relNode : cur.getInputs()) {
                gatherTableScanToChangelogNormalizeMap(relNode, context, currentMap);
            }
        }
    }

    private static class ChangelogNormalizeContext {
        private final StreamPhysicalChangelogNormalize changelogNormalize;
        private final List<RexNode> rexNodes;

        public ChangelogNormalizeContext(
                StreamPhysicalChangelogNormalize changelogNormalize, List<RexNode> rexNodes) {
            this.changelogNormalize = changelogNormalize;
            this.rexNodes = rexNodes;
        }

        public static ChangelogNormalizeContext of(
                StreamPhysicalChangelogNormalize changelogNormalize, List<RexNode> rexNodes) {
            return new ChangelogNormalizeContext(changelogNormalize, rexNodes);
        }

        public StreamPhysicalChangelogNormalize getChangelogNormalize() {
            return changelogNormalize;
        }
    }
}
