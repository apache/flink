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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A {@link FlinkOptimizeProgram} that marks ChangelogNormalize nodes using the same source and
 * determines common filters if any. This program is a preparation step for {@link
 * PushCalcPastChangelogNormalizeRule}.
 *
 * <p>There might be several scenarios:
 *
 * <p>1. Same conditions for ChangelogNormalize nodes example of the query
 *
 * <pre>
 * {@code SELECT * FROM T WHERE f1 < 0
 * UNION ALL
 * SELECT * FROM T WHERE f1 < 0}
 * </pre>
 *
 * <p>The plan before {@link PushCalcPastChangelogNormalizeRule}
 *
 * <pre>
 * {@code Union(all=[true], union=[f0, f1])
 * :- Calc(select=[f0, f1], where=[(f1 < 0)])(reuse_id=[1])
 * :  +- ChangelogNormalize(key=[f1])
 * :     +- Exchange(distribution=[hash[f1]])
 * :        +- TableSourceScan(table=[[default_catalog, default_database, T]], fields=[f0, f1])
 * +- Reused(reference_id=[1])}
 * </pre>
 *
 * <p>Since the filter condition is same for both, it will be pushed down. ChangelogNormalize node
 * will be reused.
 *
 * <pre>
 * {@code Union(all=[true], union=[f0, f1])
 * :- ChangelogNormalize(key=[f1])(reuse_id=[1])
 * :  +- Exchange(distribution=[hash[f1]])
 * :     +- Calc(select=[f0, f1], where=[(f1 < 0)])
 * :        +- TableSourceScan(table=[[default_catalog, default_database, T]], fields=[f0, f1])
 * +- Reused(reference_id=[1])}
 * </pre>
 *
 * <p>2. Conditions are different
 *
 * <pre>
 * {@code SELECT * FROM T WHERE f1 < 0
 * UNION ALL
 * SELECT * FROM T WHERE f1 < 10}
 * </pre>
 *
 * <p>The plans before and after are the same {@link PushCalcPastChangelogNormalizeRule}. Conditions
 * are different, thus, to keep reusing ChangelogNormalize they will not be pushed down.
 *
 * <pre>
 * {@code Union(all=[true], union=[f0, f1])
 * :- Calc(select=[f0, f1], where=[(f1 < 0)])
 * :  +- ChangelogNormalize(key=[f1])(reuse_id=[1])
 * :     +- Exchange(distribution=[hash[f1]])
 * :        +- TableSourceScan(table=[[default_catalog, default_database, T]], fields=[f0, f1])
 * +- Calc(select=[f0, f1], where=[(f1 < 10)])
 *    +- Reused(reference_id=[1])}
 * </pre>
 *
 * <p>3. Conditions are partially overlapping
 *
 * <pre>{@code SELECT * FROM T WHERE f1 < 10 AND f1 > 0
 * UNION ALL
 * SELECT * FROM T WHERE f1 > 0 AND f1 < 20}</pre>
 *
 * <p>In the plan before {@link PushCalcPastChangelogNormalizeRule} the conditions above
 * ChangelogNormalize.
 *
 * <pre>
 *  {@code Union(all=[true], union=[f0, f1])
 * :- Calc(select=[f0, f1], where=[SEARCH(f1, Sarg[(0..10)])])
 * :  +- ChangelogNormalize(key=[f1])(reuse_id=[1])
 * :     +- Exchange(distribution=[hash[f1]])
 * :        +- TableSourceScan(table=[[default_catalog, default_database, T]], fields=[f0, f1])
 * +- Calc(select=[f0, f1], where=[SEARCH(f1, Sarg[(0..20)])])
 *    +- Reused(reference_id=[1])}
 * </pre>
 *
 * <p>After applying {@link PushCalcPastChangelogNormalizeRule} the condition should be splitted
 * into common and not common parts. Common part should be pushed down as below.
 *
 * <pre>{@code Union(all=[true], union=[f0, f1])
 * :- Calc(select=[f0, f1], where=[(f1 < 10)])
 * :  +- ChangelogNormalize(key=[f1])(reuse_id=[1])
 * :     +- Exchange(distribution=[hash[f1]])
 * :        +- Calc(select=[f0, f1], where=[(f1 > 0)])
 * :           +- TableSourceScan(table=[[default_catalog, default_database, T]], fields=[f0, f1])
 * +- Calc(select=[f0, f1], where=[(f1 < 20)])
 *    +- Reused(reference_id=[1])}</pre>
 */
public class FlinkMarkChangelogNormalizeProgram
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
            final List<ChangelogNormalizeContext> changelogNormalizeContexts = entry.getValue();
            if (changelogNormalizeContexts.size() <= 1) {
                // we are interested only in cases with at least 2 changelog normalize nodes having
                // the same source
                continue;
            }

            final List<RexNode> commons =
                    calculateCommonCondition(rexBuilder, changelogNormalizeContexts);
            for (ChangelogNormalizeContext ctx : changelogNormalizeContexts) {
                ctx.getChangelogNormalize().markSourceReuse();
                if (!commons.isEmpty()) {
                    ctx.getChangelogNormalize().setCommonFilter(commons.toArray(new RexNode[0]));
                }
            }
        }
        return root;
    }

    private List<RexNode> calculateCommonCondition(
            RexBuilder rexBuilder, List<ChangelogNormalizeContext> changelogNormalizeContexts) {
        if (changelogNormalizeContexts.stream()
                .map(ChangelogNormalizeContext::getConditions)
                .anyMatch(Objects::isNull)) {
            return List.of();
        }

        final RexNode or =
                rexBuilder.makeCall(
                        SqlStdOperatorTable.OR,
                        changelogNormalizeContexts.stream()
                                .map(ChangelogNormalizeContext::getConditions)
                                .collect(Collectors.toList()));
        final RexCall factors = (RexCall) RexUtil.pullFactors(rexBuilder, or);

        final List<RexNode> commonCondition = new ArrayList<>();
        // Since we are interested in factors only then look for AND
        if (factors.getKind() == SqlKind.AND) {
            for (RexNode node : factors.getOperands()) {
                // If there is OR on top level then it is not a common factor anymore
                if (node.getKind() == SqlKind.OR) {
                    break;
                }
                commonCondition.addAll(RelOptUtil.conjunctions(RexUtil.toCnf(rexBuilder, node)));
            }
            return commonCondition;
        }
        return List.of();
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
                    RexLocalRef localRef = calc.getProgram().getCondition();
                    final RexNode condition;
                    if (localRef == null) {
                        condition = null;
                    } else {
                        // Expanded Sarg allows to extract partial common filter out of it
                        RexNode rexNodeWithExpandedSearch =
                                RexUtil.expandSearch(
                                        rexBuilder,
                                        calc.getProgram(),
                                        calc.getProgram().expandLocalRef(localRef));
                        // First pull factors from conditions per Changelog Normalize node
                        // then find the common for all of them
                        condition = RexUtil.pullFactors(rexBuilder, rexNodeWithExpandedSearch);
                    }
                    gatherTableScanToChangelogNormalizeMap(
                            input,
                            ChangelogNormalizeContext.of(changelogNormalize, condition),
                            map);
                }
            } else {
                gatherTableScanToChangelogNormalizeMap(input, map, rexBuilder);
            }
        }
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
        private final RexNode conditions;

        private ChangelogNormalizeContext(
                StreamPhysicalChangelogNormalize changelogNormalize, RexNode conditions) {
            this.changelogNormalize = changelogNormalize;
            this.conditions = conditions;
        }

        public static ChangelogNormalizeContext of(
                StreamPhysicalChangelogNormalize changelogNormalize, RexNode conditions) {
            return new ChangelogNormalizeContext(changelogNormalize, conditions);
        }

        public StreamPhysicalChangelogNormalize getChangelogNormalize() {
            return changelogNormalize;
        }

        public RexNode getConditions() {
            return conditions;
        }
    }
}
