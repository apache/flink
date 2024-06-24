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

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Converts {@link LogicalAggregate} instance's project into a {@link LogicalFilter}, so that, it
 * can be pushed down to the source operator. This rule matches aggregate expressions with a filter
 * clause (e.g., count(*) filter (where id > 1)). If there are multiple aggregate expressions with
 * filter clauses, then no condition is pushed down to the source operator (to ensure query
 * correctness).
 */
public class AggregateFilterPushdownRule extends RelOptRule {
    public static final AggregateFilterPushdownRule INSTANCE = new AggregateFilterPushdownRule();

    public AggregateFilterPushdownRule() {
        super(
                operand(LogicalAggregate.class, operand(LogicalProject.class, any())),
                "AggregateFilterPushdownRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalAggregate agg = call.rel(0);
        LogicalProject project = call.rel(1);
        // if there are other stateful operators between source and this operator,
        // pushing down filters might result incorrect results
        if (!reachableToSource(project)) {
            return false;
        }

        Integer aggsWithFilter =
                agg.getAggCallList().stream()
                        .map(aggCall -> aggCall.hasFilter() ? 1 : 0)
                        .reduce(0, Integer::sum);

        // make sure that all aggregates are using the filter
        if (aggsWithFilter != agg.getAggCallList().size()) {
            return false;
        }

        // make sure that there is only one filter
        List<RexCall> projectExprs =
                project.getProjects().stream()
                        .filter(this::isProjectFilter)
                        .map(expr -> (RexCall) expr)
                        .collect(Collectors.toList());
        return projectExprs.size() == 1;
    }

    private boolean reachableToSource(RelNode node) {
        if ((node instanceof Filter) || (node instanceof Project) || (node instanceof Calc)) {
            return reachableToSource(node.getInput(0));
        } else if (node instanceof TableScan) {
            return true;
        } else if (node instanceof HepRelVertex) {
            return reachableToSource(((HepRelVertex) node).getCurrentRel());
        } else {
            return false;
        }
    }

    private boolean isProjectFilter(RexNode rexNode) {
        return ((rexNode instanceof RexCall)
                && (rexNode.getType().getSqlTypeName() == SqlTypeName.BOOLEAN));
    }

    private Optional<Tuple2<RelNode, Integer>> separateFilterFromProject(LogicalProject project) {
        List<RexNode> projectExprs = project.getProjects();
        Optional<RexNode> maybeFilterProject =
                projectExprs.stream().filter(this::isProjectFilter).findFirst();
        if (!maybeFilterProject.isPresent()) {
            return Optional.empty();
        }

        RexNode filterProject = maybeFilterProject.get();
        int filterProjectIdx = projectExprs.indexOf(filterProject);

        LogicalFilter filter =
                new LogicalFilter(
                        project.getCluster(),
                        project.getTraitSet(),
                        project.getInput(),
                        filterProject);

        if (projectExprs.size() == 1) {
            return Optional.of(new Tuple2<>(filter, 0));
        }

        List<RexNode> newExprs = new ArrayList<>(projectExprs);
        newExprs.remove(filterProjectIdx);

        List<String> newFieldNames = new ArrayList<>(project.getRowType().getFieldNames());
        newFieldNames.remove(filterProjectIdx);

        return Optional.of(
                new Tuple2<>(
                        LogicalProject.create(filter, project.getHints(), newExprs, newFieldNames),
                        filterProjectIdx));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalAggregate agg = call.rel(0);
        LogicalProject project = call.rel(1);

        Optional<Tuple2<RelNode, Integer>> maybeNode = separateFilterFromProject(project);
        if (!maybeNode.isPresent()) {
            return;
        }
        RelNode newFilter = maybeNode.get().f0;
        Integer filteredIdx = maybeNode.get().f1;

        List<AggregateCall> newAggCall = new ArrayList<>();
        for (AggregateCall aggregateCall : agg.getAggCallList()) {
            List<Integer> argList = aggregateCall.getArgList();
            // One filter expr is already pushed down, so adjust arg list accordingly
            List<Integer> newArgList =
                    argList.stream()
                            .map(
                                    arg -> {
                                        if (arg == 0) {
                                            return arg;
                                        } else if (arg > filteredIdx) {
                                            return arg - 1;
                                        } else {
                                            return arg;
                                        }
                                    })
                            .collect(Collectors.toList());
            newAggCall.add(
                    AggregateCall.create(
                            aggregateCall.getAggregation(),
                            aggregateCall.isDistinct(),
                            aggregateCall.isApproximate(),
                            aggregateCall.ignoreNulls(),
                            newArgList,
                            -1,
                            aggregateCall.distinctKeys,
                            aggregateCall.collation,
                            aggregateCall.getType(),
                            aggregateCall.getName()));
        }

        LogicalAggregate newAgg =
                new LogicalAggregate(
                        agg.getCluster(),
                        agg.getTraitSet(),
                        agg.getHints(),
                        newFilter,
                        agg.getGroupSet(),
                        agg.getGroupSets(),
                        newAggCall);
        call.transformTo(newAgg);
    }
}
