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

import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import scala.Tuple2;

/**
 * Planner rule that tries to push a filter into a {@link LogicalTableScan}, which table is a {@link
 * TableSourceTable}. And the table source in the table is a {@link SupportsFilterPushDown}.
 */
public class PushFilterIntoTableSourceScanRule extends PushFilterIntoSourceScanRuleBase {
    public static final PushFilterIntoTableSourceScanRule INSTANCE =
            new PushFilterIntoTableSourceScanRule();

    public PushFilterIntoTableSourceScanRule() {
        super(
                operand(Filter.class, operand(LogicalTableScan.class, none())),
                "PushFilterIntoTableSourceScanRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        if (!super.matches(call)) {
            return false;
        }

        Filter filter = call.rel(0);
        if (filter.getCondition() == null) {
            return false;
        }

        LogicalTableScan scan = call.rel(1);
        TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);

        return canPushdownFilter(tableSourceTable);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        LogicalTableScan scan = call.rel(1);
        TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
        pushFilterIntoScan(call, filter, scan, table);
    }

    private void pushFilterIntoScan(
            RelOptRuleCall call,
            Filter filter,
            LogicalTableScan scan,
            FlinkPreparingTableBase relOptTable) {

        RelBuilder relBuilder = call.builder();
        Tuple2<RexNode[], RexNode[]> extractedPredicates =
                extractPredicates(
                        filter.getInput().getRowType().getFieldNames().toArray(new String[0]),
                        filter.getCondition(),
                        scan,
                        relBuilder.getRexBuilder());

        RexNode[] convertiblePredicates = extractedPredicates._1;
        RexNode[] unconvertedPredicates = extractedPredicates._2;
        if (convertiblePredicates.length == 0) {
            // no condition can be translated to expression
            return;
        }

        Tuple2<SupportsFilterPushDown.Result, TableSourceTable> scanAfterPushdownWithResult =
                resolveFiltersAndCreateTableSourceTable(
                        convertiblePredicates,
                        relOptTable.unwrap(TableSourceTable.class),
                        scan,
                        relBuilder);

        SupportsFilterPushDown.Result result = scanAfterPushdownWithResult._1;
        TableSourceTable tableSourceTable = scanAfterPushdownWithResult._2;

        LogicalTableScan newScan =
                LogicalTableScan.create(scan.getCluster(), tableSourceTable, scan.getHints());
        if (result.getRemainingFilters().isEmpty() && unconvertedPredicates.length == 0) {
            call.transformTo(newScan);
        } else {
            RexNode remainingCondition =
                    createRemainingCondition(
                            relBuilder, result.getRemainingFilters(), unconvertedPredicates);
            RexNode simplifiedRemainingCondition =
                    FlinkRexUtil.simplify(relBuilder.getRexBuilder(), remainingCondition);
            Filter newFilter =
                    filter.copy(filter.getTraitSet(), newScan, simplifiedRemainingCondition);
            call.transformTo(newFilter);
        }
    }
}
