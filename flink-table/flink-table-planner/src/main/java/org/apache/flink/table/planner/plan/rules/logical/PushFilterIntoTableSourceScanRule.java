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
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import scala.Tuple2;

/**
 * Pushes filters from a {@link Filter} into a {@link LogicalTableScan}. Physical filters use {@link
 * SupportsFilterPushDown}; metadata filters use {@link
 * SupportsReadingMetadata#applyMetadataFilters}.
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

        return canPushdownFilter(tableSourceTable) || canPushdownMetadataFilter(tableSourceTable);
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
            TableSourceTable tableSourceTable) {

        RelBuilder relBuilder = call.builder();
        Tuple2<RexNode[], RexNode[]> extractedPredicates =
                FlinkRexUtil.extractPredicates(
                        filter.getInput().getRowType().getFieldNames().toArray(new String[0]),
                        filter.getCondition(),
                        scan,
                        relBuilder.getRexBuilder());

        RexNode[] convertiblePredicates = extractedPredicates._1;
        RexNode[] unconvertedPredicates = extractedPredicates._2;
        if (convertiblePredicates.length == 0) {
            return;
        }

        boolean supportsPhysicalFilter = canPushdownFilter(tableSourceTable);
        boolean supportsMetadataFilter = canPushdownMetadataFilter(tableSourceTable);
        int physicalColumnCount = getPhysicalColumnCount(tableSourceTable);

        List<RexNode> allRemainingRexNodes = new ArrayList<>();
        TableSourceTable currentTable = tableSourceTable;

        // Unpushable metadata predicates stay as a runtime Calc, not the physical path —
        // physical routing produces a FilterPushDownSpec that crashes compiled-plan restore
        // once ProjectPushDownSpec narrows the scan row type.
        List<RexNode> physicalPredicates = new ArrayList<>();
        List<RexNode> metadataPredicates = new ArrayList<>();
        for (RexNode predicate : convertiblePredicates) {
            if (referencesOnlyMetadataColumns(predicate, physicalColumnCount)) {
                if (supportsMetadataFilter) {
                    metadataPredicates.add(predicate);
                } else {
                    allRemainingRexNodes.add(predicate);
                }
            } else {
                physicalPredicates.add(predicate);
            }
        }

        // Avoid re-firing on shapes we can't transform — saves wasted Hep iterations.
        boolean nothingToPushPhysically = physicalPredicates.isEmpty() || !supportsPhysicalFilter;
        if (nothingToPushPhysically && metadataPredicates.isEmpty()) {
            return;
        }

        if (!physicalPredicates.isEmpty() && supportsPhysicalFilter) {
            Tuple2<SupportsFilterPushDown.Result, TableSourceTable> physicalResult =
                    resolveFiltersAndCreateTableSourceTable(
                            physicalPredicates.toArray(new RexNode[0]),
                            currentTable,
                            scan,
                            relBuilder);
            currentTable = physicalResult._2;
            List<RexNode> physicalRemaining =
                    convertExpressionToRexNode(physicalResult._1.getRemainingFilters(), relBuilder);
            allRemainingRexNodes.addAll(physicalRemaining);
        } else {
            allRemainingRexNodes.addAll(physicalPredicates);
        }

        if (!metadataPredicates.isEmpty()) {
            MetadataPushDownOutcome metadataResult =
                    resolveMetadataFiltersAndCreateTableSourceTable(
                            metadataPredicates.toArray(new RexNode[0]), currentTable, scan);
            currentTable = metadataResult.newTableSourceTable;
            allRemainingRexNodes.addAll(metadataResult.remainingInputRexNodes);
        }

        allRemainingRexNodes.addAll(Arrays.asList(unconvertedPredicates));

        LogicalTableScan newScan =
                LogicalTableScan.create(scan.getCluster(), currentTable, scan.getHints());

        if (allRemainingRexNodes.isEmpty()) {
            call.transformTo(newScan);
        } else {
            RexNode remainingCondition = relBuilder.and(allRemainingRexNodes);
            RexNode simplifiedRemainingCondition =
                    FlinkRexUtil.simplify(
                            relBuilder.getRexBuilder(),
                            remainingCondition,
                            filter.getCluster().getPlanner().getExecutor());
            Filter newFilter =
                    filter.copy(filter.getTraitSet(), newScan, simplifiedRemainingCondition);
            call.transformTo(newFilter);
        }
    }
}
