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

import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsPartitioning;
import org.apache.flink.table.planner.plan.abilities.source.PartitioningSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilityContext;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.calcite.linq4j.tree.Expressions.visitChildren;

/**
 * Planner rule that removes BatchPhysicalExchange operator if the source is already partitioned
 * w.r.t. the partitioning keys.
 *
 * <p>Suppose we have the original physical plan:
 *
 * <pre>{@code
 * BatchPhysicalHashAggregate (global)
 * +- BatchPhysicalExchange (hash by group keys if group keys is not empty, else singleton)
 *    +- BatchPhysicalLocalHashAggregate (local)
 *       +- BatchPhysicalTableSourceScan
 * }</pre>
 *
 * <p>This physical plan will be rewritten to:
 *
 * <pre>{@code
 * BatchPhysicalHashAggregate (local)
 *   +- BatchPhysicalLocalHashAggregate (local)
 *      +- BatchPhysicalTableSourceScan
 * }</pre>
 */
public class RemoveRedundantShuffleRule extends PushLocalAggIntoScanRuleBase {
    public static final RemoveRedundantShuffleRule INSTANCE = new RemoveRedundantShuffleRule();

    public RemoveRedundantShuffleRule() {
        super(
                operand(SingleRel.class, operand(BatchPhysicalExchange.class, any())),
                "RemoveRedundantShuffleRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        BatchPhysicalExchange exchange = call.rel(1);
        TableSourceFinder finder = new TableSourceFinder();
        exchange.getInput().accept(finder);
        Optional<BatchPhysicalTableSourceScan> scanOptional = finder.getScan();
        if (scanOptional.isPresent()) {
            BatchPhysicalTableSourceScan scanOp = scanOptional.get();
            final TableSourceTable sourceTable = scanOp.getTable().unwrap(TableSourceTable.class);
            if (sourceTable == null) {
                return false;
            }
            final DynamicTableSource source = sourceTable.tableSource();
            return supportsSourcePartitioning(source, derivePartitioningColumns(exchange));
        } else {
            return false;
        }
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        SingleRel topOp = call.rel(0);
        BatchPhysicalExchange exchange = call.rel(1);

        TableSourceFinder finder = new TableSourceFinder();
        exchange.getInput().accept(finder);
        Optional<BatchPhysicalTableSourceScan> scanOptional = finder.getScan();
        if (!(scanOptional.isPresent())) {
            return;
        }

        BatchPhysicalTableSourceScan scan = scanOptional.get();

        TableSourceTable newTableSourceTable = applyPartitioning(scan);
        BatchPhysicalTableSourceScan newScan =
                new BatchPhysicalTableSourceScan(
                        scan.getCluster(),
                        scan.getTraitSet(),
                        scan.getHints(),
                        newTableSourceTable);

        RelNode exhangeInput = exchange.getInput();
        RelNode newTop = topOp.copy(topOp.getTraitSet(), Collections.singletonList(exhangeInput));

        RelNode newTopWithNewScan = replaceScan(scan, newScan, newTop);

        call.transformTo(newTopWithNewScan);
    }

    private RelNode replaceScan(
            BatchPhysicalTableSourceScan oldScan,
            BatchPhysicalTableSourceScan newScan,
            RelNode node) {
        RelNode curNode = getNode(node);
        for (int i = 0; i < curNode.getInputs().size(); ++i) {
            RelNode curChild = getNode(curNode.getInput(i));
            if (curChild.equals(oldScan)) {
                curNode.replaceInput(i, newScan);
            } else {
                replaceScan(oldScan, newScan, curChild);
            }
        }
        return curNode;
    }

    private TableSourceTable applyPartitioning(BatchPhysicalTableSourceScan scan) {
        TableSourceTable relOptTable = scan.getTable().unwrap(TableSourceTable.class);
        TableSourceTable oldTableSourceTable = relOptTable.unwrap(TableSourceTable.class);
        DynamicTableSource newTableSource = oldTableSourceTable.tableSource().copy();
        PartitioningSpec partitioningSpec = new PartitioningSpec();
        partitioningSpec.apply(newTableSource, SourceAbilityContext.from(scan));

        return oldTableSourceTable.copy(
                newTableSource,
                oldTableSourceTable.getRowType(),
                new SourceAbilitySpec[] {partitioningSpec});
    }

    private List<String> derivePartitioningColumns(BatchPhysicalExchange exchange) {
        List<Integer> partitionKeyIdx = exchange.getDistribution().getKeys();
        return exchange.getInput().getRowType().getFieldList().stream()
                .filter(f -> partitionKeyIdx.contains(f.getIndex()))
                .map(RelDataTypeField::getName)
                .collect(Collectors.toList());
    }

    private boolean supportsSourcePartitioning(
            DynamicTableSource tableSource, List<String> actualPartitionColumns) {
        if (!(tableSource instanceof SupportsPartitioning)) {
            return false;
        }

        Optional<List<String>> maybeCurrentPartitionColumns =
                ((SupportsPartitioning<?>) tableSource).partitionKeys();
        if (!(maybeCurrentPartitionColumns.isPresent())
                || maybeCurrentPartitionColumns.get().isEmpty()) {
            return false;
        }
        List<String> currentPartitionColumns = maybeCurrentPartitionColumns.get();
        if (currentPartitionColumns.size() != actualPartitionColumns.size()) {
            return false;
        }

        for (int i = 0; i < currentPartitionColumns.size(); i++) {
            if (!currentPartitionColumns.get(i).equals(actualPartitionColumns.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static RelNode getNode(RelNode node) {
        return node instanceof HepRelVertex ? ((HepRelVertex) node).getCurrentRel() : node;
    }

    private static class TableSourceFinder extends RelShuttleImpl {
        private boolean anotherExchangeExists = false;
        private BatchPhysicalTableSourceScan tableSourceScan;

        // Override the visit method for SingleRel
        @Override
        public RelNode visit(RelNode node) {
            RelNode relNode = getNode(node);
            if ((relNode instanceof BatchPhysicalExchange) || anotherExchangeExists) {
                anotherExchangeExists = true;
                return node;
            }

            if (relNode instanceof BatchPhysicalTableSourceScan) {
                this.tableSourceScan = (BatchPhysicalTableSourceScan) relNode;
                return relNode;
            } else {
                return visitChildren(relNode);
            }
        }

        public Optional<BatchPhysicalTableSourceScan> getScan() {
            if (anotherExchangeExists || tableSourceScan == null) {
                return Optional.empty();
            } else {
                return Optional.of(tableSourceScan);
            }
        }
    }
}
