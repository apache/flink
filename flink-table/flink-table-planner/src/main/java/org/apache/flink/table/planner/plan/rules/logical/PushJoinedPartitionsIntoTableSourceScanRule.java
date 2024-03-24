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

import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsPartitionPushDown;
import org.apache.flink.table.planner.plan.abilities.source.PartitionPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilityContext;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import scala.Tuple2;

/**
 * Planner rule that checks inner equality join tables' partitions, calculates their intersection,
 * pushes the resulting intersection to both tables into a {@link LogicalTableScan}.
 */
public class PushJoinedPartitionsIntoTableSourceScanRule extends RelOptRule {
    public static final PushJoinedPartitionsIntoTableSourceScanRule INSTANCE =
            new PushJoinedPartitionsIntoTableSourceScanRule();

    public PushJoinedPartitionsIntoTableSourceScanRule() {
        super(operand(LogicalJoin.class, any()), "PushJoinedPartitionsIntoTableSourceScanRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        Optional<List<String>> leftPartitionColumns = derivePartitionedColumnNames(join.getLeft());
        if (!leftPartitionColumns.isPresent()) {
            return false;
        }
        Optional<List<String>> rightPartitionColumns =
                derivePartitionedColumnNames(join.getRight());
        if (!rightPartitionColumns.isPresent()) {
            return false;
        }
        JoinInfo joinInfo = join.analyzeCondition();
        Optional<List<String>> leftJoinFields =
                deriveJoinedColumnNames(join.getLeft(), joinInfo.leftKeys.toIntegerList());
        if (!leftJoinFields.isPresent()) {
            return false;
        }
        Optional<List<String>> rightJoinFields =
                deriveJoinedColumnNames(join.getRight(), joinInfo.rightKeys.toIntegerList());
        if (!rightJoinFields.isPresent()) {
            return false;
        }
        boolean res1 = partitioningMatches(leftPartitionColumns.get(), leftJoinFields.get());
        boolean res2 = partitioningMatches(rightPartitionColumns.get(), rightJoinFields.get());
        return res1 && res2;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);

        Optional<TableScan> maybeLeftTableScan = deriveSource(join.getLeft());
        if (!maybeLeftTableScan.isPresent()) {
            return;
        }
        Optional<TableScan> maybeRightTableScan = deriveSource(join.getRight());
        if (!maybeRightTableScan.isPresent()) {
            return;
        }

        TableSourceTable tableSourceTableLeft =
                maybeLeftTableScan.get().getTable().unwrap(TableSourceTable.class);
        TableSourceTable tableSourceTableRight =
                maybeRightTableScan.get().getTable().unwrap(TableSourceTable.class);
        if (tableSourceTableLeft == null || tableSourceTableRight == null) {
            return;
        }
        Optional<List<Map<String, String>>> leftPartitions = getPartitions(tableSourceTableLeft);
        if (!leftPartitions.isPresent()) {
            return;
        }

        Optional<List<Map<String, String>>> rightPartitions = getPartitions(tableSourceTableRight);
        if (!rightPartitions.isPresent()) {
            return;
        }

        Tuple2<List<Map<String, String>>, List<Map<String, String>>> joinedPartitions =
                joinPartitions(leftPartitions.get(), rightPartitions.get());
        TableScan newLeftScan =
                applyPushdown(
                        tableSourceTableLeft, maybeLeftTableScan.get(), joinedPartitions._1());
        TableScan newRightScan =
                applyPushdown(
                        tableSourceTableRight, maybeRightTableScan.get(), joinedPartitions._2());
        Optional<RelNode> newLeft =
                replaceSource(join.getLeft(), maybeLeftTableScan.get(), newLeftScan);
        Optional<RelNode> newRight =
                replaceSource(join.getRight(), maybeRightTableScan.get(), newRightScan);
        if (!newLeft.isPresent() || !newRight.isPresent()) {
            return;
        }

        LogicalJoin newJoin =
                join.copy(
                        join.getTraitSet(),
                        join.getCondition(),
                        newLeft.get(),
                        newRight.get(),
                        join.getJoinType(),
                        join.isSemiJoinDone());
        call.transformTo(newJoin);
    }

    /**
     * Given join column indexes, derives the names of source columns that are used for join.
     *
     * @param node The top-level relational node from which the search starts.
     * @param columns List of column indexes.
     * @return List of column names used for joining wrapped with {@code Optional}. If any
     *     relational node is encountered other than @{code HepRelVertex}, @{code Filter}, @{code
     *     Project} return @{Optional.empty()}.
     */
    private Optional<List<String>> deriveJoinedColumnNames(RelNode node, List<Integer> columns) {
        if (node instanceof HepRelVertex) {
            return deriveJoinedColumnNames(((HepRelVertex) node).getCurrentRel(), columns);
        } else if (node instanceof Filter) {
            return deriveJoinedColumnNames(((Filter) node).getInput(), columns);
        } else if (node instanceof Project) {
            Project projectNode = (Project) node;
            List<RexNode> origProjects = projectNode.getProjects();
            List<RexNode> projects =
                    columns.stream().map(origProjects::get).collect(Collectors.toList());
            // make sure that the target projection columns are not modified
            boolean allColRefs = projects.stream().allMatch(p -> p instanceof RexInputRef);
            if (!allColRefs) {
                return Optional.empty();
            }
            List<Integer> newColumns =
                    projects.stream()
                            .map(p -> ((RexInputRef) p).getIndex())
                            .collect(Collectors.toList());
            return deriveJoinedColumnNames(((Project) node).getInput(), newColumns);
        } else if (node instanceof TableScan) {
            List<String> allFieldNames =
                    node.getRowType().getFieldList().stream()
                            .map(RelDataTypeField::getName)
                            .collect(Collectors.toList());

            return Optional.of(
                    columns.stream().map(allFieldNames::get).collect(Collectors.toList()));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Derives names of partitioned columns.
     *
     * @param topNode The top-level relational node from which the search starts.
     * @return {@code Optional.empty()} if the resulting node is not of type {@code
     *     TableSourceTable} or {@code TableSourceTable} or is not partitioned. Otherwise, return
     *     list of partitioned columns with {@code Optional} wrapper.
     */
    private Optional<List<String>> derivePartitionedColumnNames(RelNode topNode) {
        Optional<TableScan> maybeTable = deriveSource(topNode);
        if (!maybeTable.isPresent()) {
            return Optional.empty();
        }
        TableScan tableScan = maybeTable.get();

        TableSourceTable tableSourceTable = tableScan.getTable().unwrap(TableSourceTable.class);
        if (tableSourceTable == null) {
            return Optional.empty();
        }

        DynamicTableSource dynamicTableSource = tableSourceTable.tableSource();

        if (!(dynamicTableSource instanceof SupportsPartitionPushDown)) {
            return Optional.empty();
        }
        CatalogTable catalogTable = tableSourceTable.contextResolvedTable().getResolvedTable();

        if (!catalogTable.isPartitioned()) {
            return Optional.empty();
        }

        return Optional.ofNullable(catalogTable.getPartitionKeys());
    }

    /**
     * Derives source relational node of type {@code TableScan} by skipping nodes of types {@code
     * HepRelVertex}, type {@code Filter}, and {@code Project}.
     *
     * @param node The top-level relational node from which the search starts.
     * @return Optional.empty() if {@code TableScan} node cannot be reached by skipping the
     *     specified nodes. Otherwise, return found {@code TableScan} with {@code Optional} wrapper.
     */
    private Optional<TableScan> deriveSource(RelNode node) {
        if (node instanceof HepRelVertex) {
            return deriveSource(((HepRelVertex) node).getCurrentRel());
        } else if ((node instanceof Filter) || (node instanceof Project)) {
            return deriveSource(node.getInput(0));
        } else if (node instanceof TableScan) {
            return Optional.of((TableScan) node);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Derives partitions of the given {@code TableSourceTable} relational node. If the {@code
     * TableSourceTable} node already contains {@code PartitionPushDownSpec} spec, retrieve already
     * pushed partitions from this interface.
     *
     * @param table Input table.
     * @return {@code Optional.empty()} if the input is not an instance of {@code
     *     SupportsPartitionPushDown}. Otherwise, return found partitions with {@code Optional}
     *     wrapper.
     */
    private Optional<List<Map<String, String>>> getPartitions(TableSourceTable table) {
        SourceAbilitySpec[] sourceAbilitySpecs = table.abilitySpecs();
        for (SourceAbilitySpec sourceAbilitySpec : sourceAbilitySpecs) {
            if (sourceAbilitySpec instanceof PartitionPushDownSpec) {
                return Optional.of(((PartitionPushDownSpec) sourceAbilitySpec).getPartitions());
            }
        }
        DynamicTableSource dynamicTableSource = table.tableSource();

        if (!(dynamicTableSource instanceof SupportsPartitionPushDown)) {
            return Optional.empty();
        }

        return ((SupportsPartitionPushDown) dynamicTableSource).listPartitions();
    }

    /**
     * Given partitioned fields and joined fields, checks if join fields can be pushes down to the
     * source. Joined fields must be subset of the partition fields. For example, if a table is
     * partitioned w.r.t. fields (a,b,c) and joined w.r.t. (b,a) (e.g., t1.b = t2.x and t1.a =
     * t2.y), then, joined fields (a,b) must be the prefix-subset of partitioned fields (a,b,c).
     * Also, the prefix-subset order does not matter. For example, join fields (b,a) or (a,b) can be
     * pushed down to the table partitioned w.r.t. (a,b,c).
     *
     * @param partitionedFields List of partitioned fields.
     * @param joinFields List of joined fields.
     * @return if joined fields can be pushed down to the table with the specified partition fields.
     */
    private boolean partitioningMatches(List<String> partitionedFields, List<String> joinFields) {
        if (partitionedFields.size() < joinFields.size()) {
            return false;
        }

        List<String> partitionFieldsSublist = partitionedFields.subList(0, joinFields.size());
        Collections.sort(partitionFieldsSublist);
        Collections.sort(joinFields);

        for (int i = 0; i < partitionFieldsSublist.size(); i++) {
            if (!partitionFieldsSublist.get(i).equals(joinFields.get(i))) {
                return false; // Return false if elements at corresponding indices are not equal
            }
        }

        return true; // All elements are equal
    }

    /**
     * Finds the old {@code TableScan} and replaces it with the new one.
     *
     * @param node Top relational node from which the search starts.
     * @param originalScan Original {@code TableScan} node.
     * @param newScan New {@code TableScan} node.
     * @return the relational node with replaced {@code TableScan} and wrapped with {@code Optional}
     *     if the replacement succeeds.Otherwise, return {@code Optional.empty()}.
     */
    private Optional<RelNode> replaceSource(
            RelNode node, TableScan originalScan, TableScan newScan) {
        if (node instanceof HepRelVertex) {
            return replaceSource(((HepRelVertex) node).getCurrentRel(), originalScan, newScan);
        } else if ((node instanceof Filter) || (node instanceof Project)) {
            Optional<RelNode> relNode = replaceSource(node.getInput(0), originalScan, newScan);
            return Optional.ofNullable(
                    node.copy(node.getTraitSet(), Collections.singletonList(relNode.orElse(null))));
        } else if ((node instanceof TableScan) && node.equals(originalScan)) {
            return Optional.of(newScan);
        } else {
            return Optional.empty();
        }
    }

    /** Applies pushdown to the source table. */
    private TableScan applyPushdown(
            TableSourceTable tableSourceTable,
            TableScan scan,
            List<Map<String, String>> joinedPartitions) {
        DynamicTableSource dynamicTableSource = tableSourceTable.tableSource().copy();
        PartitionPushDownSpec partitionPushDownSpec = new PartitionPushDownSpec(joinedPartitions);
        partitionPushDownSpec.apply(dynamicTableSource, SourceAbilityContext.from(scan));

        TableSourceTable newTableSourceTable =
                tableSourceTable.copy(
                        dynamicTableSource,
                        // The statistics will be updated in FlinkRecomputeStatisticsProgram.
                        tableSourceTable.getStatistic(),
                        new SourceAbilitySpec[] {partitionPushDownSpec});
        LogicalTableScan newScan =
                LogicalTableScan.create(scan.getCluster(), newTableSourceTable, scan.getHints());
        return newScan;
    }

    /**
     * Finds the join (intersection) of two partitions. First, smaller partition is selected as hash
     * and bigger one as probe. Note that, here we join w.r.t. the values of the {@code Map}. For
     * example, {@code M1 = {a=1, b=2}} and {@code M2 = {c=1, d=2}} are considered to be
     * matched/intersected/joined.
     *
     * @param p1 First partition to be joined.
     * @param p2 Second partition to be joined.
     * @return the resulting (joined/intersected) partitions for {@code p1} and {@code p2} in {@code
     *     Tuple2}.
     */
    private Tuple2<List<Map<String, String>>, List<Map<String, String>>> joinPartitions(
            List<Map<String, String>> p1, List<Map<String, String>> p2) {
        List<Map<String, String>> hashPartition;
        List<Map<String, String>> probePartition;
        List<Map<String, String>> leftResult = new ArrayList<>();
        List<Map<String, String>> rightResult = new ArrayList<>();
        Tuple2<List<Map<String, String>>, List<Map<String, String>>> result;
        if (p1.size() > p2.size()) {
            hashPartition = p2;
            probePartition = p1;
            result = new Tuple2<>(leftResult, rightResult);
        } else {
            hashPartition = p1;
            probePartition = p2;
            result = new Tuple2<>(rightResult, leftResult);
        }

        Map<List<String>, Map<String, String>> hash = new LinkedHashMap<>();
        for (Map<String, String> p : hashPartition) {
            hash.put(new ArrayList<>(p.values()), p);
        }

        for (Map<String, String> p : probePartition) {
            Map<String, String> val = hash.get(new ArrayList<>(p.values()));
            if (val != null) {
                leftResult.add(p);
                rightResult.add(val);
            }
        }
        return result;
    }
}
