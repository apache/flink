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

package org.apache.flink.table.planner.plan.optimize.program;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsStatisticReport;
import org.apache.flink.table.plan.stats.TableStats;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.plan.abilities.source.FilterPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.PartitionPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.stats.FlinkStatistic;
import org.apache.flink.table.planner.plan.utils.DefaultRelShuttle;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.config.OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_REPORT_STATISTICS_ENABLED;
import static org.apache.flink.table.planner.utils.CatalogTableStatisticsConverter.convertToAccumulatedTableStates;

/**
 * A FlinkOptimizeProgram that recompute statistics after partition pruning and filter push down.
 *
 * <p>It's a very heavy operation to get statistics from catalogs or connectors, so this centralized
 * way can avoid getting statistics again and again.
 */
public class FlinkRecomputeStatisticsProgram implements FlinkOptimizeProgram<BatchOptimizeContext> {

    @Override
    public RelNode optimize(RelNode root, BatchOptimizeContext context) {
        DefaultRelShuttle shuttle =
                new DefaultRelShuttle() {
                    @Override
                    public RelNode visit(TableScan scan) {
                        if (scan instanceof LogicalTableScan) {
                            return recomputeStatistics((LogicalTableScan) scan);
                        }
                        return super.visit(scan);
                    }
                };
        return shuttle.visit(root);
    }

    private LogicalTableScan recomputeStatistics(LogicalTableScan scan) {
        final RelOptTable scanTable = scan.getTable();
        if (!(scanTable instanceof TableSourceTable)) {
            return scan;
        }

        FlinkContext context = ShortcutUtils.unwrapContext(scan);
        TableSourceTable table = (TableSourceTable) scanTable;
        boolean reportStatEnabled =
                context.getTableConfig().get(TABLE_OPTIMIZER_SOURCE_REPORT_STATISTICS_ENABLED)
                        && table.tableSource() instanceof SupportsStatisticReport;

        SourceAbilitySpec[] specs = table.abilitySpecs();
        PartitionPushDownSpec partitionPushDownSpec = getSpec(specs, PartitionPushDownSpec.class);

        FilterPushDownSpec filterPushDownSpec = getSpec(specs, FilterPushDownSpec.class);
        TableStats newTableStat =
                recomputeStatistics(
                        table, partitionPushDownSpec, filterPushDownSpec, reportStatEnabled);
        FlinkStatistic newStatistic =
                FlinkStatistic.builder()
                        .statistic(table.getStatistic())
                        .tableStats(newTableStat)
                        .build();
        TableSourceTable newTable = table.copy(newStatistic);
        return new LogicalTableScan(
                scan.getCluster(), scan.getTraitSet(), scan.getHints(), newTable);
    }

    private TableStats recomputeStatistics(
            TableSourceTable table,
            PartitionPushDownSpec partitionPushDownSpec,
            FilterPushDownSpec filterPushDownSpec,
            boolean reportStatEnabled) {
        TableStats origTableStats = table.getStatistic().getTableStats();
        DynamicTableSource tableSource = table.tableSource();
        if (filterPushDownSpec != null && !filterPushDownSpec.isAllPredicatesRetained()) {
            // filter push down but some predicates are accepted by source and not in reaming
            // predicates
            // the catalog do not support get statistics with filters,
            // so only call reportStatistics method if reportStatEnabled is true
            // TODO estimate statistics by selectivity
            return reportStatEnabled
                    ? ((SupportsStatisticReport) tableSource).reportStatistics()
                    : null;
        } else if (partitionPushDownSpec != null) {
            // ignore filter push down if all pushdown predicates are also in outer Filter operator
            // otherwise the result will be estimated twice.
            // partition push down
            // try to get the statistics for the remaining partitions
            TableStats newTableStat = getPartitionsTableStats(table, partitionPushDownSpec);
            // call reportStatistics method if reportStatEnabled is true and the partition
            // statistics is unknown
            if (reportStatEnabled && isUnknownTableStats(newTableStat)) {
                return ((SupportsStatisticReport) tableSource).reportStatistics();
            } else {
                return newTableStat;
            }
        } else {
            if (isPartitionedTable(table) && isUnknownTableStats(origTableStats)) {
                // if table is partition table, try to recompute stats by catalog.
                origTableStats = getPartitionsTableStats(table, null);
            }
            // call reportStatistics method if reportStatEnabled is true and the newTableStats is
            // unknown.
            if (reportStatEnabled && isUnknownTableStats(origTableStats)) {
                return ((SupportsStatisticReport) tableSource).reportStatistics();
            } else {
                return origTableStats;
            }
        }
    }

    private boolean isPartitionedTable(TableSourceTable table) {
        return table.contextResolvedTable()
                .<ResolvedCatalogTable>getResolvedTable()
                .isPartitioned();
    }

    private boolean isUnknownTableStats(TableStats stats) {
        return stats == null || stats.getRowCount() < 0 && stats.getColumnStats().isEmpty();
    }

    private TableStats getPartitionsTableStats(
            TableSourceTable table, @Nullable PartitionPushDownSpec partitionPushDownSpec) {
        if (table.contextResolvedTable().isPermanent()) {
            ObjectIdentifier identifier = table.contextResolvedTable().getIdentifier();
            ObjectPath tablePath = identifier.toObjectPath();
            Optional<Catalog> optionalCatalog = table.contextResolvedTable().getCatalog();
            if (!optionalCatalog.isPresent()) {
                return TableStats.UNKNOWN;
            }
            Catalog catalog = optionalCatalog.get();
            List<Map<String, String>> partitionList = new ArrayList<>();
            if (partitionPushDownSpec == null) {
                try {
                    List<CatalogPartitionSpec> catalogPartitionSpecs =
                            catalog.listPartitions(tablePath);
                    for (CatalogPartitionSpec partitionSpec : catalogPartitionSpecs) {
                        partitionList.add(partitionSpec.getPartitionSpec());
                    }
                } catch (TableNotExistException | TableNotPartitionedException e) {
                    throw new TableException("Table not exists!", e);
                }
            } else {
                partitionList = partitionPushDownSpec.getPartitions();
            }

            Optional<TableStats> optionalTableStats =
                    getPartitionStats(
                            catalog,
                            table.contextResolvedTable().getIdentifier().toObjectPath(),
                            partitionList);
            return optionalTableStats.orElse(TableStats.UNKNOWN);
        }

        return TableStats.UNKNOWN;
    }

    private Optional<TableStats> getPartitionStats(
            Catalog catalog, ObjectPath tablePath, List<Map<String, String>> partition) {
        try {

            final List<CatalogPartitionSpec> partitionSpecs =
                    partition.stream().map(CatalogPartitionSpec::new).collect(Collectors.toList());

            return Optional.of(
                    convertToAccumulatedTableStates(
                            catalog.bulkGetPartitionStatistics(tablePath, partitionSpecs),
                            catalog.bulkGetPartitionColumnStatistics(tablePath, partitionSpecs),
                            getPartitionKeys(partitionSpecs)));
        } catch (PartitionNotExistException e) {
            return Optional.empty();
        }
    }

    private static Set<String> getPartitionKeys(List<CatalogPartitionSpec> catalogPartitionSpecs) {
        Set<String> partitionKeys = new HashSet<>();
        for (CatalogPartitionSpec catalogPartitionSpec : catalogPartitionSpecs) {
            Map<String, String> partitionSpec = catalogPartitionSpec.getPartitionSpec();
            partitionKeys.addAll(partitionSpec.keySet());
        }
        return partitionKeys;
    }

    @SuppressWarnings({"unchecked", "raw"})
    private <T extends SourceAbilitySpec> T getSpec(SourceAbilitySpec[] specs, Class<T> specClass) {
        if (specs == null) {
            return null;
        }
        for (SourceAbilitySpec spec : specs) {
            if (spec.getClass().equals(specClass)) {
                return (T) spec;
            }
        }
        return null;
    }
}
