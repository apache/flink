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

package org.apache.flink.table.planner.plan.reuse;

import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.planner.calcite.FlinkContext;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.connectors.DynamicSourceUtils;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.plan.abilities.source.FilterPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilityContext;
import org.apache.flink.table.planner.plan.abilities.source.ProjectPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.ReadingMetadataSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.abilities.source.WatermarkPushDownSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.rules.logical.PushProjectIntoTableSourceScanRule;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.abilitySpecsWithoutEscaped;
import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.concatProjectedFields;
import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.createCalcForScan;
import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.enforceMetadataKeyOrder;
import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.getAbilitySpec;
import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.getAdjustedWatermarkSpec;
import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.indexOf;
import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.metadataKeys;
import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.pickScanWithWatermark;
import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.projectedFields;
import static org.apache.flink.table.planner.plan.reuse.ScanReuserUtils.reusableWithoutAdjust;

/**
 * Reuse sources.
 *
 * <p>When there are projection and metadata push down, the generated source cannot be reused
 * because of the difference of digest. To make source reusable, this class does the following:
 *
 * <ul>
 *   <li>First, find the same source, regardless of their projection and metadata push down.
 *   <li>Union projections for different instances of the same source and create a new instance.
 *   <li>Generate different Calc nodes for different instances.
 *   <li>Replace instances.
 * </ul>
 *
 * <p>For example, plan:
 *
 * <pre>{@code
 * Calc(select=[a, b, c])
 * +- Join(joinType=[InnerJoin], where=[(a = a0)], select=[a, b, a0, c])
 *    :- Exchange(distribution=[hash[a]])
 *    :  +- TableSourceScan(table=[[MyTable, project=[a, b]]], fields=[a, b])
 *    +- Exchange(distribution=[hash[a]])
 *    :  +- TableSourceScan(table=[[MyTable, project=[a, c]]], fields=[a, c])
 * }</pre>
 *
 * <p>Unified to:
 *
 * <pre>{@code
 * Calc(select=[a, b, c])
 * +- Join(joinType=[InnerJoin], where=[(a = a0)], select=[a, b, a0, c])
 *    :- Exchange(distribution=[hash[a]])
 *    :  +- Calc(select=[a, b])
 *    :     +- TableSourceScan(table=[[MyTable, project=[a, b, c]]], fields=[a, b, c])
 *    +- Exchange(distribution=[hash[a]])
 *       +- Calc(select=[a, c])
 *    :     +- TableSourceScan(table=[[MyTable, project=[a, b, c]]], fields=[a, b, c])
 * }</pre>
 *
 * <p>When {@code filterReuseEnabled} is set, scans with different pushed-down filters can also be
 * merged. The per-scan filters are OR'd into a combined predicate. Before committing to the merge,
 * the OR'd predicate is tested against a copy of the source via {@link
 * SupportsFilterPushDown#applyFilters}. If the connector accepts it, the merged source pushes the
 * OR'd filter and each consumer's Calc applies its original filter. If the connector rejects it,
 * the group is skipped and scans remain separate — no full-table read occurs.
 *
 * <p>This class do not reuse all sources, sources with same digest will be reused by {@link
 * SubplanReuser}.
 *
 * <p>NOTE: This class not optimize expressions like "$0.child" and "$0", keep both. But {@link
 * PushProjectIntoTableSourceScanRule} will reduce them to only one projection "$0". This is because
 * the subsequent rewrite of watermark push down will become very troublesome. Not only need to
 * adjust the index, but also generate the getter of the nested field. So, connector must deal with
 * "$0.child" and "$0" projection.
 */
public class ScanReuser {

    private static final Comparator<int[]> INT_ARRAY_COMPARATOR =
            (v1, v2) -> {
                int lim = Math.min(v1.length, v2.length);
                int k = 0;
                while (k < lim) {
                    if (v1[k] != v2[k]) {
                        return v1[k] - v2[k];
                    }
                    k++;
                }
                return v1.length - v2.length;
            };

    private final Map<CommonPhysicalTableSourceScan, RelNode> replaceMap = new HashMap<>();

    private final FlinkContext flinkContext;
    private final FlinkTypeFactory flinkTypeFactory;
    private final boolean filterReuseEnabled;

    public ScanReuser(FlinkContext flinkContext, FlinkTypeFactory flinkTypeFactory) {
        this(flinkContext, flinkTypeFactory, false);
    }

    public ScanReuser(
            FlinkContext flinkContext,
            FlinkTypeFactory flinkTypeFactory,
            boolean filterReuseEnabled) {
        this.flinkContext = flinkContext;
        this.flinkTypeFactory = flinkTypeFactory;
        this.filterReuseEnabled = filterReuseEnabled;
    }

    public List<RelNode> reuseDuplicatedScan(List<RelNode> relNodes) {
        // When filterReuseEnabled, escape filters in digest so scans with different
        // filters are grouped together. Per-group, we check SupportsFilteredSourceReuse
        // to decide whether to OR filters. If the source doesn't support it, we skip
        // groups where filters differ (same-filter groups proceed normally for projection merge).
        ReusableScanVisitor visitor = new ReusableScanVisitor(filterReuseEnabled);
        relNodes.forEach(visitor::go);

        for (List<CommonPhysicalTableSourceScan> reusableNodes :
                visitor.digestToReusableScans().values()) {
            if (reusableNodes.size() < 2 || reusableWithoutAdjust(reusableNodes)) {
                continue;
            }

            if (reusableNodes.stream()
                    .anyMatch(ScanReuserUtils::containsRexNodeSpecAfterProjection)) {
                continue;
            }

            // Determine if we should attempt to merge different filters (OR them).
            // Requires: config enabled + source supports filter pushdown.
            DynamicTableSource tableSource =
                    reusableNodes.get(0).tableSourceTable().tableSource();
            boolean mergeFilters =
                    filterReuseEnabled && tableSource instanceof SupportsFilterPushDown;

            CommonPhysicalTableSourceScan pickScan = pickScanWithWatermark(reusableNodes);
            TableSourceTable pickTable = pickScan.tableSourceTable();
            RexBuilder rexBuilder = pickScan.getCluster().getRexBuilder();

            // 1. Find union fields.
            // Input scan schema: physical projection fields + metadata fields.
            // (See DynamicSourceUtils.validateAndApplyMetadata)
            // So It is safe to collect physical projection fields + metadata fields.
            TreeSet<int[]> allProjectFieldSet = new TreeSet<>(INT_ARRAY_COMPARATOR);
            Set<String> allMetaKeySet = new HashSet<>();
            for (CommonPhysicalTableSourceScan scan : reusableNodes) {
                TableSourceTable source = scan.tableSourceTable();
                allProjectFieldSet.addAll(Arrays.asList(projectedFields(source)));
                allMetaKeySet.addAll(metadataKeys(source));
            }

            // 1.1 When merging filters, add filter-referenced columns to projection so Calc can filter.
            if (mergeFilters) {
                collectFilterReferencedColumns(reusableNodes, allProjectFieldSet);
            }

            int[][] allProjectFields = allProjectFieldSet.toArray(new int[0][]);
            List<String> allMetaKeys =
                    enforceMetadataKeyOrder(allMetaKeySet, pickTable.tableSource());

            // 2. Create new source.
            List<SourceAbilitySpec> specs =
                    abilitySpecsWithoutEscaped(pickTable, mergeFilters);

            // 2.1 Create produced type.
            // The source produced type is the input type into the runtime. The format looks as:
            // PHYSICAL COLUMNS + METADATA COLUMNS. While re-compute the source ability specs with
            // source metadata, we need to distinguish between schema type and produced type, which
            // source ability specs use produced type instead of schema type.
            RowType originType =
                    DynamicSourceUtils.createProducedType(
                            pickTable.contextResolvedTable().getResolvedSchema(),
                            pickTable.tableSource());

            // 2.2 Apply projections and metadata
            List<SourceAbilitySpec> newSpecs = new ArrayList<>();
            RowType newSourceType =
                    applyPhysicalAndMetadataPushDown(
                            pickTable.tableSource(),
                            originType,
                            newSpecs,
                            concatProjectedFields(
                                    pickTable.contextResolvedTable().getResolvedSchema(),
                                    originType,
                                    allProjectFields,
                                    allMetaKeys),
                            allProjectFields,
                            allMetaKeys);
            specs.addAll(newSpecs);

            // 2.3 Watermark spec
            Optional<WatermarkPushDownSpec> watermarkSpec =
                    getAdjustedWatermarkSpec(pickTable, originType, newSourceType);
            if (watermarkSpec.isPresent()) {
                specs.add(watermarkSpec.get());
                newSourceType = watermarkSpec.get().getProducedType().get();
            }

            // 2.4 OR all per-scan filters into combined predicate for the source.
            if (mergeFilters) {
                buildOrFilterSpec(reusableNodes, newSourceType, rexBuilder)
                        .ifPresent(specs::add);
            }

            // 2.5 Create a new ScanTableSource. ScanTableSource can not be pushed down twice.
            DynamicTableSourceSpec tableSourceSpec =
                    new DynamicTableSourceSpec(pickTable.contextResolvedTable(), specs);
            ScanTableSource newTableSource =
                    tableSourceSpec.getScanTableSource(flinkContext, flinkTypeFactory);

            TableSourceTable newSourceTable =
                    pickTable.replace(
                            newTableSource,
                            ((FlinkTypeFactory) rexBuilder.getTypeFactory())
                                    .buildRelNodeRowType(newSourceType),
                            specs.toArray(new SourceAbilitySpec[0]));

            RelNode newScan = pickScan.copy(newSourceTable);

            // 3. Create per-consumer Calc nodes.
            for (CommonPhysicalTableSourceScan scan : reusableNodes) {
                TableSourceTable source = scan.tableSourceTable();
                int[][] projectedFields = projectedFields(source);
                List<String> metaKeys = metadataKeys(source);

                // check to see if we have any merged filters we need to create Calc nodes for
                FilterPushDownSpec filterSpec =
                        getAbilitySpec(source.abilitySpecs(), FilterPushDownSpec.class);
                boolean hasFilter =
                        mergeFilters
                                && filterSpec != null
                                && !filterSpec.getPredicates().isEmpty();

                // Don't need add calc
                if (Arrays.deepEquals(projectedFields, allProjectFields)
                        && metaKeys.equals(allMetaKeys)
                        && !hasFilter) {
                    // full project may be pushed into source, update to the new source
                    replaceMap.put(scan, newScan);
                    continue;
                }

                RexProgramBuilder builder = new RexProgramBuilder(newScan.getRowType(), rexBuilder);

                for (int[] field : projectedFields) {
                    int index = indexOf(allProjectFields, field);
                    builder.addProject(index, newScan.getRowType().getFieldNames().get(index));
                }

                for (String key : metaKeys) {
                    int index = allProjectFields.length + allMetaKeys.indexOf(key);
                    builder.addProject(index, newScan.getRowType().getFieldNames().get(index));
                }

                // Add original filter as Calc condition with remapped indices
                if (hasFilter) {
                    RexShuttle remap = createFieldNameRemap(
                            physicalFieldNames(source), newScan.getRowType().getFieldNames());
                    RexNode condition = andPredicates(filterSpec.getPredicates(), remap, rexBuilder);
                    builder.addCondition(condition);
                }

                replaceMap.put(scan, createCalcForScan(newScan, builder.getProgram()));
            }
        }

        ReplaceScanWithCalcShuttle replaceShuttle = new ReplaceScanWithCalcShuttle(replaceMap);
        return relNodes.stream()
                .map(rel -> rel.accept(replaceShuttle))
                .collect(Collectors.toList());
    }

    /** Add all columns referenced by the filters to the projection set. */
    private static void collectFilterReferencedColumns(
            List<CommonPhysicalTableSourceScan> scans,
            TreeSet<int[]> allProjectFieldSet) {
        for (CommonPhysicalTableSourceScan scan : scans) {
            FilterPushDownSpec fs =
                    getAbilitySpec(scan.tableSourceTable().abilitySpecs(), FilterPushDownSpec.class);
            if (fs == null) continue;
            for (RexNode pred : fs.getPredicates()) {
                for (RexInputRef ref : FlinkRexUtil.findAllInputRefs(pred)) {
                    allProjectFieldSet.add(new int[] {ref.getIndex()});
                }
            }
        }
    }

    /**
     * OR all table scan filter into a combined FilterPushDownSpec for the unified source. Before
     * returning, verifies the connector accepts the OR'd predicate by calling applyFilters on a
     * throwaway copy. Returns empty if any scan has no filter or the connector rejects the OR.
     * TODO, still on the fence if this should return empty or throw exception indicating source
     * incompatible with this source reuse
     */
    // todo remove extra comments once finalized approach
    private Optional<FilterPushDownSpec> buildOrFilterSpec(
            List<CommonPhysicalTableSourceScan> scans,
            RowType newSourceType,
            RexBuilder rexBuilder) {
        List<RexNode> perScanFilters = new ArrayList<>();
        // Get on Node per table source scan
        for (CommonPhysicalTableSourceScan scan : scans) {
            FilterPushDownSpec fs =
                    getAbilitySpec(scan.tableSourceTable().abilitySpecs(), FilterPushDownSpec.class);
            if (fs == null || fs.getPredicates().isEmpty()) {
                // OR with empty filter -> no filter
                return Optional.empty();
            }
            RexShuttle remap = createFieldNameRemap(
                    physicalFieldNames(scan.tableSourceTable()), newSourceType.getFieldNames());
            perScanFilters.add(andPredicates(fs.getPredicates(), remap, rexBuilder));
        }
        // todo figure out when this could happen
        if (perScanFilters.isEmpty()) {
            return Optional.empty();
        }

        // more efficient way to write this
        RexNode combined = perScanFilters.get(0);
        for (int i = 1; i < perScanFilters.size(); i++) {
            combined = rexBuilder.makeCall(SqlStdOperatorTable.OR, combined, perScanFilters.get(i));
        }

        if (!connectorAcceptsFilter(scans.get(0).tableSourceTable().tableSource(), combined, newSourceType)) {
            // todo decide if exception thrown here instead of no filter being passed
            // could be dangerous passing no filter
            return Optional.empty();
        }

        return Optional.of(new FilterPushDownSpec(List.of(combined), false));
    }

    /**
     * Test whether the connector accepts a filter predicate by calling applyFilters on a
     * throwaway copy. No calls to actual data-source is used.
     */
    private boolean connectorAcceptsFilter(
            DynamicTableSource source, RexNode filter, RowType sourceType) {
        DynamicTableSource copy = source.copy();
        SourceAbilityContext context =
                new SourceAbilityContext(flinkContext, flinkTypeFactory, sourceType);
        // todo triple check the apply here is proper way to do this check is valid for ORs
        SupportsFilterPushDown.Result result =
                FilterPushDownSpec.apply(List.of(filter), copy, context);
        return !result.getAcceptedFilters().isEmpty();
    }

    /** Create a RexShuttle that remaps RexInputRef indices by matching field names. */
    private static RexShuttle createFieldNameRemap(
            List<String> oldNames, List<String> newNames) {
        return new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                String fieldName = oldNames.get(ref.getIndex());
                int newIndex = newNames.indexOf(fieldName);
                if (newIndex < 0) {
                    throw new org.apache.flink.table.api.TableException(
                            String.format(
                                    "Field '%s' not found in target type %s during filter remap.",
                                    fieldName, newNames));
                }
                return new RexInputRef(newIndex, ref.getType());
            }
        };
    }

    /** Get physical column field names from a TableSourceTable. */
    // needed because want to remap indicies from filter predicates to our unified scan
    private static List<String> physicalFieldNames(TableSourceTable source) {
        return ((RowType) source.contextResolvedTable().getResolvedSchema()
                .toPhysicalRowDataType().getLogicalType()).getFieldNames();
    }

    /** AND a list of predicates together, applying a remap shuttle to each. */
    // used for or'ing all our statements together
    private static RexNode andPredicates(
            List<RexNode> predicates, RexShuttle remap, RexBuilder rexBuilder) {
        RexNode result = predicates.get(0).accept(remap);
        for (int i = 1; i < predicates.size(); i++) {
            result = rexBuilder.makeCall(SqlStdOperatorTable.AND, result, predicates.get(i).accept(remap));
        }
        return result;
    }

    /**
     * Generate sourceAbilitySpecs and newProducedType by projected physical fields and metadata
     * keys.
     */
    private static RowType applyPhysicalAndMetadataPushDown(
            DynamicTableSource source,
            RowType originType,
            List<SourceAbilitySpec> sourceAbilitySpecs,
            int[][] physicalAndMetaFields,
            int[][] projectedPhysicalFields,
            List<String> usedMetadataNames) {
        RowType newProducedType = originType;
        boolean supportsProjectPushDown = source instanceof SupportsProjectionPushDown;
        boolean supportsReadingMeta = source instanceof SupportsReadingMetadata;
        if (supportsProjectPushDown || supportsReadingMeta) {
            newProducedType = (RowType) Projection.of(physicalAndMetaFields).project(originType);
        }
        if (supportsProjectPushDown) {
            sourceAbilitySpecs.add(
                    new ProjectPushDownSpec(projectedPhysicalFields, newProducedType));
        }
        if (supportsReadingMeta) {
            sourceAbilitySpecs.add(new ReadingMetadataSpec(usedMetadataNames, newProducedType));
        }
        return newProducedType;
    }
}
