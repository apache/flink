/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata.MetadataFilterResult;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter;
import org.apache.flink.table.planner.plan.abilities.source.FilterPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.MetadataFilterPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.ReadingMetadataSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilityContext;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.reuse.ScanReuserUtils;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;

import org.apache.flink.shaded.guava33.com.google.common.collect.Iterables;
import org.apache.flink.shaded.guava33.com.google.common.collect.Sets;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import scala.Tuple2;

/** Base class for rules that push down filters into table scan. */
public abstract class PushFilterIntoSourceScanRuleBase extends RelOptRule {
    public PushFilterIntoSourceScanRuleBase(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    protected List<RexNode> convertExpressionToRexNode(
            List<ResolvedExpression> expressions, RelBuilder relBuilder) {
        ExpressionConverter exprConverter = new ExpressionConverter(relBuilder);
        return expressions.stream().map(e -> e.accept(exprConverter)).collect(Collectors.toList());
    }

    /**
     * Pushes physical-only and metadata-only predicates to their own abilities and keeps mixed ones
     * as runtime filters, so per FLIP-574 {@code applyFilters} never sees a metadata column.
     * Returns {@code null} if nothing can be pushed.
     */
    @Nullable
    protected FilterPushDownResult classifyAndPushFilters(
            RexNode[] convertiblePredicates,
            RexNode[] unconvertedPredicates,
            TableSourceTable tableSourceTable,
            TableScan scan,
            RelBuilder relBuilder) {
        boolean supportsPhysicalFilter = canPushdownFilter(tableSourceTable);
        boolean supportsMetadataFilter = canPushdownMetadataFilter(tableSourceTable);
        List<String> metadataKeys = getReadMetadataKeys(tableSourceTable);
        int firstMetadataIndex = scan.getRowType().getFieldCount() - metadataKeys.size();

        List<RexNode> allRemainingRexNodes = new ArrayList<>();
        List<RexNode> physicalPredicates = new ArrayList<>();
        List<RexNode> metadataPredicates = new ArrayList<>();
        for (RexNode predicate : convertiblePredicates) {
            FilterColumnKind kind = classifyColumns(predicate, firstMetadataIndex);
            if (kind == FilterColumnKind.PHYSICAL_ONLY && supportsPhysicalFilter) {
                physicalPredicates.add(predicate);
            } else if (kind == FilterColumnKind.METADATA_ONLY && supportsMetadataFilter) {
                metadataPredicates.add(predicate);
            } else {
                allRemainingRexNodes.add(predicate);
            }
        }

        if (physicalPredicates.isEmpty() && metadataPredicates.isEmpty()) {
            return null;
        }

        TableSourceTable currentTable = tableSourceTable;
        if (!physicalPredicates.isEmpty()) {
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
        }

        if (!metadataPredicates.isEmpty()) {
            FilterPushDownResult metadataResult =
                    resolveMetadataFiltersAndCreateTableSourceTable(
                            metadataPredicates.toArray(new RexNode[0]),
                            currentTable,
                            scan,
                            metadataKeys);
            currentTable = metadataResult.newTableSourceTable;
            allRemainingRexNodes.addAll(metadataResult.remainingRexNodes);
        }

        allRemainingRexNodes.addAll(Arrays.asList(unconvertedPredicates));
        return new FilterPushDownResult(currentTable, allRemainingRexNodes);
    }

    /** Updated table and the predicates that must remain as runtime filters. */
    protected static final class FilterPushDownResult {
        final TableSourceTable newTableSourceTable;
        final List<RexNode> remainingRexNodes;

        FilterPushDownResult(
                TableSourceTable newTableSourceTable, List<RexNode> remainingRexNodes) {
            this.newTableSourceTable = newTableSourceTable;
            this.remainingRexNodes = remainingRexNodes;
        }
    }

    /**
     * Resolves filters via {@link SupportsFilterPushDown} and creates a new {@link
     * TableSourceTable}.
     */
    protected Tuple2<SupportsFilterPushDown.Result, TableSourceTable>
            resolveFiltersAndCreateTableSourceTable(
                    RexNode[] convertiblePredicates,
                    TableSourceTable oldTableSourceTable,
                    TableScan scan,
                    RelBuilder relBuilder) {
        // record size before applyFilters for update statistics
        int originPredicatesSize = convertiblePredicates.length;

        // update DynamicTableSource
        DynamicTableSource newTableSource = oldTableSourceTable.tableSource().copy();

        SupportsFilterPushDown.Result result =
                FilterPushDownSpec.apply(
                        Arrays.asList(convertiblePredicates),
                        newTableSource,
                        SourceAbilityContext.from(scan));

        relBuilder.push(scan);
        List<RexNode> acceptedPredicates =
                convertExpressionToRexNode(result.getAcceptedFilters(), relBuilder);
        FilterPushDownSpec filterPushDownSpec = new FilterPushDownSpec(acceptedPredicates);

        TableSourceTable newTableSourceTable =
                oldTableSourceTable.copy(
                        newTableSource,
                        oldTableSourceTable.getStatistic(),
                        new SourceAbilitySpec[] {filterPushDownSpec});

        return new Tuple2<>(result, newTableSourceTable);
    }

    /** Whether filter push-down is possible and not already assigned. */
    protected boolean canPushdownFilter(TableSourceTable tableSourceTable) {
        return tableSourceTable != null
                && tableSourceTable.tableSource() instanceof SupportsFilterPushDown
                && Arrays.stream(tableSourceTable.abilitySpecs())
                        .noneMatch(spec -> spec instanceof FilterPushDownSpec);
    }

    /** Whether metadata filter push-down is possible and not already assigned. */
    protected boolean canPushdownMetadataFilter(TableSourceTable tableSourceTable) {
        if (tableSourceTable == null) {
            return false;
        }
        DynamicTableSource source = tableSourceTable.tableSource();
        if (!(source instanceof SupportsReadingMetadata)) {
            return false;
        }
        if (!((SupportsReadingMetadata) source).supportsMetadataFilterPushDown()) {
            return false;
        }
        return Arrays.stream(tableSourceTable.abilitySpecs())
                .noneMatch(spec -> spec instanceof MetadataFilterPushDownSpec);
    }

    private enum FilterColumnKind {
        PHYSICAL_ONLY,
        METADATA_ONLY,
        MIXED
    }

    private static FilterColumnKind classifyColumns(RexNode predicate, int firstMetadataIndex) {
        ImmutableBitSet columns = RelOptUtil.InputFinder.bits(predicate);
        boolean referencesMetadata = columns.nextSetBit(firstMetadataIndex) >= 0;
        boolean referencesPhysical = !columns.get(0, firstMetadataIndex).isEmpty();
        if (referencesMetadata && referencesPhysical) {
            return FilterColumnKind.MIXED;
        }
        return referencesMetadata ? FilterColumnKind.METADATA_ONLY : FilterColumnKind.PHYSICAL_ONLY;
    }

    /** Keys of the {@link ReadingMetadataSpec}, in the order of the scan's trailing fields. */
    private static List<String> getReadMetadataKeys(TableSourceTable tableSourceTable) {
        ReadingMetadataSpec spec =
                ScanReuserUtils.getAbilitySpec(
                        tableSourceTable.abilitySpecs(), ReadingMetadataSpec.class);
        return spec == null ? Collections.emptyList() : spec.getMetadataKeys();
    }

    /** Resolves metadata filters and creates a new {@link TableSourceTable}. */
    private FilterPushDownResult resolveMetadataFiltersAndCreateTableSourceTable(
            RexNode[] metadataPredicates,
            TableSourceTable oldTableSourceTable,
            TableScan scan,
            List<String> metadataKeys) {
        DynamicTableSource newTableSource = oldTableSourceTable.tableSource().copy();
        SourceAbilityContext abilityContext = SourceAbilityContext.from(scan);

        // Field names are metadata keys, not SQL aliases, to avoid physical/metadata collisions
        // (e.g. `offset INT, msg_offset INT METADATA FROM 'offset'`).
        MetadataRowInfo metadataRowInfo =
                buildMetadataRowInfo(abilityContext.getSourceRowType(), metadataKeys);
        RexNode[] remappedPredicates =
                remapPredicates(metadataPredicates, metadataRowInfo.oldIndexToNewIndex);

        List<ResolvedExpression> resolved =
                MetadataFilterPushDownSpec.resolvedExpressions(
                        Arrays.asList(remappedPredicates),
                        metadataRowInfo.metadataRowType,
                        newTableSource,
                        abilityContext);
        MetadataFilterResult result =
                MetadataFilterPushDownSpec.applyMetadataFiltersOnSource(newTableSource, resolved);

        // Source must return back the same instances it received.
        Set<ResolvedExpression> acceptedSet = Sets.newIdentityHashSet();
        acceptedSet.addAll(result.getAcceptedFilters());
        Set<ResolvedExpression> remainingSet = Sets.newIdentityHashSet();
        remainingSet.addAll(result.getRemainingFilters());
        Set<ResolvedExpression> inputSet = Sets.newIdentityHashSet();
        inputSet.addAll(resolved);

        for (ResolvedExpression r :
                Iterables.concat(result.getAcceptedFilters(), result.getRemainingFilters())) {
            if (!inputSet.contains(r)) {
                throw new TableException(
                        "Source returned a metadata filter not in the input list. Sources must "
                                + "return back the same ResolvedExpression instances they "
                                + "received from applyMetadataFilters.");
            }
        }

        List<RexNode> acceptedRemappedPredicates = new ArrayList<>();
        List<RexNode> remainingInputRexNodes = new ArrayList<>();
        for (int i = 0; i < resolved.size(); i++) {
            ResolvedExpression r = resolved.get(i);
            boolean inAccepted = acceptedSet.contains(r);
            boolean inRemaining = remainingSet.contains(r);
            if (!inAccepted && !inRemaining) {
                throw new TableException(
                        "Source dropped a metadata filter that was passed to "
                                + "applyMetadataFilters. Every input predicate must appear in the "
                                + "result's accepted list, remaining list, or both.");
            }
            if (inAccepted) {
                acceptedRemappedPredicates.add(remappedPredicates[i]);
            }
            if (inRemaining) {
                remainingInputRexNodes.add(metadataPredicates[i]);
            }
        }

        MetadataFilterPushDownSpec metadataSpec =
                new MetadataFilterPushDownSpec(
                        acceptedRemappedPredicates, metadataRowInfo.metadataRowType);

        TableSourceTable newTableSourceTable =
                oldTableSourceTable.copy(
                        newTableSource,
                        oldTableSourceTable.getStatistic(),
                        new SourceAbilitySpec[] {metadataSpec});

        return new FilterPushDownResult(newTableSourceTable, remainingInputRexNodes);
    }

    /**
     * Builds a {@link RowType} containing only metadata columns (named by metadata key) together
     * with an old-index-to-new-index mapping from the scan's source row type. Non-metadata
     * positions are absent from the mapping.
     */
    private MetadataRowInfo buildMetadataRowInfo(RowType sourceRowType, List<String> metadataKeys) {
        int firstMetadataIndex = sourceRowType.getFieldCount() - metadataKeys.size();
        List<RowField> metadataFields = new ArrayList<>();
        Map<Integer, Integer> oldToNewIndex = new HashMap<>();
        for (int i = firstMetadataIndex; i < sourceRowType.getFieldCount(); i++) {
            oldToNewIndex.put(i, metadataFields.size());
            metadataFields.add(
                    new RowField(
                            metadataKeys.get(i - firstMetadataIndex), sourceRowType.getTypeAt(i)));
        }
        return new MetadataRowInfo(new RowType(false, metadataFields), oldToNewIndex);
    }

    /**
     * Rewrites {@link RexInputRef}s in each predicate using the supplied old->new index map. Throws
     * if a predicate references an index not in the map (would indicate a non-metadata reference
     * slipped past {@link #classifyColumns}).
     */
    private RexNode[] remapPredicates(
            RexNode[] predicates, Map<Integer, Integer> oldIndexToNewIndex) {
        RexShuttle shuttle =
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef inputRef) {
                        Integer newIdx = oldIndexToNewIndex.get(inputRef.getIndex());
                        if (newIdx == null) {
                            throw new TableException(
                                    "Metadata predicate references non-metadata column index "
                                            + inputRef.getIndex());
                        }
                        return new RexInputRef(newIdx, inputRef.getType());
                    }
                };
        return Arrays.stream(predicates).map(p -> p.accept(shuttle)).toArray(RexNode[]::new);
    }

    private static final class MetadataRowInfo {
        final RowType metadataRowType;
        final Map<Integer, Integer> oldIndexToNewIndex;

        MetadataRowInfo(RowType metadataRowType, Map<Integer, Integer> oldIndexToNewIndex) {
            this.metadataRowType = metadataRowType;
            this.oldIndexToNewIndex = oldIndexToNewIndex;
        }
    }
}
