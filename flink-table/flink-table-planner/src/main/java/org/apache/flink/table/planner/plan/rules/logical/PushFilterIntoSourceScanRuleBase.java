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
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata.MetadataFilterResult;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.planner.expressions.converter.ExpressionConverter;
import org.apache.flink.table.planner.plan.abilities.source.FilterPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.MetadataFilterPushDownSpec;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilityContext;
import org.apache.flink.table.planner.plan.abilities.source.SourceAbilitySpec;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;

import org.apache.flink.shaded.guava33.com.google.common.collect.Iterables;
import org.apache.flink.shaded.guava33.com.google.common.collect.Sets;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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

    protected RexNode createRemainingCondition(
            RelBuilder relBuilder,
            List<ResolvedExpression> expressions,
            RexNode[] unconvertedPredicates) {
        List<RexNode> remainingPredicates = convertExpressionToRexNode(expressions, relBuilder);
        remainingPredicates.addAll(Arrays.asList(unconvertedPredicates));
        return relBuilder.and(remainingPredicates);
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

    /**
     * Classifies convertible predicates into physical and metadata, pushes each through the
     * appropriate path, and returns the updated table plus any remaining predicates.
     */
    protected FilterClassificationResult classifyAndPushFilters(
            RexNode[] convertiblePredicates,
            TableSourceTable tableSourceTable,
            TableScan scan,
            RelBuilder relBuilder) {

        boolean supportsPhysicalFilter = canPushdownFilter(tableSourceTable);
        boolean supportsMetadataFilter = canPushdownMetadataFilter(tableSourceTable);
        Set<Integer> metadataColumnIndices = metadataColumnIndices(tableSourceTable, scan);

        List<RexNode> allRemainingRexNodes = new ArrayList<>();
        TableSourceTable currentTable = tableSourceTable;

        List<RexNode> physicalPredicates = new ArrayList<>();
        List<RexNode> metadataPredicates = new ArrayList<>();
        for (RexNode predicate : convertiblePredicates) {
            if (referencesOnlyMetadataColumns(predicate, metadataColumnIndices)) {
                if (supportsMetadataFilter) {
                    metadataPredicates.add(predicate);
                } else {
                    allRemainingRexNodes.add(predicate);
                }
            } else if (referencesAnyMetadataColumns(predicate, metadataColumnIndices)) {
                allRemainingRexNodes.add(predicate);
            } else {
                physicalPredicates.add(predicate);
            }
        }

        if ((physicalPredicates.isEmpty() || !supportsPhysicalFilter)
                && metadataPredicates.isEmpty()) {
            return null;
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

        return new FilterClassificationResult(currentTable, allRemainingRexNodes);
    }

    /** Result of classifying and pushing filters through physical and metadata paths. */
    protected static final class FilterClassificationResult {
        final TableSourceTable updatedTable;
        final List<RexNode> remainingPredicates;

        FilterClassificationResult(
                TableSourceTable updatedTable, List<RexNode> remainingPredicates) {
            this.updatedTable = updatedTable;
            this.remainingPredicates = remainingPredicates;
        }
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

    /**
     * True if predicate references metadata columns exclusively (no physical columns).
     *
     * <p>A predicate like {@code OR(physical_pred, metadata_pred)} returns false because it
     * references both physical and metadata columns. Mixed predicates remain as runtime filters.
     */
    protected boolean referencesOnlyMetadataColumns(
            RexNode predicate, Set<Integer> metadataColumnIndices) {
        boolean[] saw = classifyColumnReferences(predicate, metadataColumnIndices);
        return saw[1] && !saw[0];
    }

    /** True if predicate references at least one metadata column (may also reference physical). */
    protected boolean referencesAnyMetadataColumns(
            RexNode predicate, Set<Integer> metadataColumnIndices) {
        boolean[] saw = classifyColumnReferences(predicate, metadataColumnIndices);
        return saw[1];
    }

    private boolean[] classifyColumnReferences(
            RexNode predicate, Set<Integer> metadataColumnIndices) {
        boolean[] saw = new boolean[2]; // [0] = sawPhysical, [1] = sawMetadata
        predicate.accept(
                new RexVisitorImpl<Void>(true) {
                    @Override
                    public Void visitInputRef(RexInputRef inputRef) {
                        if (metadataColumnIndices.contains(inputRef.getIndex())) {
                            saw[1] = true;
                        } else {
                            saw[0] = true;
                        }
                        return null;
                    }
                });
        return saw;
    }

    /**
     * Indices of metadata columns within the scan's current (possibly projection-narrowed) row
     * type. Predicate {@link RexInputRef}s are positioned against this same row type, so the
     * physical/metadata distinction must be derived from it rather than from the full table schema.
     */
    private Set<Integer> metadataColumnIndices(TableSourceTable tableSourceTable, TableScan scan) {
        Map<String, String> columnToMetadataKey = buildColumnToMetadataKeyMap(tableSourceTable);
        List<String> fieldNames = scan.getRowType().getFieldNames();
        Set<Integer> indices = new HashSet<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            if (columnToMetadataKey.containsKey(fieldNames.get(i))) {
                indices.add(i);
            }
        }
        return indices;
    }

    /** Maps SQL column names to metadata keys for metadata columns. */
    protected Map<String, String> buildColumnToMetadataKeyMap(TableSourceTable tableSourceTable) {
        ResolvedSchema schema = tableSourceTable.contextResolvedTable().getResolvedSchema();
        Map<String, String> mapping = new HashMap<>();
        for (Column col : schema.getColumns()) {
            if (col instanceof Column.MetadataColumn) {
                Column.MetadataColumn metaCol = (Column.MetadataColumn) col;
                String sqlName = metaCol.getName();
                String metadataKey = metaCol.getMetadataKey().orElse(sqlName);
                mapping.put(sqlName, metadataKey);
            }
        }
        return mapping;
    }

    /** Outcome of metadata filter push-down: updated table and runtime-Calc predicates. */
    protected static final class MetadataPushDownOutcome {
        final TableSourceTable newTableSourceTable;
        final List<RexNode> remainingInputRexNodes;

        MetadataPushDownOutcome(
                TableSourceTable newTableSourceTable, List<RexNode> remainingInputRexNodes) {
            this.newTableSourceTable = newTableSourceTable;
            this.remainingInputRexNodes = remainingInputRexNodes;
        }
    }

    /** Resolves metadata filters and creates a new {@link TableSourceTable}. */
    protected MetadataPushDownOutcome resolveMetadataFiltersAndCreateTableSourceTable(
            RexNode[] metadataPredicates, TableSourceTable oldTableSourceTable, TableScan scan) {
        DynamicTableSource newTableSource = oldTableSourceTable.tableSource().copy();
        SourceAbilityContext abilityContext = SourceAbilityContext.from(scan);

        // Field names are metadata keys, not SQL aliases, to avoid physical/metadata collisions
        // (e.g. `offset INT, msg_offset INT METADATA FROM 'offset'`).
        MetadataRowInfo metadataRowInfo =
                buildMetadataRowInfo(oldTableSourceTable, abilityContext.getSourceRowType());
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

        return new MetadataPushDownOutcome(newTableSourceTable, remainingInputRexNodes);
    }

    /**
     * Builds a {@link RowType} containing only metadata columns (named by metadata key) together
     * with an old-index-to-new-index mapping from the scan's source row type. Non-metadata
     * positions are absent from the mapping.
     */
    private MetadataRowInfo buildMetadataRowInfo(
            TableSourceTable tableSourceTable, RowType sourceRowType) {
        Map<String, String> columnToMetadataKey = buildColumnToMetadataKeyMap(tableSourceTable);
        List<RowField> metadataFields = new ArrayList<>();
        Map<Integer, Integer> oldToNewIndex = new HashMap<>();
        for (int i = 0; i < sourceRowType.getFieldCount(); i++) {
            String sqlName = sourceRowType.getFieldNames().get(i);
            String metadataKey = columnToMetadataKey.get(sqlName);
            if (metadataKey != null) {
                oldToNewIndex.put(i, metadataFields.size());
                metadataFields.add(new RowField(metadataKey, sourceRowType.getTypeAt(i)));
            }
        }
        return new MetadataRowInfo(new RowType(false, metadataFields), oldToNewIndex);
    }

    /**
     * Rewrites {@link RexInputRef}s in each predicate using the supplied old->new index map. Throws
     * if a predicate references an index not in the map (would indicate a non-metadata reference
     * slipped past {@link #referencesOnlyMetadataColumns}).
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
