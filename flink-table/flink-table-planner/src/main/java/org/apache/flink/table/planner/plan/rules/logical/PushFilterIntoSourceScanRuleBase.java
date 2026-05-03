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
import java.util.List;
import java.util.Map;
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
    protected boolean referencesOnlyMetadataColumns(RexNode predicate, int physicalColumnCount) {
        boolean[] saw = new boolean[2]; // [0] = sawPhysical, [1] = sawMetadata
        predicate.accept(
                new RexVisitorImpl<Void>(true) {
                    @Override
                    public Void visitInputRef(RexInputRef inputRef) {
                        if (inputRef.getIndex() >= physicalColumnCount) {
                            saw[1] = true;
                        } else {
                            saw[0] = true;
                        }
                        return null;
                    }
                });
        return saw[1] && !saw[0];
    }

    /** Number of physical columns in the scan's schema. */
    protected int getPhysicalColumnCount(TableSourceTable tableSourceTable) {
        ResolvedSchema schema = tableSourceTable.contextResolvedTable().getResolvedSchema();
        return (int) schema.getColumns().stream().filter(Column::isPhysical).count();
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

    /** Resolves metadata filters and creates a new {@link TableSourceTable}. */
    protected Tuple2<MetadataFilterResult, TableSourceTable>
            resolveMetadataFiltersAndCreateTableSourceTable(
                    RexNode[] metadataPredicates,
                    TableSourceTable oldTableSourceTable,
                    TableScan scan,
                    RelBuilder relBuilder) {
        DynamicTableSource newTableSource = oldTableSourceTable.tableSource().copy();
        SourceAbilityContext abilityContext = SourceAbilityContext.from(scan);

        // Build a metadata-only row type (field names are metadata keys, not SQL aliases) and
        // an old->new index mapping. Storing only metadata columns avoids name collisions with
        // physical columns (e.g. `offset INT, msg_offset INT METADATA FROM 'offset'`).
        MetadataRowInfo metadataRowInfo =
                buildMetadataRowInfo(oldTableSourceTable, abilityContext.getSourceRowType());
        RexNode[] remappedPredicates =
                remapPredicates(metadataPredicates, metadataRowInfo.oldIndexToNewIndex);

        MetadataFilterResult result =
                MetadataFilterPushDownSpec.applyMetadataFilters(
                        Arrays.asList(remappedPredicates),
                        metadataRowInfo.metadataRowType,
                        newTableSource,
                        abilityContext);

        int acceptedCount = result.getAcceptedFilters().size();
        List<RexNode> acceptedRemappedPredicates = new ArrayList<>();
        for (int i = 0; i < acceptedCount; i++) {
            acceptedRemappedPredicates.add(remappedPredicates[i]);
        }
        MetadataFilterPushDownSpec metadataSpec =
                new MetadataFilterPushDownSpec(
                        acceptedRemappedPredicates, metadataRowInfo.metadataRowType);

        TableSourceTable newTableSourceTable =
                oldTableSourceTable.copy(
                        newTableSource,
                        oldTableSourceTable.getStatistic(),
                        new SourceAbilitySpec[] {metadataSpec});

        return new Tuple2<>(result, newTableSourceTable);
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
                            throw new IllegalStateException(
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
