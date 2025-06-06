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

package org.apache.flink.table.planner.plan.optimize;

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.planner.connectors.DynamicSourceUtils;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalcBase;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalWatermarkAssigner;
import org.apache.flink.table.planner.plan.optimize.program.FlinkChangelogModeInferenceProgram;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.trait.UpdateKindTrait$;
import org.apache.flink.table.planner.plan.trait.UpdateKindTraitDef$;
import org.apache.flink.table.planner.plan.utils.FlinkRelUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Checks if it is safe to remove ChangelogNormalize as part of {@link
 * FlinkChangelogModeInferenceProgram}. It checks:
 *
 * <ul>
 *   <li>if there is no filter pushed into the changelog normalize
 *   <li>if we don't need to produce UPDATE_BEFORE
 *   <li>we don't access any metadata columns
 * </ul>
 */
public class ChangelogNormalizeRequirementResolver {

    /** Checks if it is safe to remove ChangelogNormalize. */
    public static boolean isRequired(StreamPhysicalChangelogNormalize normalize) {
        if (normalize.filterCondition() != null) {
            return true;
        }
        if (!Objects.equals(
                normalize.getTraitSet().getTrait(UpdateKindTraitDef$.MODULE$.INSTANCE()),
                UpdateKindTrait$.MODULE$.ONLY_UPDATE_AFTER())) {
            return true;
        }

        // the changelog normalize is requested to perform deduplication on a retract stream
        if (ShortcutUtils.unwrapTableConfig(normalize)
                .get(ExecutionConfigOptions.TABLE_EXEC_SOURCE_CDC_EVENTS_DUPLICATE)) {
            return true;
        }

        // check if metadata columns are accessed
        final RelNode input = normalize.getInput();

        return visit(input, bitSetForAllOutputColumns(input));
    }

    private static ImmutableBitSet bitSetForAllOutputColumns(RelNode input) {
        return ImmutableBitSet.builder().set(0, input.getRowType().getFieldCount()).build();
    }

    private static boolean visit(final RelNode rel, final ImmutableBitSet requiredColumns) {
        if (rel instanceof StreamPhysicalCalcBase) {
            return visitCalc((StreamPhysicalCalcBase) rel, requiredColumns);
        } else if (rel instanceof StreamPhysicalTableSourceScan) {
            return visitTableSourceScan((StreamPhysicalTableSourceScan) rel, requiredColumns);
        } else if (rel instanceof StreamPhysicalWatermarkAssigner
                || rel instanceof StreamPhysicalExchange) {
            // require all input columns
            final RelNode input = ((SingleRel) rel).getInput();
            return visit(input, bitSetForAllOutputColumns(input));
        } else {
            // these nodes should not be in an input of a changelog normalize
            // StreamPhysicalChangelogNormalize
            // StreamPhysicalDropUpdateBefore
            // StreamPhysicalUnion
            // StreamPhysicalSort
            // StreamPhysicalLimit
            // StreamPhysicalSortLimit
            // StreamPhysicalTemporalSort
            // StreamPhysicalWindowTableFunction
            // StreamPhysicalWindowRank
            // StreamPhysicalWindowDeduplicate
            // StreamPhysicalRank
            // StreamPhysicalOverAggregateBase
            // CommonPhysicalJoin
            // StreamPhysicalMatch
            // StreamPhysicalMiniBatchAssigner
            // StreamPhysicalExpand
            // StreamPhysicalWindowAggregateBase
            // StreamPhysicalGroupAggregateBase
            // StreamPhysicalSink
            // StreamPhysicalLegacySink
            // StreamPhysicalCorrelateBase
            // StreamPhysicalLookupJoin
            // StreamPhysicalValues
            // StreamPhysicalDataStreamScan
            // StreamPhysicalLegacyTableSourceScan
            throw new UnsupportedOperationException(
                    String.format(
                            "Unsupported to visit node %s. The node either should not be pushed"
                                    + " through the changelog normalize or is not supported yet.",
                            rel.getClass().getSimpleName()));
        }
    }

    private static boolean visitTableSourceScan(
            StreamPhysicalTableSourceScan tableScan, ImmutableBitSet requiredColumns) {
        if (!(tableScan.tableSource() instanceof SupportsReadingMetadata)) {
            // source does not have metadata, no need to check
            return false;
        }
        final TableSourceTable sourceTable = tableScan.getTable().unwrap(TableSourceTable.class);
        assert sourceTable != null;
        // check if requiredColumns contain metadata column
        final List<Column.MetadataColumn> metadataColumns =
                DynamicSourceUtils.extractMetadataColumns(
                        sourceTable.contextResolvedTable().getResolvedSchema());
        final Set<String> metaColumnSet =
                metadataColumns.stream().map(Column::getName).collect(Collectors.toSet());
        final List<String> columns = tableScan.getRowType().getFieldNames();
        for (int index = 0; index < columns.size(); index++) {
            String column = columns.get(index);
            if (metaColumnSet.contains(column) && requiredColumns.get(index)) {
                // we require metadata column, therefore, we cannot remove the changelog normalize
                return true;
            }
        }

        return false;
    }

    private static boolean visitCalc(StreamPhysicalCalcBase calc, ImmutableBitSet requiredColumns) {
        // evaluate required columns from input
        final List<RexNode> projects =
                calc.getProgram().getProjectList().stream()
                        .map(expr -> calc.getProgram().expandLocalRef(expr))
                        .collect(Collectors.toList());
        final Map<Integer, List<Integer>> outFromSourcePos =
                FlinkRelUtil.extractSourceMapping(projects);
        final List<Integer> conv2Inputs =
                requiredColumns.toList().stream()
                        .map(
                                out ->
                                        Optional.ofNullable(outFromSourcePos.get(out))
                                                .orElseThrow(
                                                        () ->
                                                                new IllegalStateException(
                                                                        String.format(
                                                                                "Invalid pos:%d over projection:%s",
                                                                                out,
                                                                                calc
                                                                                        .getProgram()))))
                        .flatMap(Collection::stream)
                        .filter(index -> index != -1)
                        .distinct()
                        .collect(Collectors.toList());
        return visit(calc.getInput(), ImmutableBitSet.of(conv2Inputs));
    }
}
