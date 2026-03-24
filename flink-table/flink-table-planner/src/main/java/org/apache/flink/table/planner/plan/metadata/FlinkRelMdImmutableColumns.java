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

package org.apache.flink.table.planner.plan.metadata;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.planner.plan.nodes.calcite.WatermarkAssigner;
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalLookupJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalChangelogNormalize;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDropUpdateBefore;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalMiniBatchAssigner;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** A metadata handler for {@link FlinkMetadata.ImmutableColumns}. */
public class FlinkRelMdImmutableColumns implements MetadataHandler<FlinkMetadata.ImmutableColumns> {
    static final FlinkRelMdImmutableColumns INSTANCE = new FlinkRelMdImmutableColumns();

    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    FlinkMetadata.ImmutableColumns.METHOD, INSTANCE);

    // ~ Constructors -----------------------------------------------------------

    private FlinkRelMdImmutableColumns() {}

    // ~ Methods ----------------------------------------------------------------

    public MetadataDef<FlinkMetadata.ImmutableColumns> getDef() {
        return FlinkMetadata.ImmutableColumns.DEF;
    }

    public ImmutableBitSet getImmutableColumns(TableScan rel, RelMetadataQuery mq) {
        return getTableImmutableColumns(rel.getTable());
    }

    public ImmutableBitSet getImmutableColumns(Project rel, RelMetadataQuery mq) {
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        return guardByUpsertKeys(
                rel, getProjectImmutableColumns(rel.getProjects(), rel.getInput(), fmq), fmq);
    }

    public ImmutableBitSet getImmutableColumns(Filter rel, RelMetadataQuery mq) {
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        return guardByUpsertKeys(rel, fmq.getImmutableColumns(rel.getInput()), fmq);
    }

    public ImmutableBitSet getImmutableColumns(Calc rel, RelMetadataQuery mq) {
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        List<RexNode> projects =
                rel.getProgram().getProjectList().stream()
                        .map(localRef -> rel.getProgram().expandLocalRef(localRef))
                        .collect(Collectors.toList());
        return guardByUpsertKeys(
                rel, getProjectImmutableColumns(projects, rel.getInput(), fmq), fmq);
    }

    public ImmutableBitSet getImmutableColumns(Exchange rel, RelMetadataQuery mq) {
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        return guardByUpsertKeys(rel, fmq.getImmutableColumns(rel.getInput()), fmq);
    }

    public ImmutableBitSet getImmutableColumns(
            StreamPhysicalChangelogNormalize rel, RelMetadataQuery mq) {
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        return guardByUpsertKeys(rel, fmq.getImmutableColumns(rel.getInput()), fmq);
    }

    public ImmutableBitSet getImmutableColumns(
            StreamPhysicalMiniBatchAssigner rel, RelMetadataQuery mq) {
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        return guardByUpsertKeys(rel, fmq.getImmutableColumns(rel.getInput()), fmq);
    }

    public ImmutableBitSet getImmutableColumns(
            StreamPhysicalDropUpdateBefore rel, RelMetadataQuery mq) {
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        return guardByUpsertKeys(rel, fmq.getImmutableColumns(rel.getInput()), fmq);
    }

    public ImmutableBitSet getImmutableColumns(WatermarkAssigner rel, RelMetadataQuery mq) {
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        return guardByUpsertKeys(rel, fmq.getImmutableColumns(rel.getInput()), fmq);
    }

    public ImmutableBitSet getImmutableColumns(Join join, RelMetadataQuery mq) {
        JoinRelType joinType = join.getJoinType();

        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        int leftFieldCount = join.getLeft().getRowType().getFieldCount();

        return unionJoinImmutableCols(
                join,
                joinType,
                () -> fmq.getImmutableColumns(join.getLeft()),
                () -> fmq.getImmutableColumns(join.getRight()),
                leftFieldCount,
                fmq);
    }

    public ImmutableBitSet getImmutableColumns(CommonPhysicalLookupJoin join, RelMetadataQuery mq) {
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        int leftFieldCount = join.getInput().getRowType().getFieldCount();

        return unionJoinImmutableCols(
                join,
                join.joinType(),
                () -> fmq.getImmutableColumns(join.getInput()),
                // TODO support propagating immutable columns from the lookup side
                () -> null, // rightImmutableColsSupplier
                leftFieldCount,
                fmq);
    }

    public ImmutableBitSet getImmutableColumns(HepRelVertex rel, RelMetadataQuery mq) {
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        return guardByUpsertKeys(rel, fmq.getImmutableColumns(rel.getCurrentRel()), fmq);
    }

    public ImmutableBitSet getImmutableColumns(RelSubset rel, RelMetadataQuery mq) {
        FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        return guardByUpsertKeys(
                rel, fmq.getImmutableColumns(Util.first(rel.getBest(), rel.getOriginal())), fmq);
    }

    public ImmutableBitSet getImmutableColumns(RelNode rel, RelMetadataQuery mq) {
        // Catch-all rule when none of the others apply.
        // More nodes can be supported later, such as Expand, Aggregate, Window, Rank, etc.
        return null;
    }

    /**
     * Guards the immutable columns by verifying that the node has upsert keys. Immutable columns
     * are only meaningful "within each pk"; if no upsert key exists, the result is cleared.
     */
    @Nullable
    private ImmutableBitSet guardByUpsertKeys(
            RelNode rel, @Nullable ImmutableBitSet immutableColumns, FlinkRelMetadataQuery fmq) {
        if (immutableColumns == null || immutableColumns.isEmpty()) {
            return immutableColumns;
        }
        Set<ImmutableBitSet> upsertKeys = fmq.getUpsertKeys(rel);
        if (upsertKeys == null || upsertKeys.isEmpty()) {
            return null;
        }
        return immutableColumns;
    }

    /**
     * Unions left/right immutable columns for a join, respecting join type semantics:
     *
     * <ul>
     *   <li>SEMI / ANTI: output contains only left-side columns → propagate left immutable only
     *   <li>LEFT: right side may produce nulls → ignore right immutable
     *   <li>RIGHT: left side may produce nulls → ignore left immutable
     *   <li>FULL: both sides may produce nulls → ignore both
     *   <li>INNER: both sides preserved
     * </ul>
     *
     * <p>Right-side indices are shifted by leftFieldCount before union. The result is guarded by
     * upsert keys.
     */
    @Nullable
    private ImmutableBitSet unionJoinImmutableCols(
            RelNode rel,
            JoinRelType joinType,
            Supplier<ImmutableBitSet> leftImmutableColsSupplier,
            Supplier<ImmutableBitSet> rightImmutableColsSupplier,
            int leftFieldCount,
            FlinkRelMetadataQuery fmq) {
        if (joinType == JoinRelType.SEMI || joinType == JoinRelType.ANTI) {
            return guardByUpsertKeys(rel, leftImmutableColsSupplier.get(), fmq);
        }

        // nullable side's columns may flip between value/null → not immutable
        ImmutableBitSet leftImmutableColumns =
                joinType.generatesNullsOnLeft() ? null : leftImmutableColsSupplier.get();
        ImmutableBitSet rightImmutableColumns =
                joinType.generatesNullsOnRight() ? null : rightImmutableColsSupplier.get();

        // shift right side indices by left field count
        ImmutableBitSet shiftedRight =
                rightImmutableColumns == null || rightImmutableColumns.isEmpty()
                        ? rightImmutableColumns
                        : ImmutableBitSet.of(
                                rightImmutableColumns.toList().stream()
                                        .map(i -> i + leftFieldCount)
                                        .collect(Collectors.toList()));

        // union left and right immutable columns
        ImmutableBitSet result;
        if (leftImmutableColumns != null && shiftedRight != null) {
            result = leftImmutableColumns.union(shiftedRight);
        } else {
            result = Optional.ofNullable(leftImmutableColumns).orElse(shiftedRight);
        }
        return guardByUpsertKeys(rel, result, fmq);
    }

    @Nullable
    private ImmutableBitSet getTableImmutableColumns(RelOptTable relOptTable) {
        if (!(relOptTable instanceof TableSourceTable)) {
            return null;
        }

        TableSourceTable tst = (TableSourceTable) relOptTable;
        ResolvedSchema schema = tst.contextResolvedTable().getResolvedTable().getResolvedSchema();

        if (schema.getPrimaryKey().isEmpty()) {
            return null;
        }

        // use relOptTable's type which may be projected based on original schema
        List<String> tableOutputFields = relOptTable.getRowType().getFieldNames();

        // add pk
        Set<String> allImmutableFieldsInSchema =
                new HashSet<>(schema.getPrimaryKey().get().getColumns());
        // add constraint for immutable columns
        if (schema.getImmutableColumns().isPresent()) {
            allImmutableFieldsInSchema.addAll(schema.getImmutableColumns().get().getColumns());
        }

        Set<Integer> outputImmutableColumns =
                allImmutableFieldsInSchema.stream()
                        .flatMap(
                                immutableField -> {
                                    int fieldIdx = tableOutputFields.indexOf(immutableField);
                                    if (fieldIdx >= 0) {
                                        return Stream.of(fieldIdx);
                                    } else {
                                        return Stream.empty();
                                    }
                                })
                        .collect(Collectors.toSet());

        return ImmutableBitSet.of(outputImmutableColumns);
    }

    @Nullable
    private ImmutableBitSet getProjectImmutableColumns(
            List<RexNode> projects, RelNode inputNode, FlinkRelMetadataQuery fmq) {
        ImmutableBitSet inputImmutableColumns = fmq.getImmutableColumns(inputNode);
        if (inputImmutableColumns == null || inputImmutableColumns.isEmpty()) {
            return inputImmutableColumns;
        }

        Map<Integer, List<Integer>> mapInToOutPos = new HashMap<>();
        for (int i = 0; i < projects.size(); i++) {
            RexNode projExpr = projects.get(i);
            if (projExpr instanceof RexInputRef) {
                mapInToOutPos
                        .computeIfAbsent(
                                ((RexInputRef) projExpr).getIndex(), k -> new ArrayList<>())
                        .add(i);
            }
        }
        return ImmutableBitSet.of(
                inputImmutableColumns.toList().stream()
                        .flatMap(in -> mapInToOutPos.getOrDefault(in, List.of()).stream())
                        .collect(Collectors.toList()));
    }
}
