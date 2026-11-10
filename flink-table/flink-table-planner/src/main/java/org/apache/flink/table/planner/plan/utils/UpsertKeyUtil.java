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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery;
import org.apache.flink.table.utils.UpsertKeyUtils;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility for upsertKey which represented as a Set of {@link
 * org.apache.calcite.util.ImmutableBitSet}.
 */
public class UpsertKeyUtil {

    private static final Set<SqlKind> EQUALITY_KINDS =
            EnumSet.of(SqlKind.EQUALS, SqlKind.IS_NOT_DISTINCT_FROM);

    /**
     * Returns the smallest key of given upsert keys. The rule of 'small' is an upsert key
     * represented by {@link ImmutableBitSet} has smaller cardinality or has a smaller leading
     * element when the same cardinality. E.g., '{0,1}' is smaller than '{0,1,2}' and '{0,1}' is
     * smaller than '{0,2}'.
     *
     * @param upsertKeys input upsert keys
     * @return the smallest key
     */
    @Nonnull
    public static int[] getSmallestKey(@Nullable Set<ImmutableBitSet> upsertKeys) {
        return smallestKey(upsertKeys).orElse(new int[0]);
    }

    /**
     * Returns the smallest key of given upsert keys wrapped with a java {@link Optional}. Different
     * from {@link #getSmallestKey(Set)}, it'll return result with an empty {@link Optional} if the
     * input set is null or empty.
     */
    public static Optional<int[]> smallestKey(@Nullable Set<ImmutableBitSet> upsertKeys) {
        if (null == upsertKeys || upsertKeys.isEmpty()) {
            return Optional.empty();
        }
        final List<int[]> asArrays =
                upsertKeys.stream().map(ImmutableBitSet::toArray).collect(Collectors.toList());
        return Optional.of(UpsertKeyUtils.smallestKey(asArrays));
    }

    /**
     * Returns the upsert keys of the given node, plus each key whose columns outside {@code
     * columns} can all be replaced by columns inside that are equal to them on every row, e.g.
     * through an inner equi-join condition. For example, for {@code t1 JOIN t2 ON a1 = a2} and
     * columns {a1, b2}, the upsert key {a2, b2} adds {a1, b2}.
     *
     * @return the upsert keys and the aligned ones, or null if the upsert keys are unknown
     */
    public static @Nullable Set<ImmutableBitSet> getUpsertKeysAlignedTo(
            RelNode rel, ImmutableBitSet columns, RelMetadataQuery mq) {
        final FlinkRelMetadataQuery fmq = FlinkRelMetadataQuery.reuseOrCreate(mq);
        final Set<ImmutableBitSet> upsertKeys = fmq.getUpsertKeys(rel);
        if (upsertKeys == null || upsertKeys.stream().allMatch(columns::contains)) {
            return upsertKeys;
        }

        final Map<Integer, Integer> parents = getEqualColumnParents(rel, fmq);
        final Map<Integer, Integer> replacements = new HashMap<>();
        for (int column : columns) {
            replacements.putIfAbsent(root(parents, column), column);
        }
        final Set<ImmutableBitSet> alignedKeys = new HashSet<>(upsertKeys);
        for (ImmutableBitSet key : upsertKeys) {
            final ImmutableBitSet.Builder alignedKey = ImmutableBitSet.builder();
            for (int column : key) {
                alignedKey.set(
                        columns.get(column)
                                ? column
                                : replacements.getOrDefault(root(parents, column), column));
            }
            final ImmutableBitSet aligned = alignedKey.build();
            if (columns.contains(aligned)) {
                alignedKeys.add(aligned);
            }
        }
        return alignedKeys;
    }

    /**
     * Returns {@code columns} and every column of the given node that is equal to one of them on
     * every row.
     */
    public static ImmutableBitSet getEqualColumns(
            RelNode rel, ImmutableBitSet columns, RelMetadataQuery mq) {
        final Map<Integer, Integer> parents = getEqualColumnParents(rel, mq);
        final Set<Integer> roots =
                columns.asList().stream()
                        .map(column -> root(parents, column))
                        .collect(Collectors.toSet());
        final ImmutableBitSet.Builder equalColumns = columns.rebuild();
        for (int column : parents.keySet()) {
            final int root = root(parents, column);
            if (roots.contains(root)) {
                equalColumns.set(column).set(root);
            }
        }
        return equalColumns.build();
    }

    /** Groups the columns that are equal on every row, as a union-find forest of parents. */
    private static Map<Integer, Integer> getEqualColumnParents(RelNode rel, RelMetadataQuery mq) {
        final Map<Integer, Integer> parents = new HashMap<>();
        for (RexNode predicate : mq.getPulledUpPredicates(rel).pulledUpPredicates) {
            if (!predicate.isA(EQUALITY_KINDS)) {
                continue;
            }
            final RexNode left = ((RexCall) predicate).getOperands().get(0);
            final RexNode right = ((RexCall) predicate).getOperands().get(1);
            // comparing different types may implicitly cast one side, which needn't be injective,
            // and FLOAT/DOUBLE compare -0.0 = 0.0 although they are different keys
            if (left instanceof RexInputRef
                    && right instanceof RexInputRef
                    && SqlTypeUtil.equalSansNullability(left.getType(), right.getType())
                    && !SqlTypeUtil.isApproximateNumeric(left.getType())) {
                final int leftRoot = root(parents, ((RexInputRef) left).getIndex());
                final int rightRoot = root(parents, ((RexInputRef) right).getIndex());
                if (leftRoot != rightRoot) {
                    parents.put(leftRoot, rightRoot);
                }
            }
        }
        return parents;
    }

    private static int root(Map<Integer, Integer> parents, int column) {
        int root = column;
        Integer parent;
        while ((parent = parents.get(root)) != null) {
            root = parent;
        }
        return root;
    }
}
