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

import org.apache.flink.table.utils.UpsertKeyUtils;

import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Utility for upsertKey which represented as a Set of {@link
 * org.apache.calcite.util.ImmutableBitSet}.
 */
public class UpsertKeyUtil {

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
     * Returns whether {@code fieldRefs} is fully contained in some single upsert key. Returns
     * {@code false} when {@code upsertKeys} is null or empty.
     */
    public static boolean isContainedInAnyUpsertKey(
            @Nullable Set<ImmutableBitSet> upsertKeys, ImmutableBitSet fieldRefs) {
        return upsertKeys != null && upsertKeys.stream().anyMatch(uk -> uk.contains(fieldRefs));
    }

    /**
     * Remaps top-level column indices through a {@link
     * org.apache.flink.table.planner.plan.abilities.source.ProjectPushDownSpec#getProjectedFields()}
     * mapping. Returns empty when any original index has no top-level entry (column dropped or only
     * nested sub-fields kept).
     */
    public static Optional<ImmutableBitSet> remapFieldRefsThroughProjection(
            ImmutableBitSet originalRefs, int[][] projectedFields) {
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        for (int origIdx : originalRefs.toArray()) {
            OptionalInt newIdx =
                    IntStream.range(0, projectedFields.length)
                            .filter(
                                    i ->
                                            projectedFields[i].length == 1
                                                    && projectedFields[i][0] == origIdx)
                            .findFirst();
            if (!newIdx.isPresent()) {
                return Optional.empty();
            }
            builder.set(newIdx.getAsInt());
        }
        return Optional.of(builder.build());
    }
}
