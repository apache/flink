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

import org.apache.calcite.util.ImmutableBitSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Set;

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
        if (null == upsertKeys || upsertKeys.isEmpty()) {
            return new int[0];
        }
        return upsertKeys.stream()
                .map(ImmutableBitSet::toArray)
                .reduce(
                        (k1, k2) -> {
                            if (k1.length < k2.length) {
                                return k1;
                            }
                            if (k1.length == k2.length) {
                                for (int index = 0; index < k1.length; index++) {
                                    if (k1[index] < k2[index]) {
                                        return k1;
                                    }
                                }
                            }
                            return k2;
                        })
                .get();
    }
}
