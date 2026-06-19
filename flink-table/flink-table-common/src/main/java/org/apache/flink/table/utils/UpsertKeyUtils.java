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

package org.apache.flink.table.utils;

import org.apache.flink.annotation.Internal;

import java.util.Comparator;
import java.util.List;

/** Helpers for working with upsert key candidates. */
@Internal
public final class UpsertKeyUtils {

    /**
     * Comparator that orders upsert-key candidates deterministically and stably across releases:
     * candidates with fewer columns come first; ties between equal-cardinality candidates are
     * broken by the column indices in ascending order, leading column first.
     */
    private static final Comparator<int[]> SMALLEST_FIRST =
            Comparator.<int[]>comparingInt(a -> a.length)
                    .thenComparing(
                            (a, b) -> {
                                for (int i = 0; i < a.length; i++) {
                                    final int cmp = Integer.compare(a[i], b[i]);
                                    if (cmp != 0) {
                                        return cmp;
                                    }
                                }
                                return 0;
                            });

    /**
     * Picks the smallest upsert key from the given candidates using {@link #SMALLEST_FIRST}.
     * Returns an empty array when the candidate list is empty. The returned reference is one of the
     * input arrays; callers must not mutate it.
     */
    public static int[] smallestKey(final List<int[]> candidates) {
        if (candidates.isEmpty()) {
            return new int[0];
        }
        int[] smallest = candidates.get(0);
        for (int i = 1; i < candidates.size(); i++) {
            if (SMALLEST_FIRST.compare(candidates.get(i), smallest) < 0) {
                smallest = candidates.get(i);
            }
        }
        return smallest;
    }

    private UpsertKeyUtils() {}
}
