/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.util.IntArrayList;
import org.apache.flink.util.CollectionUtil;

import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Contains the fine-grain channel mappings that occur when a connected operator has been rescaled.
 *
 * <p>Usually the mapping is materialized from new->old channel/subtask indexes. Through {@link
 * #invert()}, the direction may change accordingly. To generalize, the left side is called source
 * and the right side is called target(s) in this class.
 *
 * <p>{@ImplNote This class omits trailing empty targets.}
 */
public class RescaleMappings implements Serializable {
    public static final RescaleMappings SYMMETRIC_IDENTITY =
            RescaleMappings.identity(Integer.MAX_VALUE, Integer.MAX_VALUE);
    static final int[] EMPTY_TARGETS = new int[0];

    private static final long serialVersionUID = -8719670050630674631L;

    private final int numberOfSources;

    /**
     * The mapping from source to multiple targets. In most cases, the targets arrays are of
     * different sizes.
     */
    private final int[][] mappings;

    private final int numberOfTargets;

    RescaleMappings(int numberOfSources, int[][] mappings, int numberOfTargets) {
        this.numberOfSources = numberOfSources;
        this.mappings = checkNotNull(mappings);
        this.numberOfTargets = numberOfTargets;
    }

    public static RescaleMappings identity(int numberOfSources, int numberOfTargets) {
        return new IdentityRescaleMappings(numberOfSources, numberOfTargets);
    }

    public boolean isIdentity() {
        return false;
    }

    public int[] getMappedIndexes(int sourceIndex) {
        if (sourceIndex >= mappings.length) {
            return EMPTY_TARGETS;
        }
        return mappings[sourceIndex];
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final RescaleMappings that = (RescaleMappings) o;
        return Arrays.deepEquals(mappings, that.mappings);
    }

    @Override
    public int hashCode() {
        return Arrays.deepHashCode(mappings);
    }

    @Override
    public String toString() {
        return "RescaleMappings{" + "mappings=" + Arrays.deepToString(mappings) + '}';
    }

    public RescaleMappings invert() {
        IntArrayList[] inverted = new IntArrayList[numberOfTargets];
        for (int source = 0; source < mappings.length; source++) {
            final int[] targets = mappings[source];
            for (int target : targets) {
                IntArrayList sources = inverted[target];
                if (sources == null) {
                    inverted[target] = sources = new IntArrayList(1);
                }
                sources.add(source);
            }
        }
        return of(Arrays.stream(inverted).map(RescaleMappings::toSortedArray), numberOfSources);
    }

    public Set<Integer> getAmbiguousTargets() {
        final Set<Integer> ambiguousTargets =
                CollectionUtil.newHashSetWithExpectedSize(numberOfTargets);
        final BitSet usedTargets = new BitSet(numberOfTargets);

        for (int[] targets : mappings) {
            for (int target : targets) {
                if (usedTargets.get(target)) {
                    ambiguousTargets.add(target);
                } else {
                    usedTargets.set(target);
                }
            }
        }

        return ambiguousTargets;
    }

    public static RescaleMappings of(Stream<int[]> mappedTargets, int numberOfTargets) {
        final int[][] mappings =
                mappedTargets
                        .map(targets -> targets.length == 0 ? EMPTY_TARGETS : targets)
                        .toArray(int[][]::new);

        if (isIdentity(mappings, numberOfTargets)) {
            return new IdentityRescaleMappings(mappings.length, numberOfTargets);
        }

        int lastNonEmpty = mappings.length - 1;
        for (; lastNonEmpty >= 0; lastNonEmpty--) {
            if (mappings[lastNonEmpty] != EMPTY_TARGETS) {
                break;
            }
        }

        int length = lastNonEmpty + 1;
        return new RescaleMappings(
                mappings.length,
                length == mappings.length ? mappings : Arrays.copyOf(mappings, length),
                numberOfTargets);
    }

    private static boolean isIdentity(int[][] mappings, int numberOfTargets) {
        if (mappings.length < numberOfTargets) {
            return false;
        }
        for (int source = numberOfTargets; source < mappings.length; source++) {
            if (mappings[source] != EMPTY_TARGETS) {
                return false;
            }
        }
        for (int source = 0; source < numberOfTargets; source++) {
            if (mappings[source].length != 1 || source != mappings[source][0]) {
                return false;
            }
        }
        return true;
    }

    private static int[] toSortedArray(IntArrayList sourceList) {
        if (sourceList == null) {
            return EMPTY_TARGETS;
        }
        final int[] sources = sourceList.toArray();
        Arrays.sort(sources);
        return sources;
    }

    @VisibleForTesting
    int getNumberOfSources() {
        return numberOfSources;
    }

    @VisibleForTesting
    int getNumberOfTargets() {
        return numberOfTargets;
    }

    @VisibleForTesting
    int[][] getMappings() {
        return mappings;
    }

    private static final class IdentityRescaleMappings extends RescaleMappings {
        public static final int[][] IMPLICIT_MAPPING = new int[0][0];
        private static final long serialVersionUID = -4406023794753660925L;

        public IdentityRescaleMappings(int numberOfSources, int numberOfTargets) {
            super(numberOfSources, IMPLICIT_MAPPING, numberOfTargets);
        }

        @Override
        public int[] getMappedIndexes(int sourceIndex) {
            if (sourceIndex >= getNumberOfTargets()) {
                return EMPTY_TARGETS;
            }
            return new int[] {sourceIndex};
        }

        @Override
        public boolean isIdentity() {
            return true;
        }

        @Override
        public Set<Integer> getAmbiguousTargets() {
            return Collections.emptySet();
        }

        @Override
        public RescaleMappings invert() {
            return new IdentityRescaleMappings(getNumberOfTargets(), getNumberOfSources());
        }

        @Override
        public String toString() {
            return "IdentityRescaleMappings{"
                    + getNumberOfSources()
                    + "->"
                    + getAmbiguousTargets()
                    + '}';
        }
    }
}
