/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * This class combines a key-group range with offsets that correspond to the key-groups in the
 * range.
 */
public class KeyGroupRangeOffsets implements Iterable<Tuple2<Integer, Long>>, Serializable {

    public static IllegalArgumentException newIllegalKeyGroupException(
            int keyGroup, KeyGroupRange keyGroupRange) {
        return new IllegalArgumentException(
                String.format(
                        "Key group %d is not in %s. Unless you're directly using low level state access APIs, this"
                                + " is most likely caused by non-deterministic shuffle key (hashCode and equals implementation).",
                        keyGroup, keyGroupRange));
    }

    private static final long serialVersionUID = 6595415219136429696L;

    /** the range of key-groups */
    private final KeyGroupRange keyGroupRange;

    /** the aligned array of offsets for the key-groups */
    private final long[] offsets;

    /**
     * Creates key-group range with offsets from the given key-group range. The order of given
     * offsets must be aligned with respect to the key-groups in the range.
     *
     * @param keyGroupRange The range of key-groups.
     * @param offsets The aligned array of offsets for the given key-groups.
     */
    public KeyGroupRangeOffsets(KeyGroupRange keyGroupRange, long[] offsets) {
        this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
        this.offsets = Preconditions.checkNotNull(offsets);
        Preconditions.checkArgument(offsets.length == keyGroupRange.getNumberOfKeyGroups());
    }

    /**
     * Creates key-group range with offsets from the given start key-group to end key-group. The
     * order of given offsets must be aligned with respect to the key-groups in the range.
     *
     * @param rangeStart Start key-group of the range (inclusive)
     * @param rangeEnd End key-group of the range (inclusive)
     * @param offsets The aligned array of offsets for the given key-groups.
     */
    public KeyGroupRangeOffsets(int rangeStart, int rangeEnd, long[] offsets) {
        this(KeyGroupRange.of(rangeStart, rangeEnd), offsets);
    }

    /**
     * Creates key-group range with offsets from the given start key-group to end key-group. All
     * offsets are initially zero.
     *
     * @param rangeStart Start key-group of the range (inclusive)
     * @param rangeEnd End key-group of the range (inclusive)
     */
    public KeyGroupRangeOffsets(int rangeStart, int rangeEnd) {
        this(KeyGroupRange.of(rangeStart, rangeEnd));
    }

    /**
     * Creates key-group range with offsets for the given key-group range, where all offsets are
     * initially zero.
     *
     * @param keyGroupRange The range of key-groups.
     */
    public KeyGroupRangeOffsets(KeyGroupRange keyGroupRange) {
        this(keyGroupRange, new long[keyGroupRange.getNumberOfKeyGroups()]);
    }

    /**
     * Returns the offset for the given key-group. The key-group must be contained in the range.
     *
     * @param keyGroup Key-group for which we query the offset. Key-group must be contained in the
     *     range.
     * @return The offset for the given key-group which must be contained in the range.
     */
    public long getKeyGroupOffset(int keyGroup) {
        return offsets[computeKeyGroupIndex(keyGroup)];
    }

    /**
     * Sets the offset for the given key-group. The key-group must be contained in the range.
     *
     * @param keyGroup Key-group for which we set the offset. Must be contained in the range.
     * @param offset Offset for the key-group.
     */
    public void setKeyGroupOffset(int keyGroup, long offset) {
        offsets[computeKeyGroupIndex(keyGroup)] = offset;
    }

    /**
     * Returns a key-group range with offsets which is the intersection of the internal key-group
     * range with the given key-group range.
     *
     * @param keyGroupRange Key-group range to intersect with the internal key-group range.
     * @return The key-group range with offsets for the intersection of the internal key-group range
     *     with the given key-group range.
     */
    public KeyGroupRangeOffsets getIntersection(KeyGroupRange keyGroupRange) {
        Preconditions.checkNotNull(keyGroupRange);
        KeyGroupRange intersection = this.keyGroupRange.getIntersection(keyGroupRange);
        long[] subOffsets = new long[intersection.getNumberOfKeyGroups()];
        if (subOffsets.length > 0) {
            System.arraycopy(
                    offsets,
                    computeKeyGroupIndex(intersection.getStartKeyGroup()),
                    subOffsets,
                    0,
                    subOffsets.length);
        }
        return new KeyGroupRangeOffsets(intersection, subOffsets);
    }

    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    @Override
    public Iterator<Tuple2<Integer, Long>> iterator() {
        return new KeyGroupOffsetsIterator();
    }

    private int computeKeyGroupIndex(int keyGroup) {
        int idx = keyGroup - keyGroupRange.getStartKeyGroup();
        if (idx < 0 || idx >= offsets.length) {
            throw newIllegalKeyGroupException(keyGroup, keyGroupRange);
        }
        return idx;
    }

    /** Iterator for the Key-group/Offset pairs. */
    private final class KeyGroupOffsetsIterator implements Iterator<Tuple2<Integer, Long>> {

        public KeyGroupOffsetsIterator() {
            this.keyGroupIterator = keyGroupRange.iterator();
        }

        private final Iterator<Integer> keyGroupIterator;

        @Override
        public boolean hasNext() {
            return keyGroupIterator.hasNext();
        }

        @Override
        public Tuple2<Integer, Long> next() {
            Integer currentKeyGroup = keyGroupIterator.next();
            Tuple2<Integer, Long> result =
                    new Tuple2<>(
                            currentKeyGroup,
                            offsets[currentKeyGroup - keyGroupRange.getStartKeyGroup()]);
            return result;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Unsupported by this iterator!");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KeyGroupRangeOffsets)) {
            return false;
        }

        KeyGroupRangeOffsets that = (KeyGroupRangeOffsets) o;

        if (keyGroupRange != null
                ? !keyGroupRange.equals(that.keyGroupRange)
                : that.keyGroupRange != null) {
            return false;
        }
        return Arrays.equals(offsets, that.offsets);
    }

    @Override
    public int hashCode() {
        int result = keyGroupRange != null ? keyGroupRange.hashCode() : 0;
        result = 31 * result + Arrays.hashCode(offsets);
        return result;
    }

    @Override
    public String toString() {
        return "KeyGroupRangeOffsets{"
                + "keyGroupRange="
                + keyGroupRange
                + ", offsets="
                + Arrays.toString(offsets)
                + '}';
    }
}
