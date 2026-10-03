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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;

import java.io.IOException;
import java.util.UUID;

/**
 * Comparator for {@link UUID}.
 *
 * <p>Ordering follows the canonical unsigned big-endian byte comparison of the 16-byte encoding, so
 * every 128-bit value sorts by its raw bytes. This deliberately differs from {@link
 * UUID#compareTo(UUID)}, which compares the two 64-bit halves as signed longs; the unsigned order
 * is the one defined by the type and used consistently across serialized, normalized-key, and
 * object comparisons.
 */
@Internal
public final class UuidComparator extends BasicTypeComparator<UUID> {

    private static final long serialVersionUID = 1L;

    private transient UUID reference;

    public UuidComparator(boolean ascending) {
        super(ascending);
    }

    private static int compareUnsigned(UUID first, UUID second) {
        int comp =
                Long.compareUnsigned(
                        first.getMostSignificantBits(), second.getMostSignificantBits());
        if (comp == 0) {
            comp =
                    Long.compareUnsigned(
                            first.getLeastSignificantBits(), second.getLeastSignificantBits());
        }
        return comp;
    }

    @Override
    public void setReference(UUID toCompare) {
        super.setReference(toCompare);
        this.reference = toCompare;
    }

    @Override
    public int compareToReference(TypeComparator<UUID> referencedComparator) {
        // Mirror BasicTypeComparator's inverted operand order (referenced vs. this) using the
        // unsigned ordering, so it stays consistent with compare/compareSerialized.
        final int comp =
                compareUnsigned(((UuidComparator) referencedComparator).reference, this.reference);
        return ascendingComparison ? comp : -comp;
    }

    @Override
    public int compare(UUID first, UUID second) {
        final int comp = compareUnsigned(first, second);
        return ascendingComparison ? comp : -comp;
    }

    @Override
    public int compareSerialized(DataInputView firstSource, DataInputView secondSource)
            throws IOException {
        final long lMostSignificantBits = firstSource.readLong();
        final long rMostSignificantBits = secondSource.readLong();
        int comp = Long.compareUnsigned(lMostSignificantBits, rMostSignificantBits);
        if (comp == 0) {
            final long lLeastSignificantBits = firstSource.readLong();
            final long rLeastSignificantBits = secondSource.readLong();
            comp = Long.compareUnsigned(lLeastSignificantBits, rLeastSignificantBits);
        }
        return ascendingComparison ? comp : -comp;
    }

    @Override
    public boolean supportsNormalizedKey() {
        return true;
    }

    @Override
    public int getNormalizeKeyLen() {
        return UuidSerializer.UUID_BYTES;
    }

    @Override
    public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
        return keyBytes < getNormalizeKeyLen();
    }

    @Override
    public void putNormalizedKey(UUID record, MemorySegment target, int offset, int numBytes) {
        // The raw 16-byte big-endian encoding is already order-preserving under the unsigned
        // byte-wise comparison used for normalized keys, so no sign flip is applied.
        final long mostSignificantBits = record.getMostSignificantBits();
        final long leastSignificantBits = record.getLeastSignificantBits();
        if (numBytes >= Long.BYTES) {
            target.putLongBigEndian(offset, mostSignificantBits);
            offset += Long.BYTES;
            numBytes -= Long.BYTES;
            if (numBytes >= Long.BYTES) {
                target.putLongBigEndian(offset, leastSignificantBits);
                offset += Long.BYTES;
                numBytes -= Long.BYTES;
                for (int i = 0; i < numBytes; i++) {
                    target.put(offset + i, (byte) 0);
                }
            } else {
                for (int i = 0; i < numBytes; i++) {
                    target.put(offset + i, (byte) (leastSignificantBits >>> ((7 - i) << 3)));
                }
            }
        } else {
            for (int i = 0; i < numBytes; i++) {
                target.put(offset + i, (byte) (mostSignificantBits >>> ((7 - i) << 3)));
            }
        }
    }

    @Override
    public TypeComparator<UUID> duplicate() {
        return new UuidComparator(ascendingComparison);
    }
}
