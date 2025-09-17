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

package org.apache.flink.types.bitmap;

import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nullable;

/**
 * A compressed data structure for storing sets of 32-bit integers.
 *
 * <p>The modifying methods in this interface modify the bitmap in place by default. Consider using
 * {@link Bitmap#from(Bitmap other)} to create a copied bitmap before modification if immutability
 * is required.
 */
@PublicEvolving
public interface Bitmap {

    /** Adds the value to the bitmap. */
    void add(int value);

    /**
     * Adds all integers in [rangeStart,rangeEnd) to the bitmap.
     *
     * @param rangeStart valid range [0L, 0xFFFFFFFFL]
     * @param rangeEnd valid range [0L, 0xFFFFFFFFL + 1]
     */
    void add(long rangeStart, long rangeEnd);

    /** Adds the first n elements of the specified array starting at the specified offset. */
    void addN(int[] values, int offset, int n);

    /**
     * Performs an in-place logical AND (intersection) operation with another bitmap.
     *
     * <p>Does nothing if {@code other} is null.
     */
    void and(@Nullable Bitmap other);

    /**
     * Performs an in-place logical AND-NOT (difference) operation with another bitmap, which is
     * equivalent to {@code this AND (NOT other)}.
     *
     * <p>Does nothing if {@code other} is null.
     */
    void andNot(@Nullable Bitmap other);

    /** Resets to an empty bitmap. */
    void clear();

    /** Checks whether the value appears in the bitmap. */
    boolean contains(int value);

    /** Gets the number of distinct values in the bitmap. */
    int getCardinality();

    /** Gets the number of distinct values in the bitmap. This returns a full 64-bit result. */
    long getLongCardinality();

    /** Checks whether the bitmap is empty. */
    boolean isEmpty();

    /**
     * Performs an in-place logical OR (union) operation with another bitmap.
     *
     * <p>Does nothing if {@code other} is null.
     */
    void or(@Nullable Bitmap other);

    /** Removes the value from the bitmap. */
    void remove(int value);

    /**
     * Converts the bitmap to an array of 32-bit integers, the values are sorted by {@link
     * Integer#compareUnsigned}. Avoid calling this method if the bitmap is too large.
     */
    int[] toArray();

    /**
     * Converts the bitmap to an array of bytes.
     *
     * <p>Following the format defined in <a
     * href="https://github.com/RoaringBitmap/RoaringFormatSpec">32-bit RoaringBitmap format
     * specification</a>.
     */
    byte[] toBytes();

    /**
     * Converts the bitmap to a string, the values are sorted by {@link Integer#compareUnsigned}.
     * The string will be truncated and end with "..." if it is too long.
     *
     * <p>For example:
     *
     * <ul>
     *   <li>{@code "{}"}, {@code "{1,2,3,4,5}"}
     *   <li>Negative values (converted to unsigned): {@code "{0,1,4294967294,4294967295}"}
     *   <li>String too long: {@code "{1,2,3,...}"}
     * </ul>
     */
    String toString();

    /**
     * Performs an in-place logical XOR (symmetric difference) operation with another bitmap.
     *
     * <p>Does nothing if {@code other} is null.
     */
    void xor(@Nullable Bitmap other);

    // ~ Static Methods --------------------------------------------------------------

    /** Gets an empty bitmap. */
    static Bitmap empty() {
        return RoaringBitmapData.empty();
    }

    /** Gets a copied bitmap. Returns null if {@code other} is null. */
    static Bitmap from(Bitmap other) {
        if (other == null) {
            return null;
        }
        return RoaringBitmapData.from(other);
    }

    /**
     * Gets a bitmap from an array of bytes. Returns null if {@code bytes} is null.
     *
     * <p>Following the format defined in <a
     * href="https://github.com/RoaringBitmap/RoaringFormatSpec">32-bit RoaringBitmap format
     * specification</a>.
     *
     * @throws org.apache.flink.types.DeserializationException if failed to deserialize bitmap from
     *     bytes
     */
    static Bitmap fromBytes(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return RoaringBitmapData.fromBytes(bytes);
    }

    /** Gets a bitmap from an array of values. Returns null if {@code values} is null. */
    static Bitmap fromArray(int[] values) {
        if (values == null) {
            return null;
        }
        return RoaringBitmapData.fromArray(values);
    }
}
