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

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.DeserializationException;
import org.apache.flink.util.Preconditions;

import org.roaringbitmap.IntConsumer;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * An internal Bitmap implementation that wraps {@link RoaringBitmap} for Flink-specific
 * modifications.
 */
@Internal
public final class RoaringBitmapData implements Bitmap {

    private final RoaringBitmap roaringBitmap;

    private RoaringBitmapData() {
        this.roaringBitmap = new RoaringBitmap();
    }

    private RoaringBitmapData(RoaringBitmapData other) {
        this.roaringBitmap = other.roaringBitmap.clone();
    }

    private RoaringBitmapData(RoaringBitmap roaringBitmap) {
        this.roaringBitmap = roaringBitmap;
    }

    // ~ Static Methods ----------------------------------------------------------------

    public static RoaringBitmapData empty() {
        return new RoaringBitmapData();
    }

    public static RoaringBitmapData from(@Nonnull Bitmap other) {
        return new RoaringBitmapData(toRoaringBitmapData(other));
    }

    public static RoaringBitmapData fromBytes(@Nonnull byte[] bytes)
            throws DeserializationException {
        RoaringBitmapData rb32 = new RoaringBitmapData();
        try {
            rb32.roaringBitmap.deserialize(ByteBuffer.wrap(bytes));
        } catch (Exception e) {
            throw new DeserializationException("Failed to deserialize bitmap from bytes.", e);
        }
        return rb32;
    }

    public static RoaringBitmapData fromArray(@Nonnull int[] values) {
        RoaringBitmapData rb32 = new RoaringBitmapData();
        rb32.roaringBitmap.add(values);
        return rb32;
    }

    /**
     * Wraps the given {@link RoaringBitmap} without copying. The returned {@link RoaringBitmapData}
     * shares the same internal object as the input.
     */
    public static RoaringBitmapData wrap(@Nonnull RoaringBitmap roaringBitmap) {
        Preconditions.checkNotNull(roaringBitmap);
        return new RoaringBitmapData(roaringBitmap);
    }

    private static RoaringBitmapData toRoaringBitmapData(Bitmap bm)
            throws IllegalArgumentException {
        if (!(bm instanceof RoaringBitmapData)) {
            throw new IllegalArgumentException("Unsupported bitmap type: " + bm.getClass() + ".");
        }
        return (RoaringBitmapData) bm;
    }

    // ~ Bitmap Interface Implementations ------------------------------------------------

    @Override
    public void add(int value) {
        roaringBitmap.add(value);
    }

    /**
     * @throws IllegalArgumentException if rangeStart or rangeEnd is out of range.
     */
    @Override
    public void add(long rangeStart, long rangeEnd) throws IllegalArgumentException {
        roaringBitmap.add(rangeStart, rangeEnd);
    }

    /**
     * @throws NullPointerException if values is null.
     * @throws IllegalArgumentException if offset or n is negative, or offset + n is greater than
     *     values.length.
     */
    @Override
    public void addN(int[] values, int offset, int n)
            throws NullPointerException, IllegalArgumentException {
        roaringBitmap.addN(values, offset, n);
    }

    @Override
    public void and(@Nullable Bitmap other) {
        if (other == null) {
            return;
        }
        roaringBitmap.and(toRoaringBitmapData(other).roaringBitmap);
    }

    @Override
    public void andNot(@Nullable Bitmap other) {
        if (other == null) {
            return;
        }
        roaringBitmap.andNot(toRoaringBitmapData(other).roaringBitmap);
    }

    @Override
    public void clear() {
        roaringBitmap.clear();
    }

    @Override
    public boolean contains(int value) {
        return roaringBitmap.contains(value);
    }

    @Override
    public int getCardinality() {
        return roaringBitmap.getCardinality();
    }

    @Override
    public long getLongCardinality() {
        return roaringBitmap.getLongCardinality();
    }

    @Override
    public boolean isEmpty() {
        return roaringBitmap.isEmpty();
    }

    @Override
    public void or(@Nullable Bitmap other) {
        if (other == null) {
            return;
        }
        roaringBitmap.or(toRoaringBitmapData(other).roaringBitmap);
    }

    @Override
    public void remove(int value) {
        roaringBitmap.remove(value);
    }

    /**
     * @throws RuntimeException if the bitmap is too large.
     */
    @Override
    public int[] toArray() throws RuntimeException {
        return roaringBitmap.toArray();
    }

    @Override
    public byte[] toBytes() {
        roaringBitmap.runOptimize();
        ByteBuffer buffer = ByteBuffer.allocate(roaringBitmap.serializedSizeInBytes());
        roaringBitmap.serialize(buffer);
        return buffer.array();
    }

    @Override
    public String toString() {
        return roaringBitmap.toString();
    }

    @Override
    public void xor(@Nullable Bitmap other) {
        if (other == null) {
            return;
        }
        roaringBitmap.xor(toRoaringBitmapData(other).roaringBitmap);
    }

    // ~ Non-interface Methods ----------------------------------------------------------

    /** Calls the specified consumer for each value in the bitmap. */
    public void forEach(IntConsumer consumer) {
        roaringBitmap.forEach(consumer);
    }

    // ~ Object Overrides ---------------------------------------------------------------

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RoaringBitmapData)) {
            return false;
        }
        RoaringBitmapData other = (RoaringBitmapData) obj;
        return Objects.equals(roaringBitmap, other.roaringBitmap);
    }

    @Override
    public int hashCode() {
        // RoaringBitmap#hashCode() requires both bitmaps to have the same
        // runOptimize() state to produce consistent hash codes for equal bitmaps.
        // Calling runOptimize() here is a semantically safe side effect that only
        // changes the internal container encoding without altering the bitmap's content.
        roaringBitmap.runOptimize();
        return roaringBitmap.hashCode();
    }
}
