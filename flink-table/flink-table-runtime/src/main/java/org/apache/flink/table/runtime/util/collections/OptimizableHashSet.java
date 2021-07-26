/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.util.collections;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A type-specific hash set with with a fast, small-footprint implementation. Refer to the
 * implementation of fastutil.
 *
 * <p>Instances of this class use a hash table to represent a set. The table is enlarged as needed
 * by doubling its size when new entries are created.
 *
 * <p>The difference with fastutil is that if the range of the maximum and minimum values is small
 * or the data is dense, a Dense array will be used to greatly improve the access speed.
 */
public abstract class OptimizableHashSet {

    /** The initial default size of a hash table. */
    public static final int DEFAULT_INITIAL_SIZE = 16;

    /** The default load factor of a hash table. */
    public static final float DEFAULT_LOAD_FACTOR = 0.75f;

    /**
     * Decide whether to convert to dense mode if it does not require more memory or could fit
     * within L1 cache.
     */
    public static final int DENSE_THRESHOLD = 8192;

    /** The acceptable load factor. */
    protected final float f;

    /** The mask for wrapping a position counter. */
    protected int mask;

    /** The current table size. */
    protected int n;

    /** Threshold after which we rehash. It must be the table size times {@link #f}. */
    protected int maxFill;

    /** Is this set has a null key. */
    protected boolean containsNull;

    /** Is this set has a zero key. */
    protected boolean containsZero;

    /** Number of entries in the set. */
    protected int size;

    /** Is now dense mode. */
    protected boolean isDense = false;

    /** Used array for dense mode. */
    protected boolean[] used;

    public OptimizableHashSet(final int expected, final float f) {
        checkArgument(f > 0 && f <= 1);
        checkArgument(expected >= 0);
        this.f = f;
        this.n = OptimizableHashSet.arraySize(expected, f);
        this.mask = this.n - 1;
        this.maxFill = OptimizableHashSet.maxFill(this.n, f);
    }

    /** Add a null key. */
    public void addNull() {
        this.containsNull = true;
    }

    /** Is there a null key. */
    public boolean containsNull() {
        return containsNull;
    }

    protected int realSize() {
        return this.containsZero ? this.size - 1 : this.size;
    }

    /** Decide whether to convert to dense mode. */
    public abstract void optimize();

    /**
     * Return the least power of two greater than or equal to the specified value.
     *
     * <p>Note that this function will return 1 when the argument is 0.
     *
     * @param x a long integer smaller than or equal to 2<sup>62</sup>.
     * @return the least power of two greater than or equal to the specified value.
     */
    public static long nextPowerOfTwo(long x) {
        if (x == 0L) {
            return 1L;
        } else {
            --x;
            x |= x >> 1;
            x |= x >> 2;
            x |= x >> 4;
            x |= x >> 8;
            x |= x >> 16;
            return (x | x >> 32) + 1L;
        }
    }

    /**
     * Returns the maximum number of entries that can be filled before rehashing.
     *
     * @param n the size of the backing array.
     * @param f the load factor.
     * @return the maximum number of entries before rehashing.
     */
    public static int maxFill(int n, float f) {
        return Math.min((int) Math.ceil((double) ((float) n * f)), n - 1);
    }

    /**
     * Returns the least power of two smaller than or equal to 2<sup>30</sup> and larger than or
     * equal to <code>Math.ceil( expected / f )</code>.
     *
     * @param expected the expected number of elements in a hash table.
     * @param f the load factor.
     * @return the minimum possible size for a backing array.
     * @throws IllegalArgumentException if the necessary size is larger than 2<sup>30</sup>.
     */
    public static int arraySize(int expected, float f) {
        long s = Math.max(2L, nextPowerOfTwo((long) Math.ceil((double) ((float) expected / f))));
        if (s > (Integer.MAX_VALUE / 2 + 1)) {
            throw new IllegalArgumentException(
                    "Too large (" + expected + " expected elements with load factor " + f + ")");
        } else {
            return (int) s;
        }
    }
}
