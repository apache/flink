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

import org.apache.flink.table.runtime.util.MurmurHashUtil;

/** Long hash set. */
public class LongHashSet extends OptimizableHashSet {

    private long[] key;

    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;

    public LongHashSet(final int expected, final float f) {
        super(expected, f);
        this.key = new long[this.n + 1];
    }

    public LongHashSet(final int expected) {
        this(expected, DEFAULT_LOAD_FACTOR);
    }

    public LongHashSet() {
        this(DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR);
    }

    public boolean add(final long k) {
        if (k == 0L) {
            if (this.containsZero) {
                return false;
            }

            this.containsZero = true;
        } else {
            long[] key = this.key;
            int pos;
            long curr;
            if ((curr = key[pos = (int) MurmurHashUtil.fmix(k) & this.mask]) != 0L) {
                if (curr == k) {
                    return false;
                }

                while ((curr = key[pos = pos + 1 & this.mask]) != 0L) {
                    if (curr == k) {
                        return false;
                    }
                }
            }

            key[pos] = k;
        }

        if (this.size++ >= this.maxFill) {
            this.rehash(OptimizableHashSet.arraySize(this.size + 1, this.f));
        }

        if (k < min) {
            min = k;
        }
        if (k > max) {
            max = k;
        }
        return true;
    }

    public boolean contains(final long k) {
        if (isDense) {
            return k >= min && k <= max && used[(int) (k - min)];
        } else {
            if (k == 0L) {
                return this.containsZero;
            } else {
                long[] key = this.key;
                long curr;
                int pos;
                if ((curr = key[pos = (int) MurmurHashUtil.fmix(k) & this.mask]) == 0L) {
                    return false;
                } else if (k == curr) {
                    return true;
                } else {
                    while ((curr = key[pos = pos + 1 & this.mask]) != 0L) {
                        if (k == curr) {
                            return true;
                        }
                    }

                    return false;
                }
            }
        }
    }

    private void rehash(final int newN) {
        long[] key = this.key;
        int mask = newN - 1;
        long[] newKey = new long[newN + 1];
        int i = this.n;

        int pos;
        for (int j = this.realSize(); j-- != 0; newKey[pos] = key[i]) {
            do {
                --i;
            } while (key[i] == 0L);

            if (newKey[pos = (int) MurmurHashUtil.fmix(key[i]) & mask] != 0L) {
                while (newKey[pos = pos + 1 & mask] != 0L) {}
            }
        }

        this.n = newN;
        this.mask = mask;
        this.maxFill = OptimizableHashSet.maxFill(this.n, this.f);
        this.key = newKey;
    }

    @Override
    public void optimize() {
        long range = max - min;
        if (range >= 0 && (range < key.length || range < OptimizableHashSet.DENSE_THRESHOLD)) {
            this.used = new boolean[(int) (max - min + 1)];
            for (long v : key) {
                if (v != 0) {
                    used[(int) (v - min)] = true;
                }
            }
            if (containsZero) {
                used[(int) (-min)] = true;
            }
            isDense = true;
            key = null;
        }
    }
}
