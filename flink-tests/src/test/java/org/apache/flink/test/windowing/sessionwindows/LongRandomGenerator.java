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

package org.apache.flink.test.windowing.sessionwindows;

import java.util.Collection;
import java.util.Random;

/** Provide additional PRNG methods for selecting in a range and for collection choice. */
public class LongRandomGenerator extends Random {

    static final long serialVersionUID = 32523525277L;

    public LongRandomGenerator(long seed) {
        super(seed);
    }

    /**
     * @param minInclusive lower bound for the returned value (inclusive)
     * @param maxExclusive upper bound for the returned value (exclusive)
     * @return random long between the provided min (inclusive) and max (exclusive)
     */
    public long randomLongBetween(long minInclusive, long maxExclusive) {
        if (maxExclusive <= minInclusive) {
            throw new IllegalArgumentException(
                    String.format(
                            "Error: min (found %s) must be < than max (found %s)!",
                            minInclusive, maxExclusive));
        }
        long bits;
        long generatedValue;
        long delta = maxExclusive - minInclusive;
        do {
            bits = (nextLong() << 1) >>> 1;
            generatedValue = bits % delta;
        } while (bits - generatedValue + (delta - 1) < 0L);
        return minInclusive + generatedValue;
    }

    /**
     * @param collection collection to chose from
     * @return selects a valid random index from the collection's range of indices
     */
    public int choseRandomIndex(Collection<?> collection) {
        return nextInt(collection.size());
    }

    /** @return a randomly chosen element from collection */
    public <T> T chooseRandomElement(Collection<T> collection) {
        int choice = choseRandomIndex(collection);
        for (T key : collection) {
            if (choice == 0) {
                return key;
            }
            --choice;
        }
        return null;
    }
}
