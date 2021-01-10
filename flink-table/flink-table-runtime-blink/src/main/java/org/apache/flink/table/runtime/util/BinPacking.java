/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.table.runtime.util;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

/** A bin packing implementation. */
public class BinPacking {
    private BinPacking() {}

    public static <T> List<List<T>> pack(
            Iterable<T> items, Function<T, Long> weightFunc, long targetWeight) {
        List<List<T>> packed = new ArrayList<>();
        Deque<Bin<T>> bins = new LinkedList<>();

        for (T item : items) {
            long weight = weightFunc.apply(item);
            Bin<T> bin = findBin(bins, weight);

            if (bin != null) {
                bin.add(item, weight);
            } else {
                bin = new Bin<>(targetWeight);
                bin.add(item, weight);
                bins.addLast(bin);

                // avoid n * n algorithm complexity
                if (bins.size() > 10) {
                    packed.add(bins.removeFirst().items());
                }
            }
        }

        bins.forEach(bin -> packed.add(bin.items));
        return packed;
    }

    private static <T> Bin<T> findBin(Iterable<Bin<T>> bins, long weight) {
        for (Bin<T> bin : bins) {
            if (bin.canAdd(weight)) {
                return bin;
            }
        }
        return null;
    }

    private static class Bin<T> {
        private final long targetWeight;
        private final List<T> items = new ArrayList<>();
        private long binWeight = 0L;

        Bin(long targetWeight) {
            this.targetWeight = targetWeight;
        }

        List<T> items() {
            return items;
        }

        boolean canAdd(long weight) {
            return binWeight + weight <= targetWeight;
        }

        void add(T item, long weight) {
            this.binWeight += weight;
            items.add(item);
        }
    }
}
