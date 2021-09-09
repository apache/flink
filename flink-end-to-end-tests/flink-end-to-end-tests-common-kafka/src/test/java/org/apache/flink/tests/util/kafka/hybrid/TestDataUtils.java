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

package org.apache.flink.tests.util.kafka.hybrid;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TestDataUtils {

    /**
     * Divides the collection into two parts based on the proportion between the part's sizes.
     *
     * @param input collection to be divided.
     * @param fraction indicating which portion of the input elements should be placed into the
     *     first part of the division. The second part will be assigned the remaining elements (1 -
     *     {@code fraction}).
     * @param <T> type of the collection.
     * @return resulting parts of the division stored separately at index 0 and 1 of the returned
     *     list.
     */
    public static <T> List<Collection<T>> divide(Collection<T> input, double fraction) {
        int divideIndex = divideIndex(input, fraction);
        return slice(input, divideIndex);
    }

    /**
     * Calculates the index of an element in the {@code input} collection that denotes the division
     * according to the specified {@code fraction} relation between the parts.
     *
     * @param input collection to be divided.
     * @param fraction indicating which portion of the input elements should be placed into the
     *     first part of the division. The second part will be assigned the remaining elements
     *     {@code (1 - fraction)}.
     * @param <T> type of the collection.
     * @return the index of the first element in the {@code input} collection that belongs to the
     *     second part of the division.
     */
    public static <T> int divideIndex(Collection<T> input, double fraction) {
        return (int) (input.size() * fraction);
    }

    /**
     * Divides the collection into two parts with the first part containing {@code numRecords}.
     *
     * @param input collection to be divided.
     * @param numRecords indicating how many elements the first part of the division has to contain.
     *     If {@code numRecords} is larger than the size of the input collection, the second part is
     *     going to be empty.
     * @param <T> type of the collection.
     * @return resulting parts of the division stored separately at index 0 and 1 of the returned
     *     list.
     */
    public static <T> List<Collection<T>> slice(Collection<T> input, int numRecords) {
        final List<T> list = new ArrayList<>(input);
        final AtomicInteger counter = new AtomicInteger();

        Map<Boolean, List<T>> map =
                list.stream()
                        .collect(
                                Collectors.partitioningBy(
                                        e -> counter.incrementAndGet() > numRecords));

        return new ArrayList<>(map.values());
    }
}
