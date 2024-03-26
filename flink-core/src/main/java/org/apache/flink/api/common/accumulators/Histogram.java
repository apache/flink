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

package org.apache.flink.api.common.accumulators;

import org.apache.flink.annotation.Public;

import java.util.Map;
import java.util.TreeMap;

/**
 * Histogram accumulator, which builds a histogram in a distributed manner. Implemented as a
 * Integer-&gt;Integer TreeMap, so that the entries are sorted according to the values.
 *
 * <p>This class does not extend to continuous values later, because it makes no attempt to put the
 * data in bins.
 */
@Public
public class Histogram implements Accumulator<Integer, TreeMap<Integer, Integer>> {

    private static final long serialVersionUID = 1L;

    private TreeMap<Integer, Integer> treeMap = new TreeMap<>();

    @Override
    public void add(Integer value) {
        treeMap.merge(value, 1, Integer::sum);
    }

    @Override
    public TreeMap<Integer, Integer> getLocalValue() {
        return this.treeMap;
    }

    @Override
    public void merge(Accumulator<Integer, TreeMap<Integer, Integer>> other) {
        // Merge the values into this map
        for (Map.Entry<Integer, Integer> otherEntry : other.getLocalValue().entrySet()) {
            treeMap.merge(otherEntry.getKey(), otherEntry.getValue(), Integer::sum);
        }
    }

    @Override
    public void resetLocal() {
        this.treeMap.clear();
    }

    @Override
    public String toString() {
        return this.treeMap.toString();
    }

    @Override
    public Histogram clone() {
        Histogram result = new Histogram();
        result.treeMap = new TreeMap<>(treeMap);
        return result;
    }
}
