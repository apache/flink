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

package org.apache.flink.runtime.executiongraph;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/** Common utils for processing vertex groups. */
public final class VertexGroupComputeUtil {

    public static <V> Set<V> mergeVertexGroups(
            final Set<V> group1, final Set<V> group2, final Map<V, Set<V>> vertexToGroup) {

        // merge the smaller group into the larger one to reduce the cost
        final Set<V> smallerSet;
        final Set<V> largerSet;
        if (group1.size() < group2.size()) {
            smallerSet = group1;
            largerSet = group2;
        } else {
            smallerSet = group2;
            largerSet = group1;
        }
        for (V v : smallerSet) {
            vertexToGroup.put(v, largerSet);
        }
        largerSet.addAll(smallerSet);
        return largerSet;
    }

    public static <V> Set<Set<V>> uniqueVertexGroups(final Map<V, Set<V>> vertexToGroup) {
        final Set<Set<V>> distinctGroups = Collections.newSetFromMap(new IdentityHashMap<>());
        distinctGroups.addAll(vertexToGroup.values());
        return distinctGroups;
    }

    private VertexGroupComputeUtil() {}
}
