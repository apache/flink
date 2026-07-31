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

package org.apache.flink.state.api.filter;

import java.util.HashSet;
import java.util.Set;

/** A filter that accepts a finite set of keys. */
final class ExactKeyFilter<K> implements SavepointKeyFilter<K> {

    private static final long serialVersionUID = 2L;

    private final Set<K> keys;

    ExactKeyFilter(Set<K> keys) {
        this.keys = Set.copyOf(keys);
    }

    @Override
    public boolean test(K key) {
        return keys.contains(key);
    }

    @Override
    public Set<K> getExactKeys() {
        return keys;
    }

    @Override
    public SavepointKeyFilter<K> intersect(SavepointKeyFilter<K> other) {
        if (other.isEmpty()) {
            return other;
        }
        final Set<K> otherKeys = other.getExactKeys();
        if (otherKeys != null) {
            final Set<K> intersection = new HashSet<>(keys);
            intersection.retainAll(otherKeys);
            return SavepointKeyFilter.exact(intersection);
        }
        return SavepointKeyFilter.filterKeys(keys, other);
    }

    @Override
    public String toString() {
        return "ExactKeyFilter" + keys;
    }
}
