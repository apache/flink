/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.hashmap;

import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;

// the semantics is different from state snapshots, i.e. not CoW, because removals can be erased
// from memory once snapshotted
interface RemovalLog<K, N> {

    void startNewVersion(int newVersion);

    void added(K k, N n);

    void removed(K k, N n);

    void confirmed(int version);

    NavigableMap<Integer, Set<StateEntryRemoval<K, N>>> snapshot();

    class StateEntryRemoval<K, N> {
        private final K key;
        private final N namespace;

        public StateEntryRemoval(K key, N namespace) {
            this.key = key;
            this.namespace = namespace;
        }

        public static <K, N> StateEntryRemoval<K, N> of(K k, N n) {
            return new StateEntryRemoval<>(k, n);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof StateEntryRemoval)) {
                return false;
            }
            StateEntryRemoval<?, ?> that = (StateEntryRemoval<?, ?>) o;
            return Objects.equals(key, that.key) && Objects.equals(namespace, that.namespace);
        }

        @Override
        public int hashCode() {
            return Objects.hash(key, namespace);
        }

        public K getKey() {
            return key;
        }

        public N getNamespace() {
            return namespace;
        }
    }
}
