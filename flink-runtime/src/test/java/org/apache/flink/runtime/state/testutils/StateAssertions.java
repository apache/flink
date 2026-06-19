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

package org.apache.flink.runtime.state.testutils;

import org.apache.flink.runtime.state.StateEntry;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** State-related assertions. */
public final class StateAssertions {

    public static <K, N, S> void assertContainsExactly(
            Iterator<StateEntry<K, N, S>> stateEntries, Map<N, Map<K, S>> expected) {

        final List<StateEntryWithEquals<K, N, S>> expectedEntries =
                expected.entrySet().stream()
                        .flatMap(
                                entry ->
                                        entry.getValue().entrySet().stream()
                                                .map(
                                                        ksEntry ->
                                                                new StateEntryWithEquals<>(
                                                                        entry.getKey(),
                                                                        ksEntry.getKey(),
                                                                        ksEntry.getValue())))
                        .collect(Collectors.toList());

        assertThat(stateEntries)
                .toIterable()
                .map(StateEntryWithEquals::new)
                .containsExactlyInAnyOrderElementsOf(expectedEntries);
    }

    private static class StateEntryWithEquals<K, N, S> implements StateEntry<K, N, S> {

        private final N namespace;
        private final K key;
        private final S state;

        private StateEntryWithEquals(StateEntry<K, N, S> entry) {
            this(entry.getNamespace(), entry.getKey(), entry.getState());
        }

        private StateEntryWithEquals(N namespace, K key, S state) {
            this.namespace = namespace;
            this.key = key;
            this.state = state;
        }

        @Override
        public K getKey() {
            return key;
        }

        @Override
        public N getNamespace() {
            return namespace;
        }

        @Override
        public S getState() {
            return state;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            StateEntryWithEquals<?, ?, ?> that = (StateEntryWithEquals<?, ?, ?>) o;
            return Objects.equals(namespace, that.namespace)
                    && Objects.equals(key, that.key)
                    && Objects.equals(state, that.state);
        }

        @Override
        public int hashCode() {
            return Objects.hash(namespace, key, state);
        }
    }

    private StateAssertions() {}
}
