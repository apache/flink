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

package org.apache.flink.runtime.state.testutils;

import org.apache.flink.runtime.state.StateEntry;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.Objects;

/** A matcher for {@link StateEntry StateEntries}. */
public class StateEntryMatcher<K, N, S> extends TypeSafeMatcher<StateEntry<K, N, S>> {
    private final K key;
    private final N namespace;
    private final S state;

    StateEntryMatcher(K key, N namespace, S state) {
        this.key = key;
        this.namespace = namespace;
        this.state = state;
    }

    public static <K, N, S> StateEntryMatcher<K, N, S> entry(K key, N namespace, S state) {
        return new StateEntryMatcher<>(key, namespace, state);
    }

    @Override
    protected boolean matchesSafely(StateEntry<K, N, S> item) {
        return Objects.equals(item.getKey(), key)
                && Objects.equals(item.getNamespace(), namespace)
                && Objects.equals(item.getState(), state);
    }

    @Override
    public void describeTo(Description description) {
        description.appendText(
                String.format(
                        "expected entry: key: %s, namespace: %s, state: %s",
                        key, namespace, state));
    }
}
