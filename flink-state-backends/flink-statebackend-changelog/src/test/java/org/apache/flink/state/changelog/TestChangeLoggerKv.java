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

package org.apache.flink.state.changelog;

import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.function.ThrowingConsumer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

class TestChangeLoggerKv<State> implements KvStateChangeLogger<State, String> {
    boolean stateUpdated;
    boolean stateUpdatedInternal;
    boolean stateAdded;
    boolean stateCleared;
    boolean stateElementAdded;
    boolean stateElementChanged;
    boolean stateElementRemoved;
    boolean stateMerged;
    final BiFunction<State, State, State> stateAggregator;
    State state;

    public static <T> TestChangeLoggerKv<List<T>> forList(List<T> data) {
        return new TestChangeLoggerKv<>(
                data,
                (a, b) -> {
                    ArrayList<T> c = new ArrayList<>();
                    if (a != null) {
                        c.addAll(a);
                    }
                    if (b != null) {
                        c.addAll(b);
                    }
                    return c;
                });
    }

    public static <K, V> TestChangeLoggerKv<Map<K, V>> forMap(Map<K, V> data) {
        return new TestChangeLoggerKv<>(
                data,
                (a, b) -> {
                    HashMap<K, V> c = new HashMap<>();
                    if (a != null) {
                        c.putAll(a);
                    }
                    if (b != null) {
                        c.putAll(b);
                    }
                    return c;
                });
    }

    TestChangeLoggerKv(State initState, BiFunction<State, State, State> stateAggregator) {
        this.stateAggregator = stateAggregator;
        this.state = initState;
    }

    @Override
    public void valueUpdated(State newState, String ns) {
        stateUpdated = true;
        state = newState;
    }

    @Override
    public void valueUpdatedInternal(State newState, String ns) {
        stateUpdatedInternal = true;
        state = newState;
    }

    @Override
    public void valueAdded(State addedState, String ns) {
        stateAdded = true;
        state = stateAggregator.apply(state, addedState);
    }

    @Override
    public void valueCleared(String ns) {
        stateCleared = true;
        state = null;
    }

    @Override
    public void valueElementAdded(
            ThrowingConsumer<DataOutputView, IOException> dataSerializer, String ns) {
        stateElementAdded = true;
    }

    @Override
    public void valueElementAddedOrUpdated(
            ThrowingConsumer<DataOutputView, IOException> dataSerializer, String ns) {
        stateElementChanged = true;
    }

    @Override
    public void valueElementRemoved(
            ThrowingConsumer<DataOutputView, IOException> dataSerializer, String ns) {
        stateElementRemoved = true;
    }

    @Override
    public void resetWritingMetaFlag() {}

    @Override
    public void namespacesMerged(String target, Collection<String> sources) {
        stateMerged = true;
    }

    public boolean anythingChanged() {
        return stateUpdated
                || stateUpdatedInternal
                || stateAdded
                || stateCleared
                || stateElementChanged
                || stateElementRemoved
                || stateMerged;
    }

    @Override
    public void close() {}
}
