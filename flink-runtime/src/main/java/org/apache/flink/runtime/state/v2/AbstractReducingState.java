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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.v2.ReducingState;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.v2.internal.InternalReducingState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * A default implementation of {@link ReducingState} which delegates all async requests to {@link
 * StateRequestHandler}.
 *
 * @param <K> The type of key the state is associated to.
 * @param <V> The type of values kept internally in state.
 */
public class AbstractReducingState<K, N, V> extends AbstractKeyedState<K, N, V>
        implements InternalReducingState<K, N, V> {

    protected final ReduceFunction<V> reduceFunction;

    public AbstractReducingState(
            StateRequestHandler stateRequestHandler, ReducingStateDescriptor<V> stateDescriptor) {
        super(stateRequestHandler, stateDescriptor);
        this.reduceFunction = stateDescriptor.getReduceFunction();
    }

    @Override
    public StateFuture<V> asyncGet() {
        return asyncGetInternal();
    }

    @Override
    public StateFuture<Void> asyncAdd(V value) {
        return asyncGetInternal()
                .thenAccept(
                        oldValue -> {
                            V newValue =
                                    oldValue == null
                                            ? value
                                            : reduceFunction.reduce((V) oldValue, value);
                            asyncUpdateInternal(newValue);
                        });
    }

    @Override
    public V get() {
        return getInternal();
    }

    @Override
    public void add(V value) {
        V oldValue = getInternal();
        try {
            V newValue = oldValue == null ? value : reduceFunction.reduce(oldValue, value);
            updateInternal(newValue);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public StateFuture<Void> asyncMergeNamespaces(N target, Collection<N> sources) {
        if (sources == null || sources.isEmpty()) {
            return StateFutureUtils.completedVoidFuture();
        }
        // phase 1: read from the sources and target
        List<StateFuture<V>> futures = new ArrayList<>(sources.size() + 1);
        for (N source : sources) {
            if (source != null) {
                setCurrentNamespace(source);
                futures.add(asyncGetInternal());
            }
        }
        setCurrentNamespace(target);
        futures.add(asyncGetInternal());
        // phase 2: merge the sources to the target
        return StateFutureUtils.combineAll(futures)
                .thenCompose(
                        values -> {
                            List<StateFuture<Void>> updateFutures =
                                    new ArrayList<>(sources.size() + 1);
                            V current = null;
                            Iterator<V> valueIterator = values.iterator();
                            for (N source : sources) {
                                V value = valueIterator.next();
                                if (value != null) {
                                    setCurrentNamespace(source);
                                    updateFutures.add(asyncUpdateInternal(null));
                                    if (current != null) {
                                        current = reduceFunction.reduce(current, value);
                                    } else {
                                        current = value;
                                    }
                                }
                            }
                            V targetValue = valueIterator.next();
                            if (current != null) {
                                if (targetValue != null) {
                                    current = reduceFunction.reduce(current, targetValue);
                                }
                                setCurrentNamespace(target);
                                updateFutures.add(asyncUpdateInternal(current));
                            }
                            return StateFutureUtils.combineAll(updateFutures)
                                    .thenAccept(ignores -> {});
                        });
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) {
        if (sources == null || sources.isEmpty()) {
            return;
        }
        try {
            V current = null;
            // merge the sources to the target
            for (N source : sources) {
                if (source != null) {
                    setCurrentNamespace(source);
                    V oldValue = getInternal();

                    if (oldValue != null) {
                        updateInternal(null);

                        if (current != null) {
                            current = reduceFunction.reduce(current, oldValue);
                        } else {
                            current = oldValue;
                        }
                    }
                }
            }

            // if something came out of merging the sources, merge it or write it to the target
            if (current != null) {
                // create the target full-binary-key
                setCurrentNamespace(target);
                V targetValue = getInternal();

                if (targetValue != null) {
                    current = reduceFunction.reduce(current, targetValue);
                }
                updateInternal(current);
            }
        } catch (Exception e) {
            throw new RuntimeException("merge namespace fail.", e);
        }
    }

    @Override
    public StateFuture<V> asyncGetInternal() {
        return handleRequest(StateRequestType.REDUCING_GET, null);
    }

    @Override
    public StateFuture<Void> asyncUpdateInternal(V valueToStore) {
        return handleRequest(StateRequestType.REDUCING_ADD, valueToStore);
    }

    @Override
    public V getInternal() {
        return handleRequestSync(StateRequestType.REDUCING_GET, null);
    }

    @Override
    public void updateInternal(V valueToStore) {
        handleRequestSync(StateRequestType.REDUCING_ADD, valueToStore);
    }
}
