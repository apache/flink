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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.v2.AggregatingState;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.v2.internal.InternalAggregatingState;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * The default implementation of {@link AggregatingState}, which delegates all async requests to
 * {@link StateRequestHandler}.
 *
 * @param <K> The type of key the state is associated to.
 * @param <IN> The type of the values that are added into the state.
 * @param <ACC> TThe type of the accumulator (intermediate aggregation state).
 * @param <OUT> The type of the values that are returned from the state.
 */
public class AbstractAggregatingState<K, N, IN, ACC, OUT> extends AbstractKeyedState<K, N, ACC>
        implements InternalAggregatingState<K, N, IN, ACC, OUT> {

    protected final AggregateFunction<IN, ACC, OUT> aggregateFunction;

    /**
     * Creates a new AbstractKeyedState with the given asyncExecutionController and stateDescriptor.
     *
     * @param stateRequestHandler The async request handler for handling all requests.
     * @param stateDescriptor The properties of the state.
     */
    public AbstractAggregatingState(
            StateRequestHandler stateRequestHandler,
            AggregatingStateDescriptor<IN, ACC, OUT> stateDescriptor) {
        super(stateRequestHandler, stateDescriptor);
        this.aggregateFunction = stateDescriptor.getAggregateFunction();
    }

    @Override
    public StateFuture<OUT> asyncGet() {
        return asyncGetInternal()
                .thenApply(acc -> (acc == null) ? null : this.aggregateFunction.getResult(acc));
    }

    @Override
    public StateFuture<Void> asyncAdd(IN value) {
        return asyncGetInternal()
                .thenAccept(
                        acc -> {
                            final ACC safeAcc =
                                    (acc == null)
                                            ? this.aggregateFunction.createAccumulator()
                                            : acc;
                            asyncUpdateInternal(this.aggregateFunction.add(value, safeAcc));
                        });
    }

    @Override
    public StateFuture<ACC> asyncGetInternal() {
        return handleRequest(StateRequestType.AGGREGATING_GET, null);
    }

    @Override
    public StateFuture<Void> asyncUpdateInternal(ACC valueToStore) {
        return handleRequest(StateRequestType.AGGREGATING_ADD, valueToStore);
    }

    @Override
    public OUT get() {
        ACC acc = getInternal();
        return acc == null ? null : this.aggregateFunction.getResult(acc);
    }

    @Override
    public void add(IN value) {
        ACC acc = getInternal();
        try {
            ACC newValue =
                    acc == null
                            ? this.aggregateFunction.createAccumulator()
                            : this.aggregateFunction.add(value, acc);
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
        List<StateFuture<ACC>> futures = new ArrayList<>(sources.size() + 1);
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
                            ACC current = null;
                            Iterator<ACC> valueIterator = values.iterator();
                            for (N source : sources) {
                                ACC value = valueIterator.next();
                                if (value != null) {
                                    setCurrentNamespace(source);
                                    updateFutures.add(asyncUpdateInternal(null));
                                    if (current == null) {
                                        current = value;
                                    } else {
                                        current = aggregateFunction.merge(current, value);
                                    }
                                }
                            }
                            ACC targetValue = valueIterator.next();
                            if (current != null) {
                                if (targetValue != null) {
                                    current = aggregateFunction.merge(current, targetValue);
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
            ACC current = null;
            // merge the sources to the target
            for (N source : sources) {
                if (source != null) {
                    setCurrentNamespace(source);
                    ACC oldValue = handleRequestSync(StateRequestType.AGGREGATING_GET, null);

                    if (oldValue != null) {
                        handleRequestSync(StateRequestType.AGGREGATING_ADD, null);

                        if (current != null) {
                            current = aggregateFunction.merge(current, oldValue);
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
                ACC targetValue = handleRequestSync(StateRequestType.AGGREGATING_GET, null);

                if (targetValue != null) {
                    current = aggregateFunction.merge(current, targetValue);
                }
                handleRequestSync(StateRequestType.AGGREGATING_ADD, current);
            }
        } catch (Exception e) {
            throw new RuntimeException("merge namespace fail.", e);
        }
    }

    @Override
    public ACC getInternal() {
        return handleRequestSync(StateRequestType.AGGREGATING_GET, null);
    }

    @Override
    public void updateInternal(ACC valueToStore) {
        handleRequestSync(StateRequestType.AGGREGATING_ADD, valueToStore);
    }
}
