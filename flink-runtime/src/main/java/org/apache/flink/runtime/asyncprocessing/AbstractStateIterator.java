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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.core.state.InternalStateFuture;
import org.apache.flink.core.state.StateFutureUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A {@link StateIterator} implementation to facilitate async data load of iterator. Each state
 * backend could override this class to maintain more variables in need. Any subclass should
 * implement two methods, {@link #hasNext()} and {@link #nextPayloadForContinuousLoading()}. The
 * philosophy behind this class is to carry some already loaded elements and provide iterating right
 * on the task thread, and load following ones if needed (determined by {@link #hasNext()}) by
 * creating **ANOTHER** iterating request. Thus, later it returns another iterator instance, and we
 * continue to apply the user iteration on that instance. The whole elements will be iterated by
 * recursive call of {@code #onNext()}.
 */
@SuppressWarnings("rawtypes")
public abstract class AbstractStateIterator<T> implements StateIterator<T> {

    /** The state this iterator iterates on. */
    final State originalState;

    /** The request type that create this iterator. */
    final StateRequestType requestType;

    /** The controller that can receive further requests. */
    final StateRequestHandler stateHandler;

    /** The already loaded partial elements. */
    final Collection<T> cache;

    public AbstractStateIterator(
            State originalState,
            StateRequestType requestType,
            StateRequestHandler stateHandler,
            Collection<T> partialResult) {
        this.originalState = originalState;
        this.requestType = requestType;
        this.stateHandler = stateHandler;
        this.cache = partialResult;
    }

    /** Return whether this iterator has more elements to load besides current cache. */
    protected abstract boolean hasNext();

    /**
     * To perform following loading, build and get next payload for the next request. This will put
     * into {@link StateRequest#getPayload()}.
     *
     * @return the packed payload for next loading.
     */
    protected abstract Object nextPayloadForContinuousLoading();

    protected StateRequestType getRequestType() {
        return requestType;
    }

    @SuppressWarnings("unchecked")
    private InternalStateFuture<StateIterator<T>> asyncNextLoad() {
        return stateHandler.handleRequest(
                originalState,
                StateRequestType.ITERATOR_LOADING,
                nextPayloadForContinuousLoading());
    }

    private StateIterator<T> syncNextLoad() {
        return stateHandler.handleRequestSync(
                originalState,
                StateRequestType.ITERATOR_LOADING,
                nextPayloadForContinuousLoading());
    }

    @Override
    public <U> StateFuture<Collection<U>> onNext(Function<T, StateFuture<? extends U>> iterating) {
        // Public interface implementation, this is on task thread.
        // We perform the user code on cache, and create a new request and chain with it.
        if (isEmpty()) {
            return StateFutureUtils.completedFuture(Collections.emptyList());
        }
        Collection<StateFuture<? extends U>> resultFutures = new ArrayList<>();

        for (T item : cache) {
            StateFuture<? extends U> resultFuture = iterating.apply(item);
            if (resultFuture != null) {
                resultFutures.add(resultFuture);
            }
        }
        if (hasNext()) {
            return StateFutureUtils.combineAll(resultFutures)
                    .thenCombine(
                            asyncNextLoad().thenCompose(itr -> itr.onNext(iterating)),
                            (a, b) -> {
                                // TODO optimization: Avoid results copy.
                                Collection<U> result = new ArrayList<>(a.size() + b.size());
                                result.addAll(a);
                                result.addAll(b);
                                return result;
                            });
        } else {
            return StateFutureUtils.combineAll(resultFutures);
        }
    }

    @Override
    public StateFuture<Void> onNext(Consumer<T> iterating) {
        // Public interface implementation, this is on task thread.
        // We perform the user code on cache, and create a new request and chain with it.
        if (isEmpty()) {
            return StateFutureUtils.completedVoidFuture();
        }
        for (T item : cache) {
            iterating.accept(item);
        }
        if (hasNext()) {
            return asyncNextLoad().thenCompose(itr -> itr.onNext(iterating));
        } else {
            return StateFutureUtils.completedVoidFuture();
        }
    }

    public void onNextSync(Consumer<T> iterating) {
        if (isEmpty()) {
            return;
        }
        for (T item : cache) {
            iterating.accept(item);
        }
        if (hasNext()) {
            ((AbstractStateIterator<T>) syncNextLoad()).onNextSync(iterating);
        }
    }

    @Override
    public boolean isEmpty() {
        return (cache == null || cache.isEmpty()) && !hasNext();
    }
}
