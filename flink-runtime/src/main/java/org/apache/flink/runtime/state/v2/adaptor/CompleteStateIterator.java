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

package org.apache.flink.runtime.state.v2.adaptor;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.core.state.InternalStateIterator;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/** A {@link org.apache.flink.api.common.state.v2.StateIterator} that has all elements. */
public class CompleteStateIterator<T> implements InternalStateIterator<T> {

    final Iterable<T> iterable;
    final boolean empty;

    public CompleteStateIterator(@Nullable Iterable<T> iterable) {
        if (iterable == null) {
            this.iterable = Collections.emptyList();
            this.empty = true;
        } else {
            this.iterable = iterable;
            this.empty = !iterable.iterator().hasNext();
        }
    }

    @Override
    public <U> StateFuture<Collection<U>> onNext(
            FunctionWithException<T, StateFuture<? extends U>, Exception> iterating) {
        if (isEmpty()) {
            return StateFutureUtils.completedFuture(Collections.emptyList());
        }
        Collection<StateFuture<? extends U>> resultFutures = new ArrayList<>();

        try {
            for (T item : iterable) {
                resultFutures.add(iterating.apply(item));
            }
        } catch (Exception e) {
            // Since this is on task thread, we can directly throw the runtime exception.
            throw new FlinkRuntimeException("Failed to iterate over state.", e);
        }
        return StateFutureUtils.combineAll(resultFutures);
    }

    @Override
    public StateFuture<Void> onNext(ThrowingConsumer<T, Exception> iterating) {
        try {
            for (T item : iterable) {
                iterating.accept(item);
            }
        } catch (Exception e) {
            // Since this is on task thread, we can directly throw the runtime exception.
            throw new FlinkRuntimeException("Failed to iterate over state.", e);
        }
        return StateFutureUtils.completedVoidFuture();
    }

    @Override
    public boolean isEmpty() {
        return empty;
    }

    @Override
    public boolean hasNextLoading() {
        return false;
    }

    @Override
    public Iterable<T> getCurrentCache() {
        return iterable;
    }
}
