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

package org.apache.flink.core.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/** A {@link StateFuture} that has already been completed when it is created. */
@Internal
public class CompletedStateFuture<T> implements InternalStateFuture<T> {

    T result;

    // no public access
    CompletedStateFuture(T result) {
        this.result = result;
    }

    @Override
    public <U> StateFuture<U> thenApply(Function<? super T, ? extends U> fn) {
        return StateFutureUtils.completedFuture(fn.apply(result));
    }

    @Override
    public StateFuture<Void> thenAccept(Consumer<? super T> action) {
        action.accept(result);
        return StateFutureUtils.completedVoidFuture();
    }

    @Override
    public <U> StateFuture<U> thenCompose(Function<? super T, ? extends StateFuture<U>> action) {
        return action.apply(result);
    }

    @Override
    public <U, V> StateFuture<V> thenCombine(
            StateFuture<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
        return other.thenCompose(
                (u) -> {
                    try {
                        V v = fn.apply(result, u);
                        return StateFutureUtils.completedFuture(v);
                    } catch (Throwable e) {
                        throw new FlinkRuntimeException("Error binding or executing callback", e);
                    }
                });
    }

    @Override
    public void complete(T result) {
        throw new UnsupportedOperationException("This state future has already been completed.");
    }

    @Override
    public void thenSyncAccept(Consumer<? super T> action) {
        action.accept(result);
    }
}
