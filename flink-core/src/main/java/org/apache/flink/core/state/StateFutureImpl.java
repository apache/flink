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

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The default implementation of {@link StateFuture}. This is managed by the runtime framework and
 * should never be directly created in user code. It will handle the completion and callback
 * trigger, and most of the design are borrowed from the {@link CompletableFuture}. In the basic
 * version of this implementation, we wrap {@link CompletableFuture} for simplification. TODO:
 * remove CompletableFuture.
 *
 * <p>This class is on hot path and very complex, please take care of the performance as well as the
 * running thread of each block when you decide to touch it.
 */
@Internal
public class StateFutureImpl<T> implements InternalStateFuture<T> {

    /** The future holds the result. The completes in async threads. */
    CompletableFuture<T> completableFuture;

    /** The callback runner. */
    CallbackRunner callbackRunner;

    StateFutureImpl(CallbackRunner callbackRunner) {
        this.completableFuture = new CompletableFuture<>();
        this.callbackRunner = callbackRunner;
    }

    @Override
    public <U> StateFuture<U> thenApply(Function<? super T, ? extends U> fn) {
        try {
            if (completableFuture.isDone()) {
                U r = fn.apply(completableFuture.get());
                return StateFutureUtils.completedFuture(r);
            } else {
                StateFutureImpl<U> ret = new StateFutureImpl<>(callbackRunner);
                completableFuture.thenAccept(
                        (t) -> {
                            callbackRunner.submit(
                                    () -> {
                                        ret.complete(fn.apply(t));
                                    });
                        });
                return ret;
            }
        } catch (Throwable e) {
            throw new FlinkRuntimeException("Error binding or executing callback", e);
        }
    }

    @Override
    public StateFuture<Void> thenAccept(Consumer<? super T> action) {
        try {
            if (completableFuture.isDone()) {
                action.accept(completableFuture.get());
                return StateFutureUtils.completedVoidFuture();
            } else {
                StateFutureImpl<Void> ret = new StateFutureImpl<>(callbackRunner);
                completableFuture.thenAccept(
                        (t) -> {
                            callbackRunner.submit(
                                    () -> {
                                        action.accept(t);
                                        ret.complete(null);
                                    });
                        });
                return ret;
            }
        } catch (Throwable e) {
            throw new FlinkRuntimeException("Error binding or executing callback", e);
        }
    }

    @Override
    public <U> StateFuture<U> thenCompose(Function<? super T, ? extends StateFuture<U>> action) {
        try {
            if (completableFuture.isDone()) {
                return action.apply(completableFuture.get());
            } else {
                StateFutureImpl<U> ret = new StateFutureImpl<>(callbackRunner);
                completableFuture.thenAccept(
                        (t) -> {
                            callbackRunner.submit(
                                    () -> {
                                        StateFuture<U> su = action.apply(t);
                                        su.thenAccept(ret::complete);
                                    });
                        });
                return ret;
            }
        } catch (Throwable e) {
            throw new FlinkRuntimeException("Error binding or executing callback", e);
        }
    }

    @Override
    public <U, V> StateFuture<V> thenCombine(
            StateFuture<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
        try {
            if (completableFuture.isDone()) {
                return other.thenCompose(
                        (u) -> {
                            try {
                                V v = fn.apply(completableFuture.get(), u);
                                return StateFutureUtils.completedFuture(v);
                            } catch (Throwable e) {
                                throw new FlinkRuntimeException(
                                        "Error binding or executing callback", e);
                            }
                        });
            } else {
                StateFutureImpl<V> ret = new StateFutureImpl<>(callbackRunner);
                ((InternalStateFuture<? extends U>) other)
                        .thenSyncAccept(
                                (u) -> {
                                    completableFuture.thenAccept(
                                            (t) -> {
                                                callbackRunner.submit(
                                                        () -> {
                                                            ret.complete(fn.apply(t, u));
                                                        });
                                            });
                                });
                return ret;
            }
        } catch (Throwable e) {
            throw new FlinkRuntimeException("Error binding or executing callback", e);
        }
    }

    public <A> StateFutureImpl<A> makeNewStateFuture() {
        return new StateFutureImpl<>(callbackRunner);
    }

    public void complete(T result) {
        completableFuture.complete(result);
    }

    @Override
    public void thenSyncAccept(Consumer<? super T> action) {
        completableFuture.thenAccept(action);
    }

    /** The entry for a state future to submit task to mailbox. */
    public interface CallbackRunner {
        void submit(Runnable task);
    }
}
