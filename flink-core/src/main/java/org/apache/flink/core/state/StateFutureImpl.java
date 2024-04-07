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

    /** The future holds the result. This may complete in async threads. */
    private final CompletableFuture<T> completableFuture;

    /** The callback runner. */
    protected final CallbackRunner callbackRunner;

    public StateFutureImpl(CallbackRunner callbackRunner) {
        this.completableFuture = new CompletableFuture<>();
        this.callbackRunner = callbackRunner;
    }

    @Override
    public <U> StateFuture<U> thenApply(Function<? super T, ? extends U> fn) {
        callbackRegistered();
        try {
            if (completableFuture.isDone()) {
                U r = fn.apply(completableFuture.get());
                callbackFinished();
                return StateFutureUtils.completedFuture(r);
            } else {
                StateFutureImpl<U> ret = makeNewStateFuture();
                completableFuture.thenAccept(
                        (t) -> {
                            callbackRunner.submit(
                                    () -> {
                                        ret.complete(fn.apply(t));
                                        callbackFinished();
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
        callbackRegistered();
        try {
            if (completableFuture.isDone()) {
                action.accept(completableFuture.get());
                callbackFinished();
                return StateFutureUtils.completedVoidFuture();
            } else {
                StateFutureImpl<Void> ret = makeNewStateFuture();
                completableFuture.thenAccept(
                        (t) -> {
                            callbackRunner.submit(
                                    () -> {
                                        action.accept(t);
                                        ret.complete(null);
                                        callbackFinished();
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
        callbackRegistered();
        try {
            if (completableFuture.isDone()) {
                callbackFinished();
                return action.apply(completableFuture.get());
            } else {
                StateFutureImpl<U> ret = makeNewStateFuture();
                completableFuture.thenAccept(
                        (t) -> {
                            callbackRunner.submit(
                                    () -> {
                                        StateFuture<U> su = action.apply(t);
                                        su.thenAccept(ret::complete);
                                        callbackFinished();
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
        callbackRegistered();
        try {
            if (completableFuture.isDone()) {
                return other.thenCompose(
                        (u) -> {
                            try {
                                V v = fn.apply(completableFuture.get(), u);
                                callbackFinished();
                                return StateFutureUtils.completedFuture(v);
                            } catch (Throwable e) {
                                throw new FlinkRuntimeException(
                                        "Error binding or executing callback", e);
                            }
                        });
            } else {
                StateFutureImpl<V> ret = makeNewStateFuture();
                ((InternalStateFuture<? extends U>) other)
                        .thenSyncAccept(
                                (u) -> {
                                    completableFuture.thenAccept(
                                            (t) -> {
                                                callbackRunner.submit(
                                                        () -> {
                                                            ret.complete(fn.apply(t, u));
                                                            callbackFinished();
                                                        });
                                            });
                                });
                return ret;
            }
        } catch (Throwable e) {
            throw new FlinkRuntimeException("Error binding or executing callback", e);
        }
    }

    /**
     * Make a new future based on context of this future. Subclasses need to overload this method to
     * generate their own instances (if needed).
     *
     * @return the new created future.
     */
    public <A> StateFutureImpl<A> makeNewStateFuture() {
        return new StateFutureImpl<>(callbackRunner);
    }

    @Override
    public void complete(T result) {
        completableFuture.complete(result);
        postComplete();
    }

    /** Will be triggered when a callback is registered. */
    public void callbackRegistered() {
        // does nothing by default.
    }

    /** Will be triggered when this future completes. */
    public void postComplete() {
        // does nothing by default.
    }

    /** Will be triggered when a callback finishes processing. */
    public void callbackFinished() {
        // does nothing by default.
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
