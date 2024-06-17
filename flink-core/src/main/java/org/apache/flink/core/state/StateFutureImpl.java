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
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

import java.util.concurrent.CompletableFuture;

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

    /** The exception handler that handles callback framework's error. */
    protected final AsyncFrameworkExceptionHandler exceptionHandler;

    public StateFutureImpl(
            CallbackRunner callbackRunner, AsyncFrameworkExceptionHandler exceptionHandler) {
        this.completableFuture = new CompletableFuture<>();
        this.callbackRunner = callbackRunner;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public <U> StateFuture<U> thenApply(
            FunctionWithException<? super T, ? extends U, ? extends Exception> fn) {
        callbackRegistered();
        if (completableFuture.isDone()) {
            // this branch must be invoked in task thread when expected
            T t;
            try {
                t = completableFuture.get();
            } catch (Exception e) {
                exceptionHandler.handleException(
                        "Caught exception when processing completed StateFuture's callback.", e);
                return null;
            }
            U r = FunctionWithException.unchecked(fn).apply(t);
            callbackFinished();
            return StateFutureUtils.completedFuture(r);
        } else {
            StateFutureImpl<U> ret = makeNewStateFuture();
            completableFuture
                    .thenAccept(
                            (t) -> {
                                callbackRunner.submit(
                                        () -> {
                                            ret.completeInCallbackRunner(fn.apply(t));
                                            callbackFinished();
                                        });
                            })
                    .exceptionally(
                            (e) -> {
                                exceptionHandler.handleException(
                                        "Caught exception when submitting StateFuture's callback.",
                                        e);
                                return null;
                            });
            return ret;
        }
    }

    @Override
    public StateFuture<Void> thenAccept(ThrowingConsumer<? super T, ? extends Exception> action) {
        callbackRegistered();
        if (completableFuture.isDone()) {
            // this branch must be invoked in task thread when expected
            T t;
            try {
                t = completableFuture.get();
            } catch (Exception e) {
                exceptionHandler.handleException(
                        "Caught exception when processing completed StateFuture's callback.", e);
                return null;
            }
            ThrowingConsumer.unchecked(action).accept(t);
            callbackFinished();
            return StateFutureUtils.completedVoidFuture();
        } else {
            StateFutureImpl<Void> ret = makeNewStateFuture();
            completableFuture
                    .thenAccept(
                            (t) -> {
                                callbackRunner.submit(
                                        () -> {
                                            action.accept(t);
                                            ret.completeInCallbackRunner(null);
                                            callbackFinished();
                                        });
                            })
                    .exceptionally(
                            (e) -> {
                                exceptionHandler.handleException(
                                        "Caught exception when submitting StateFuture's callback.",
                                        e);
                                return null;
                            });
            return ret;
        }
    }

    @Override
    public <U> StateFuture<U> thenCompose(
            FunctionWithException<? super T, ? extends StateFuture<U>, ? extends Exception>
                    action) {
        callbackRegistered();
        if (completableFuture.isDone()) {
            // this branch must be invoked in task thread when expected
            T t;
            try {
                t = completableFuture.get();
            } catch (Throwable e) {
                exceptionHandler.handleException(
                        "Caught exception when processing completed StateFuture's callback.", e);
                return null;
            }
            callbackFinished();
            return FunctionWithException.unchecked(action).apply(t);
        } else {
            StateFutureImpl<U> ret = makeNewStateFuture();
            completableFuture
                    .thenAccept(
                            (t) -> {
                                callbackRunner.submit(
                                        () -> {
                                            StateFuture<U> su = action.apply(t);
                                            su.thenAccept(ret::completeInCallbackRunner);
                                            callbackFinished();
                                        });
                            })
                    .exceptionally(
                            (e) -> {
                                exceptionHandler.handleException(
                                        "Caught exception when submitting StateFuture's callback.",
                                        e);
                                return null;
                            });
            return ret;
        }
    }

    @Override
    public <U, V> StateFuture<V> thenCombine(
            StateFuture<? extends U> other,
            BiFunctionWithException<? super T, ? super U, ? extends V, ? extends Exception> fn) {
        callbackRegistered();
        if (completableFuture.isDone()) {
            // this branch must be invoked in task thread when expected
            T t;
            try {
                t = completableFuture.get();
            } catch (Throwable e) {
                exceptionHandler.handleException(
                        "Caught exception when submitting StateFuture's callback.", e);
                return null;
            }

            return other.thenCompose(
                    (u) -> {
                        V v = fn.apply(t, u);
                        callbackFinished();
                        return StateFutureUtils.completedFuture(v);
                    });
        } else {
            StateFutureImpl<V> ret = makeNewStateFuture();
            ((InternalStateFuture<? extends U>) other)
                    .thenSyncAccept(
                            (u) -> {
                                completableFuture
                                        .thenAccept(
                                                (t) -> {
                                                    callbackRunner.submit(
                                                            () -> {
                                                                ret.completeInCallbackRunner(
                                                                        fn.apply(t, u));
                                                                callbackFinished();
                                                            });
                                                })
                                        .exceptionally(
                                                (e) -> {
                                                    exceptionHandler.handleException(
                                                            "Caught exception when submitting StateFuture's callback.",
                                                            e);
                                                    return null;
                                                });
                            });
            return ret;
        }
    }

    /**
     * Make a new future based on context of this future. Subclasses need to overload this method to
     * generate their own instances (if needed).
     *
     * @return the new created future.
     */
    public <A> StateFutureImpl<A> makeNewStateFuture() {
        return new StateFutureImpl<>(callbackRunner, exceptionHandler);
    }

    @Override
    public void complete(T result) {
        if (completableFuture.isCompletedExceptionally()) {
            throw new IllegalStateException("StateFuture already failed !");
        }
        completableFuture.complete(result);
        postComplete(false);
    }

    @Override
    public void completeExceptionally(String message, Throwable ex) {
        exceptionHandler.handleException(message, ex);
    }

    private void completeInCallbackRunner(T result) {
        completableFuture.complete(result);
        postComplete(true);
    }

    /** Will be triggered when a callback is registered. */
    public void callbackRegistered() {
        // does nothing by default.
    }

    /** Will be triggered when this future completes. */
    public void postComplete(boolean inCallbackRunner) {
        // does nothing by default.
    }

    /** Will be triggered when a callback finishes processing. */
    public void callbackFinished() {
        // does nothing by default.
    }

    @Override
    public void thenSyncAccept(ThrowingConsumer<? super T, ? extends Exception> action) {
        completableFuture
                .thenAccept(ThrowingConsumer.unchecked(action))
                .exceptionally(
                        (e) -> {
                            exceptionHandler.handleException(
                                    "Caught exception when processing completed StateFuture's callback.",
                                    e);
                            return null;
                        });
    }

    /** The entry for a state future to submit task to mailbox. */
    public interface CallbackRunner {
        void submit(ThrowingRunnable<? extends Exception> task);
    }

    /** Handle exceptions thrown by async state callback framework. */
    public interface AsyncFrameworkExceptionHandler {
        /** Handles an exception thrown by callback. */
        void handleException(String message, Throwable exception);
    }
}
