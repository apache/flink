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

package org.apache.flink.core.asyncprocessing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.function.BiFunctionWithException;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.ThrowingConsumer;
import org.apache.flink.util.function.ThrowingRunnable;

import java.util.concurrent.CompletableFuture;

/**
 * The default implementation of {@link AsyncFuture}. This is managed by the runtime framework and
 * should never be directly created in user code. It will handle the completion and callback
 * trigger, and most of the design are borrowed from the {@link CompletableFuture}. In the basic
 * version of this implementation, we wrap {@link CompletableFuture} for simplification. TODO:
 * remove CompletableFuture.
 *
 * <p>This class is on hot path and very complex, please take care of the performance as well as the
 * running thread of each block when you decide to touch it.
 */
@Internal
public class AsyncFutureImpl<T> implements InternalAsyncFuture<T> {

    /** The future holds the result. This may complete in async threads. */
    private final CompletableFuture<T> completableFuture;

    /** The callback runner. */
    protected final CallbackRunner callbackRunner;

    /** The exception handler that handles callback framework's error. */
    protected final AsyncFrameworkExceptionHandler exceptionHandler;

    public AsyncFutureImpl(
            CallbackRunner callbackRunner, AsyncFrameworkExceptionHandler exceptionHandler) {
        this.completableFuture = new CompletableFuture<>();
        this.callbackRunner = callbackRunner;
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public <U> InternalAsyncFuture<U> thenApply(
            FunctionWithException<? super T, ? extends U, ? extends Exception> fn) {
        callbackRegistered();
        if (completableFuture.isDone()) {
            // this branch must be invoked in task thread when expected
            T t;
            try {
                t = completableFuture.get();
            } catch (Exception e) {
                exceptionHandler.handleException(
                        "Caught exception when processing completed AsyncFuture's callback.", e);
                return null;
            }
            U r = FunctionWithException.unchecked(fn).apply(t);
            callbackFinished();
            return InternalAsyncFutureUtils.completedFuture(r);
        } else {
            AsyncFutureImpl<U> ret = makeNewFuture();
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
                                        "Caught exception when submitting AsyncFuture's callback.",
                                        e);
                                return null;
                            });
            return ret;
        }
    }

    @Override
    public InternalAsyncFuture<Void> thenAccept(
            ThrowingConsumer<? super T, ? extends Exception> action) {
        callbackRegistered();
        if (completableFuture.isDone()) {
            // this branch must be invoked in task thread when expected
            T t;
            try {
                t = completableFuture.get();
            } catch (Exception e) {
                exceptionHandler.handleException(
                        "Caught exception when processing completed AsyncFuture's callback.", e);
                return null;
            }
            ThrowingConsumer.unchecked(action).accept(t);
            callbackFinished();
            return InternalAsyncFutureUtils.completedVoidFuture();
        } else {
            AsyncFutureImpl<Void> ret = makeNewFuture();
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
                                        "Caught exception when submitting AsyncFuture's callback.",
                                        e);
                                return null;
                            });
            return ret;
        }
    }

    @Override
    public <U> InternalAsyncFuture<U> thenCompose(
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
                        "Caught exception when processing completed AsyncFuture's callback.", e);
                return null;
            }
            callbackFinished();
            return (InternalAsyncFuture<U>) FunctionWithException.unchecked(action).apply(t);
        } else {
            AsyncFutureImpl<U> ret = makeNewFuture();
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
                                        "Caught exception when submitting AsyncFuture's callback.",
                                        e);
                                return null;
                            });
            return ret;
        }
    }

    @Override
    public <U, V> InternalAsyncFuture<V> thenCombine(
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
                        "Caught exception when submitting AsyncFuture's callback.", e);
                return null;
            }

            return (InternalAsyncFuture<V>)
                    other.thenCompose(
                            (u) -> {
                                V v = fn.apply(t, u);
                                callbackFinished();
                                return InternalAsyncFutureUtils.completedFuture(v);
                            });
        } else {
            AsyncFutureImpl<V> ret = makeNewFuture();
            ((InternalAsyncFuture<? extends U>) other)
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
                                                            "Caught exception when submitting AsyncFuture's callback.",
                                                            e);
                                                    return null;
                                                });
                            });
            return ret;
        }
    }

    @Override
    public <U, V> InternalAsyncFuture<Tuple2<Boolean, Object>> thenConditionallyApply(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            FunctionWithException<? super T, ? extends U, ? extends Exception> actionIfTrue,
            FunctionWithException<? super T, ? extends V, ? extends Exception> actionIfFalse) {
        callbackRegistered();
        if (completableFuture.isDone()) {
            // this branch must be invoked in task thread when expected
            T t;
            try {
                t = completableFuture.get();
            } catch (Exception e) {
                exceptionHandler.handleException(
                        "Caught exception when processing completed AsyncFuture's callback.", e);
                return null;
            }
            boolean test = FunctionWithException.unchecked(condition).apply(t);
            Object r =
                    test
                            ? FunctionWithException.unchecked(actionIfTrue).apply(t)
                            : FunctionWithException.unchecked(actionIfFalse).apply(t);

            callbackFinished();
            return InternalAsyncFutureUtils.completedFuture(Tuple2.of(test, r));
        } else {
            AsyncFutureImpl<Tuple2<Boolean, Object>> ret = makeNewFuture();
            completableFuture
                    .thenAccept(
                            (t) -> {
                                callbackRunner.submit(
                                        () -> {
                                            boolean test = condition.apply(t);
                                            Object r =
                                                    test
                                                            ? actionIfTrue.apply(t)
                                                            : actionIfFalse.apply(t);
                                            ret.completeInCallbackRunner(Tuple2.of(test, r));
                                            callbackFinished();
                                        });
                            })
                    .exceptionally(
                            (e) -> {
                                exceptionHandler.handleException(
                                        "Caught exception when submitting AsyncFuture's callback.",
                                        e);
                                return null;
                            });
            return ret;
        }
    }

    @Override
    public <U> InternalAsyncFuture<Tuple2<Boolean, U>> thenConditionallyApply(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            FunctionWithException<? super T, ? extends U, ? extends Exception> actionIfTrue) {
        callbackRegistered();
        if (completableFuture.isDone()) {
            // this branch must be invoked in task thread when expected
            T t;
            try {
                t = completableFuture.get();
            } catch (Exception e) {
                exceptionHandler.handleException(
                        "Caught exception when processing completed InternalAsyncFuture's callback.",
                        e);
                return null;
            }
            boolean test = FunctionWithException.unchecked(condition).apply(t);
            U r = test ? FunctionWithException.unchecked(actionIfTrue).apply(t) : null;

            callbackFinished();
            return InternalAsyncFutureUtils.completedFuture(Tuple2.of(test, r));
        } else {
            AsyncFutureImpl<Tuple2<Boolean, U>> ret = makeNewFuture();
            completableFuture
                    .thenAccept(
                            (t) -> {
                                callbackRunner.submit(
                                        () -> {
                                            boolean test = condition.apply(t);
                                            U r = test ? actionIfTrue.apply(t) : null;
                                            ret.completeInCallbackRunner(Tuple2.of(test, r));
                                            callbackFinished();
                                        });
                            })
                    .exceptionally(
                            (e) -> {
                                exceptionHandler.handleException(
                                        "Caught exception when submitting AsyncFuture's callback.",
                                        e);
                                return null;
                            });
            return ret;
        }
    }

    @Override
    public InternalAsyncFuture<Boolean> thenConditionallyAccept(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            ThrowingConsumer<? super T, ? extends Exception> actionIfTrue,
            ThrowingConsumer<? super T, ? extends Exception> actionIfFalse) {
        callbackRegistered();
        if (completableFuture.isDone()) {
            // this branch must be invoked in task thread when expected
            T t;
            try {
                t = completableFuture.get();
            } catch (Exception e) {
                exceptionHandler.handleException(
                        "Caught exception when processing completed AsyncFuture's callback.", e);
                return null;
            }
            boolean test = FunctionWithException.unchecked(condition).apply(t);
            if (test) {
                ThrowingConsumer.unchecked(actionIfTrue).accept(t);
            } else {
                ThrowingConsumer.unchecked(actionIfFalse).accept(t);
            }
            callbackFinished();
            return InternalAsyncFutureUtils.completedFuture(test);
        } else {
            AsyncFutureImpl<Boolean> ret = makeNewFuture();
            completableFuture
                    .thenAccept(
                            (t) -> {
                                callbackRunner.submit(
                                        () -> {
                                            boolean test = condition.apply(t);
                                            if (test) {
                                                actionIfTrue.accept(t);
                                            } else {
                                                actionIfFalse.accept(t);
                                            }
                                            ret.completeInCallbackRunner(test);
                                            callbackFinished();
                                        });
                            })
                    .exceptionally(
                            (e) -> {
                                exceptionHandler.handleException(
                                        "Caught exception when submitting AsyncFuture's callback.",
                                        e);
                                return null;
                            });
            return ret;
        }
    }

    @Override
    public InternalAsyncFuture<Boolean> thenConditionallyAccept(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            ThrowingConsumer<? super T, ? extends Exception> actionIfTrue) {
        return thenConditionallyAccept(condition, actionIfTrue, (b) -> {});
    }

    @Override
    public <U, V> InternalAsyncFuture<Tuple2<Boolean, Object>> thenConditionallyCompose(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            FunctionWithException<? super T, ? extends StateFuture<U>, ? extends Exception>
                    actionIfTrue,
            FunctionWithException<? super T, ? extends StateFuture<V>, ? extends Exception>
                    actionIfFalse) {
        callbackRegistered();
        if (completableFuture.isDone()) {
            // this branch must be invoked in task thread when expected
            T t;
            try {
                t = completableFuture.get();
            } catch (Throwable e) {
                exceptionHandler.handleException(
                        "Caught exception when processing completed AsyncFuture's callback.", e);
                return null;
            }
            boolean test = FunctionWithException.unchecked(condition).apply(t);
            StateFuture<?> actionResult;
            if (test) {
                actionResult = FunctionWithException.unchecked(actionIfTrue).apply(t);
            } else {
                actionResult = FunctionWithException.unchecked(actionIfFalse).apply(t);
            }
            AsyncFutureImpl<Tuple2<Boolean, Object>> ret = makeNewFuture();
            actionResult.thenAccept(
                    (e) -> {
                        ret.completeInCallbackRunner(Tuple2.of(test, e));
                    });
            callbackFinished();
            return ret;
        } else {
            AsyncFutureImpl<Tuple2<Boolean, Object>> ret = makeNewFuture();
            completableFuture
                    .thenAccept(
                            (t) -> {
                                callbackRunner.submit(
                                        () -> {
                                            boolean test = condition.apply(t);
                                            StateFuture<?> actionResult;
                                            if (test) {
                                                actionResult = actionIfTrue.apply(t);
                                            } else {
                                                actionResult = actionIfFalse.apply(t);
                                            }
                                            actionResult.thenAccept(
                                                    (e) -> {
                                                        ret.completeInCallbackRunner(
                                                                Tuple2.of(test, e));
                                                    });
                                            callbackFinished();
                                        });
                            })
                    .exceptionally(
                            (e) -> {
                                exceptionHandler.handleException(
                                        "Caught exception when submitting AsyncFuture's callback.",
                                        e);
                                return null;
                            });
            return ret;
        }
    }

    @Override
    public <U> InternalAsyncFuture<Tuple2<Boolean, U>> thenConditionallyCompose(
            FunctionWithException<? super T, Boolean, ? extends Exception> condition,
            FunctionWithException<? super T, ? extends StateFuture<U>, ? extends Exception>
                    actionIfTrue) {
        callbackRegistered();
        if (completableFuture.isDone()) {
            // this branch must be invoked in task thread when expected
            T t;
            try {
                t = completableFuture.get();
            } catch (Throwable e) {
                exceptionHandler.handleException(
                        "Caught exception when processing completed AsyncFuture's callback.", e);
                return null;
            }
            boolean test = FunctionWithException.unchecked(condition).apply(t);

            if (test) {
                StateFuture<U> actionResult =
                        FunctionWithException.unchecked(actionIfTrue).apply(t);
                AsyncFutureImpl<Tuple2<Boolean, U>> ret = makeNewFuture();
                actionResult.thenAccept(
                        (e) -> {
                            ret.completeInCallbackRunner(Tuple2.of(true, e));
                        });
                callbackFinished();
                return ret;
            } else {
                callbackFinished();
                return InternalAsyncFutureUtils.completedFuture(Tuple2.of(false, null));
            }
        } else {
            AsyncFutureImpl<Tuple2<Boolean, U>> ret = makeNewFuture();
            completableFuture
                    .thenAccept(
                            (t) -> {
                                callbackRunner.submit(
                                        () -> {
                                            boolean test = condition.apply(t);
                                            if (test) {
                                                StateFuture<U> actionResult = actionIfTrue.apply(t);
                                                actionResult.thenAccept(
                                                        (e) -> {
                                                            ret.completeInCallbackRunner(
                                                                    Tuple2.of(true, e));
                                                        });
                                            } else {
                                                ret.completeInCallbackRunner(
                                                        Tuple2.of(false, null));
                                            }
                                            callbackFinished();
                                        });
                            })
                    .exceptionally(
                            (e) -> {
                                exceptionHandler.handleException(
                                        "Caught exception when submitting AsyncFuture's callback.",
                                        e);
                                return null;
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
    public <A> AsyncFutureImpl<A> makeNewFuture() {
        return new AsyncFutureImpl<>(callbackRunner, exceptionHandler);
    }

    @Override
    public boolean isDone() {
        return completableFuture.isDone();
    }

    @Override
    public T get() {
        T t;
        try {
            t = completableFuture.get();
        } catch (Exception e) {
            exceptionHandler.handleException(
                    "Caught exception when getting AsyncFuture's result.", e);
            return null;
        }
        return t;
    }

    @Override
    public void complete(T result) {
        if (completableFuture.isCompletedExceptionally()) {
            throw new IllegalStateException("AsyncFuture already failed !");
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
                                    "Caught exception when processing completed AsyncFuture's callback.",
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
