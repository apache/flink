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

package org.apache.flink.core.testutils;

import org.assertj.core.api.AbstractCompletableFutureAssert;
import org.assertj.core.api.AssertionInfo;
import org.assertj.core.api.ObjectAssert;
import org.assertj.core.api.ThrowableAssertAlternative;
import org.assertj.core.error.BasicErrorMessageFactory;
import org.assertj.core.internal.Failures;
import org.assertj.core.internal.Objects;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Enhanced version of {@link org.assertj.core.api.CompletableFutureAssert}, that allows asserting
 * futures without relying on timeouts.
 *
 * @param <T> type of the value contained in the {@link CompletableFuture}.
 */
public class FlinkCompletableFutureAssert<T>
        extends AbstractCompletableFutureAssert<FlinkCompletableFutureAssert<T>, T> {

    private static final String SHOULD_HAVE_SUCCEEDED = "%nExpecting%n  <%s>%nto have succeeded.%n";

    private static final String SHOULD_HAVE_FAILED = "%nExpecting%n  <%s>%nto have failed.%n";

    private static final String SHOULD_NOT_COMPLETED =
            "%nExpecting%n  <%s>%nto have not completed normally or exceptionally within %s ms.%n";

    /** A strongly typed alternative to {@link org.assertj.core.api.WithThrowable}. */
    public static class WithThrowable {

        private final Throwable throwable;

        private WithThrowable(Throwable throwable) {
            this.throwable = throwable;
        }

        /**
         * Checks that the underlying throwable is of the given type and returns a {@link
         * ThrowableAssertAlternative} to chain further assertions on the underlying throwable.
         *
         * @param type the expected {@link Throwable} type
         * @param <T> the expected {@link Throwable} type
         * @return a {@link ThrowableAssertAlternative} built with underlying throwable.
         */
        public <T extends Throwable> ThrowableAssertAlternative<T> withThrowableOfType(
                Class<T> type) {
            final ThrowableAssertAlternative<Throwable> throwableAssert =
                    new ThrowableAssertAlternative<>(throwable).isInstanceOf(type);
            @SuppressWarnings("unchecked")
            final ThrowableAssertAlternative<T> cast =
                    (ThrowableAssertAlternative<T>) throwableAssert;
            return cast;
        }
    }

    FlinkCompletableFutureAssert(CompletableFuture<T> actual) {
        super(actual, FlinkCompletableFutureAssert.class);
    }

    FlinkCompletableFutureAssert(CompletionStage<T> actual) {
        super(actual.toCompletableFuture(), FlinkCompletableFutureAssert.class);
    }

    /**
     * An equivalent of {@link #succeedsWithin(Duration)}, that doesn't rely on timeouts.
     *
     * @return a new assertion object on the future's result
     */
    public ObjectAssert<T> eventuallySucceeds() {
        final T object = assertEventuallySucceeds(info, actual);
        return new ObjectAssert<>(object);
    }

    /**
     * An equivalent of {@link #failsWithin(Duration)}, that doesn't rely on timeouts.
     *
     * @return a new assertion instance on the future's exception.
     */
    public WithThrowable eventuallyFails() {
        return new WithThrowable(assertEventuallyFails(info, actual));
    }

    /**
     * An equivalent of {@link #failsWithin(Duration)}, that doesn't rely on timeouts.
     *
     * @param exceptionClass type of the exception we expect the future to complete with
     * @return a new assertion instance on the future's exception.
     * @param <E> type of the exception we expect the future to complete with
     */
    public <E extends Throwable> ThrowableAssertAlternative<E> eventuallyFailsWith(
            Class<E> exceptionClass) {
        return eventuallyFails().withThrowableOfType(exceptionClass);
    }

    /**
     * Assert that {@link CompletableFuture} will not complete within a fixed duration.
     *
     * <p>This is a replacement for {@link FlinkMatchers#willNotComplete(Duration)} in assertj.
     *
     * @return {@code this} assertion object.
     */
    public FlinkCompletableFutureAssert<T> willNotCompleteWithin(Duration duration) {
        assertWillNotCompleteWithin(info, actual, duration);
        return myself;
    }

    private T assertEventuallySucceeds(AssertionInfo info, Future<T> actual) {
        Objects.instance().assertNotNull(info, actual);
        try {
            return actual.get();
        } catch (InterruptedException | ExecutionException | CancellationException e) {
            throw Failures.instance()
                    .failure(info, new BasicErrorMessageFactory(SHOULD_HAVE_SUCCEEDED, actual));
        }
    }

    private Exception assertEventuallyFails(AssertionInfo info, Future<?> actual) {
        Objects.instance().assertNotNull(info, actual);
        try {
            actual.get();
            throw Failures.instance()
                    .failure(info, new BasicErrorMessageFactory(SHOULD_HAVE_FAILED, actual));
        } catch (InterruptedException | ExecutionException | CancellationException e) {
            return e;
        }
    }

    private void assertWillNotCompleteWithin(
            AssertionInfo info, Future<T> actual, Duration duration) {
        Objects.instance().assertNotNull(info, actual);
        try {
            actual.get(duration.toMillis(), TimeUnit.MILLISECONDS);
            throw Failures.instance()
                    .failure(
                            info,
                            new BasicErrorMessageFactory(
                                    SHOULD_NOT_COMPLETED, actual, duration.toMillis()));
        } catch (TimeoutException ignored) {
            // only timeout exception is expected.
        } catch (Throwable e) {
            throw Failures.instance()
                    .failure(
                            info,
                            new BasicErrorMessageFactory(
                                    SHOULD_NOT_COMPLETED, actual, duration.toMillis()));
        }
    }
}
