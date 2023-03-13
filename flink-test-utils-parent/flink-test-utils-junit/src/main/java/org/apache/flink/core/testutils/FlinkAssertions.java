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

import org.assertj.core.api.AbstractThrowableAssert;
import org.assertj.core.api.AssertFactory;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.assertj.core.api.ListAssert;
import org.assertj.core.api.ThrowingConsumer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Some reusable assertions and utilities for AssertJ. */
public final class FlinkAssertions {

    private FlinkAssertions() {}

    /** @see #chainOfCauses(Throwable) */
    @SuppressWarnings({"rawtypes", "unused"})
    public static final InstanceOfAssertFactory<Stream, ListAssert<Throwable>> STREAM_THROWABLE =
            new InstanceOfAssertFactory<>(Stream.class, Assertions::<Throwable>assertThat);

    /**
     * Shorthand to assert the chain of causes includes a {@link Throwable} matching a specific
     * {@link Class} and containing the provided message. Same as:
     *
     * <pre>{@code
     * assertThatChainOfCauses(throwable)
     *     .anySatisfy(
     *          cause ->
     *              assertThat(cause)
     *                  .isInstanceOf(clazz)
     *                  .hasMessageContaining(containsMessage));
     * }</pre>
     */
    public static ThrowingConsumer<? super Throwable> anyCauseMatches(
            Class<? extends Throwable> clazz, String containsMessage) {
        return t ->
                assertThatChainOfCauses(t)
                        .as(
                                "Any cause is instance of class '%s' and contains message '%s'",
                                clazz, containsMessage)
                        .anySatisfy(
                                cause ->
                                        assertThat(cause)
                                                .isInstanceOf(clazz)
                                                .hasMessageContaining(containsMessage));
    }

    /**
     * Shorthand to assert the chain of causes includes a {@link Throwable} matching a specific
     * {@link Class}. Same as:
     *
     * <pre>{@code
     * assertThatChainOfCauses(throwable)
     *     .anySatisfy(
     *          cause ->
     *              assertThat(cause)
     *                  .isInstanceOf(clazz));
     * }</pre>
     */
    public static ThrowingConsumer<? super Throwable> anyCauseMatches(
            Class<? extends Throwable> clazz) {
        return t ->
                assertThatChainOfCauses(t)
                        .as("Any cause is instance of class '%s'", clazz)
                        .anySatisfy(cause -> assertThat(cause).isInstanceOf(clazz));
    }

    /**
     * Shorthand to assert the chain of causes includes a {@link Throwable} matching a specific
     * {@link Class} and containing the provided message. Same as:
     *
     * <pre>{@code
     * assertThatChainOfCauses(throwable)
     *     .anySatisfy(
     *          cause ->
     *              assertThat(cause)
     *                  .hasMessageContaining(containsMessage));
     * }</pre>
     */
    public static ThrowingConsumer<? super Throwable> anyCauseMatches(String containsMessage) {
        return t ->
                assertThatChainOfCauses(t)
                        .as("Any cause contains message '%s'", containsMessage)
                        .anySatisfy(t1 -> assertThat(t1).hasMessageContaining(containsMessage));
    }

    /**
     * Shorthand to assert chain of causes. Same as:
     *
     * <pre>{@code
     * assertThat(throwable)
     *     .extracting(FlinkAssertions::chainOfCauses, FlinkAssertions.STREAM_THROWABLE)
     * }</pre>
     */
    public static ListAssert<Throwable> assertThatChainOfCauses(Throwable root) {
        return assertThat(root).extracting(FlinkAssertions::chainOfCauses, STREAM_THROWABLE);
    }

    /**
     * You can use this method in combination with {@link
     * AbstractThrowableAssert#extracting(Function, AssertFactory)} to perform assertions on a chain
     * of causes. For example:
     *
     * <pre>{@code
     * assertThat(throwable)
     *     .extracting(FlinkAssertions::chainOfCauses, FlinkAssertions.STREAM_THROWABLE)
     * }</pre>
     *
     * @return the list is ordered from the current {@link Throwable} up to the root cause.
     */
    public static Stream<Throwable> chainOfCauses(Throwable throwable) {
        if (throwable == null) {
            return Stream.empty();
        }
        if (throwable.getCause() == null) {
            return Stream.of(throwable);
        }
        return Stream.concat(Stream.of(throwable), chainOfCauses(throwable.getCause()));
    }

    /**
     * Create assertion for {@link java.util.concurrent.CompletableFuture}.
     *
     * @param actual the actual value.
     * @param <T> the type of the value contained in the {@link
     *     java.util.concurrent.CompletableFuture}.
     * @return the created assertion object.
     */
    public static <T> FlinkCompletableFutureAssert<T> assertThatFuture(
            CompletableFuture<T> actual) {
        return new FlinkCompletableFutureAssert<>(actual);
    }

    /**
     * Create assertion for {@link java.util.concurrent.CompletionStage}.
     *
     * @param actual the actual value.
     * @param <T> the type of the value contained in the {@link
     *     java.util.concurrent.CompletionStage}.
     * @return the created assertion object.
     */
    public static <T> FlinkCompletableFutureAssert<T> assertThatFuture(CompletionStage<T> actual) {
        return new FlinkCompletableFutureAssert<>(actual);
    }
}
