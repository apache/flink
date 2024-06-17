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

package org.apache.flink.util.concurrent;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.concurrent.FutureUtils.ConjunctFuture;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link ConjunctFuture} and its subclasses. */
@ExtendWith(ParameterizedTestExtension.class)
class ConjunctFutureTest {

    @Parameters
    private static Collection<FutureFactory> parameters() {
        return Arrays.asList(new ConjunctFutureFactory(), new WaitingFutureFactory());
    }

    private final FutureFactory futureFactory;

    ConjunctFutureTest(FutureFactory futureFactory) {
        this.futureFactory = futureFactory;
    }

    @TestTemplate
    void testConjunctFutureFailsOnEmptyAndNull() {
        assertThatThrownBy(() -> futureFactory.createFuture(null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(
                        () ->
                                futureFactory.createFuture(
                                        Arrays.asList(
                                                new CompletableFuture<>(),
                                                null,
                                                new CompletableFuture<>())))
                .isInstanceOf(NullPointerException.class);
    }

    @TestTemplate
    void testConjunctFutureCompletion() {
        // some futures that we combine
        java.util.concurrent.CompletableFuture<Object> future1 =
                new java.util.concurrent.CompletableFuture<>();
        java.util.concurrent.CompletableFuture<Object> future2 =
                new java.util.concurrent.CompletableFuture<>();
        java.util.concurrent.CompletableFuture<Object> future3 =
                new java.util.concurrent.CompletableFuture<>();
        java.util.concurrent.CompletableFuture<Object> future4 =
                new java.util.concurrent.CompletableFuture<>();

        // some future is initially completed
        future2.complete(new Object());

        // build the conjunct future
        ConjunctFuture<?> result =
                futureFactory.createFuture(Arrays.asList(future1, future2, future3, future4));

        CompletableFuture<?> resultMapped = result.thenAccept(value -> {});

        assertThat(result.getNumFuturesTotal()).isEqualTo(4);
        assertThat(result.getNumFuturesCompleted()).isOne();
        assertThat(result).isNotDone();
        assertThat(resultMapped).isNotDone();

        // complete two more futures
        future4.complete(new Object());
        assertThat(result.getNumFuturesCompleted()).isEqualTo(2);
        assertThat(result).isNotDone();
        assertThat(resultMapped).isNotDone();

        future1.complete(new Object());
        assertThat(result.getNumFuturesCompleted()).isEqualTo(3);
        assertThat(result).isNotDone();
        assertThat(resultMapped).isNotDone();

        // complete one future again
        future1.complete(new Object());
        assertThat(result.getNumFuturesCompleted()).isEqualTo(3);
        assertThat(result).isNotDone();
        assertThat(resultMapped).isNotDone();

        // complete the final future
        future3.complete(new Object());
        assertThat(result.getNumFuturesCompleted()).isEqualTo(4);
        assertThat(result).isDone();
        assertThat(resultMapped).isDone();
    }

    @TestTemplate
    void testConjunctFutureFailureOnFirst() {

        java.util.concurrent.CompletableFuture<Object> future1 =
                new java.util.concurrent.CompletableFuture<>();
        java.util.concurrent.CompletableFuture<Object> future2 =
                new java.util.concurrent.CompletableFuture<>();
        java.util.concurrent.CompletableFuture<Object> future3 =
                new java.util.concurrent.CompletableFuture<>();
        java.util.concurrent.CompletableFuture<Object> future4 =
                new java.util.concurrent.CompletableFuture<>();

        // build the conjunct future
        ConjunctFuture<?> result =
                futureFactory.createFuture(Arrays.asList(future1, future2, future3, future4));

        CompletableFuture<?> resultMapped = result.thenAccept(value -> {});

        assertThat(result.getNumFuturesTotal()).isEqualTo(4);
        assertThat(result.getNumFuturesCompleted()).isZero();
        assertThat(result).isNotDone();
        assertThat(resultMapped).isNotDone();

        future2.completeExceptionally(new IOException());

        assertThat(result.getNumFuturesCompleted()).isZero();
        assertThat(result).isDone();
        assertThat(resultMapped).isDone();

        assertThatThrownBy(result::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IOException.class);

        assertThatThrownBy(resultMapped::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IOException.class);
    }

    @TestTemplate
    void testConjunctFutureFailureOnSuccessive() {

        java.util.concurrent.CompletableFuture<Object> future1 =
                new java.util.concurrent.CompletableFuture<>();
        java.util.concurrent.CompletableFuture<Object> future2 =
                new java.util.concurrent.CompletableFuture<>();
        java.util.concurrent.CompletableFuture<Object> future3 =
                new java.util.concurrent.CompletableFuture<>();
        java.util.concurrent.CompletableFuture<Object> future4 =
                new java.util.concurrent.CompletableFuture<>();

        // build the conjunct future
        ConjunctFuture<?> result =
                futureFactory.createFuture(Arrays.asList(future1, future2, future3, future4));
        assertThat(result.getNumFuturesTotal()).isEqualTo(4);

        java.util.concurrent.CompletableFuture<?> resultMapped = result.thenAccept(value -> {});

        future1.complete(new Object());
        future3.complete(new Object());
        future4.complete(new Object());

        future2.completeExceptionally(new IOException());

        assertThat(result.getNumFuturesCompleted()).isEqualTo(3);
        assertThat(result).isDone();
        assertThat(resultMapped).isDone();

        assertThatThrownBy(result::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IOException.class);

        assertThatThrownBy(resultMapped::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(IOException.class);
    }

    /**
     * Tests that the conjunct future returns upon completion the collection of all future values in
     * the same order in which the futures were inserted.
     */
    @TestTemplate
    void testConjunctFutureValue() throws Exception {
        final int numberFutures = 10;

        final List<CompletableFuture<Integer>> futures = new ArrayList<>(numberFutures);
        for (int i = 0; i < numberFutures; i++) {
            futures.add(new CompletableFuture<>());
        }

        ConjunctFuture<Collection<Number>> result = FutureUtils.combineAll(futures);

        final List<Tuple2<Integer, CompletableFuture<Integer>>> shuffledFutures =
                IntStream.range(0, futures.size())
                        .mapToObj(index -> Tuple2.of(index, futures.get(index)))
                        .collect(Collectors.toList());
        Collections.shuffle(shuffledFutures);

        for (Tuple2<Integer, CompletableFuture<Integer>> shuffledFuture : shuffledFutures) {
            assertThat(result).isNotDone();
            shuffledFuture.f1.complete(shuffledFuture.f0);
        }

        assertThat(result).isDone();

        assertThat(result.get())
                .isEqualTo(IntStream.range(0, numberFutures).boxed().collect(Collectors.toList()));
    }

    @TestTemplate
    void testConjunctOfNone() {
        final ConjunctFuture<?> result =
                futureFactory.createFuture(
                        Collections.<java.util.concurrent.CompletableFuture<Object>>emptyList());

        assertThat(result.getNumFuturesTotal()).isZero();
        assertThat(result.getNumFuturesCompleted()).isZero();
        assertThat(result).isDone();
    }

    /** Factory to create {@link ConjunctFuture} for testing. */
    private interface FutureFactory {
        ConjunctFuture<?> createFuture(
                Collection<? extends java.util.concurrent.CompletableFuture<?>> futures);
    }

    private static class ConjunctFutureFactory implements FutureFactory {

        @Override
        public ConjunctFuture<?> createFuture(
                Collection<? extends java.util.concurrent.CompletableFuture<?>> futures) {
            return FutureUtils.combineAll(futures);
        }
    }

    private static class WaitingFutureFactory implements FutureFactory {

        @Override
        public ConjunctFuture<?> createFuture(
                Collection<? extends java.util.concurrent.CompletableFuture<?>> futures) {
            return FutureUtils.waitForAll(futures);
        }
    }
}
