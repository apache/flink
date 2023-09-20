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

package org.apache.flink.runtime.rest.handler.async;

import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.messages.TriggerId;
import org.apache.flink.runtime.util.ManualTicker;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link CompletedOperationCache}. */
class CompletedOperationCacheTest {

    private static final OperationKey TEST_OPERATION_KEY = new OperationKey(new TriggerId());

    private static final CompletableFuture<String> TEST_OPERATION_RESULT =
            CompletableFuture.completedFuture("foo");

    private ManualTicker manualTicker;

    private CompletedOperationCache<OperationKey, String> completedOperationCache;

    @BeforeEach
    void setUp() {
        manualTicker = new ManualTicker();
        completedOperationCache = new CompletedOperationCache<>(manualTicker);
    }

    @Test
    void testShouldFinishClosingCacheIfAllResultsAreEvicted() {
        completedOperationCache.registerOngoingOperation(TEST_OPERATION_KEY, TEST_OPERATION_RESULT);
        final CompletableFuture<Void> closeCacheFuture = completedOperationCache.closeAsync();
        assertThat(closeCacheFuture).isNotDone();

        manualTicker.advanceTime(300, TimeUnit.SECONDS);
        completedOperationCache.cleanUp();

        assertThat(closeCacheFuture).isDone();
    }

    @Test
    void testShouldFinishClosingCacheIfAllResultsAccessed() throws Exception {
        completedOperationCache.registerOngoingOperation(TEST_OPERATION_KEY, TEST_OPERATION_RESULT);
        final CompletableFuture<Void> closeCacheFuture = completedOperationCache.closeAsync();
        assertThat(closeCacheFuture).isNotDone();

        final Optional<OperationResult<String>> operationResultOptional =
                completedOperationCache.get(TEST_OPERATION_KEY);

        assertThat(operationResultOptional).isPresent();
        OperationResult<String> operationResult = operationResultOptional.get();
        assertThat(OperationResultStatus.SUCCESS).isEqualTo(operationResult.getStatus());
        assertThat(operationResult.getResult()).isEqualTo(TEST_OPERATION_RESULT.get());
        assertThat(closeCacheFuture).isDone();
    }

    @Test
    void testCannotAddOperationAfterClosing() {
        completedOperationCache.registerOngoingOperation(
                TEST_OPERATION_KEY, new CompletableFuture<>());
        final CompletableFuture<Void> terminationFuture = completedOperationCache.closeAsync();

        assertThat(terminationFuture).isNotDone();

        try {
            completedOperationCache.registerOngoingOperation(
                    new OperationKey(new TriggerId()), new CompletableFuture<>());
            fail(
                    "It should no longer be possible to register new operations because the cache is shutting down.");
        } catch (IllegalStateException ignored) {
            // expected
        }
    }

    @Test
    void testCanGetOperationResultAfterClosing() throws Exception {
        completedOperationCache.registerOngoingOperation(TEST_OPERATION_KEY, TEST_OPERATION_RESULT);
        completedOperationCache.closeAsync();

        final Optional<OperationResult<String>> operationResultOptional =
                completedOperationCache.get(TEST_OPERATION_KEY);

        assertThat(operationResultOptional).isPresent();
        final OperationResult<String> operationResult = operationResultOptional.get();
        assertThat(OperationResultStatus.SUCCESS).isEqualTo(operationResult.getStatus());
        assertThat(operationResult.getResult()).isEqualTo(TEST_OPERATION_RESULT.get());
    }

    @Test
    void testCacheTimeout() throws Exception {
        final Duration timeout = RestOptions.ASYNC_OPERATION_STORE_DURATION.defaultValue();

        completedOperationCache = new CompletedOperationCache<>(timeout, manualTicker);
        completedOperationCache.registerOngoingOperation(TEST_OPERATION_KEY, TEST_OPERATION_RESULT);

        // sanity check that the operation can be retrieved before the timeout
        assertThat(completedOperationCache.get(TEST_OPERATION_KEY)).isPresent();

        manualTicker.advanceTime(timeout.multipliedBy(2).getSeconds(), TimeUnit.SECONDS);

        assertThat(completedOperationCache.get(TEST_OPERATION_KEY)).isNotPresent();
    }

    @Test
    void testCacheTimeoutCanBeDisabled() throws Exception {
        completedOperationCache =
                new CompletedOperationCache<>(Duration.ofSeconds(0), manualTicker);
        completedOperationCache.registerOngoingOperation(TEST_OPERATION_KEY, TEST_OPERATION_RESULT);

        manualTicker.advanceTime(365, TimeUnit.DAYS);

        assertThat(completedOperationCache.get(TEST_OPERATION_KEY)).isPresent();
    }

    @Test
    void testCacheTimeoutCanBeConfigured() throws Exception {
        final Duration baseTimeout = RestOptions.ASYNC_OPERATION_STORE_DURATION.defaultValue();

        completedOperationCache =
                new CompletedOperationCache<>(baseTimeout.multipliedBy(10), manualTicker);
        completedOperationCache.registerOngoingOperation(TEST_OPERATION_KEY, TEST_OPERATION_RESULT);

        manualTicker.advanceTime(baseTimeout.multipliedBy(2).getSeconds(), TimeUnit.SECONDS);

        assertThat(completedOperationCache.get(TEST_OPERATION_KEY)).isPresent();
    }

    @Test
    void containsReturnsFalseForUnknownOperation() {
        assertThat(completedOperationCache.containsOperation(TEST_OPERATION_KEY)).isFalse();
    }

    @Test
    void containsChecksOnoingOperations() {
        completedOperationCache.registerOngoingOperation(
                TEST_OPERATION_KEY, new CompletableFuture<>());
        assertThat(completedOperationCache.containsOperation(TEST_OPERATION_KEY)).isTrue();
    }

    @Test
    void containsChecksCompletedOperations() {
        completedOperationCache.registerOngoingOperation(
                TEST_OPERATION_KEY, CompletableFuture.completedFuture(null));
        assertThat(completedOperationCache.containsOperation(TEST_OPERATION_KEY)).isTrue();
    }

    @Test
    void containsDoesNotMarkResultAsAccessed() {
        completedOperationCache.registerOngoingOperation(
                TEST_OPERATION_KEY, CompletableFuture.completedFuture(null));
        assertThat(completedOperationCache.containsOperation(TEST_OPERATION_KEY)).isTrue();

        assertThatFuture(completedOperationCache.closeAsync())
                .willNotCompleteWithin(Duration.ofMillis(10));
    }
}
