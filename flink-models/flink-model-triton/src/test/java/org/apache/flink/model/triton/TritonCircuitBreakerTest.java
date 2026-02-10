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

package org.apache.flink.model.triton;

import org.apache.flink.model.triton.exception.TritonCircuitBreakerOpenException;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TritonCircuitBreaker}. */
class TritonCircuitBreakerTest {

    @Test
    void testCircuitBreakerStartsInClosedState() {
        TritonCircuitBreaker breaker =
                new TritonCircuitBreaker("http://localhost:8000", 0.5, Duration.ofSeconds(1), 3);

        assertThat(breaker.getState()).isEqualTo(TritonCircuitBreaker.State.CLOSED);
        assertThat(breaker.allowRequest()).isTrue();
    }

    @Test
    void testCircuitOpensAfterThresholdExceeded() {
        TritonCircuitBreaker breaker =
                new TritonCircuitBreaker("http://localhost:8000", 0.5, Duration.ofSeconds(60), 3);

        // Send 10 requests with 6 failures (60% failure rate)
        for (int i = 0; i < 10; i++) {
            breaker.allowRequest();
            if (i < 6) {
                breaker.recordFailure();
            } else {
                breaker.recordSuccess();
            }
        }

        // Circuit should now be OPEN
        assertThat(breaker.getState()).isEqualTo(TritonCircuitBreaker.State.OPEN);
        assertThatThrownBy(() -> breaker.allowRequest())
                .isInstanceOf(TritonCircuitBreakerOpenException.class)
                .hasMessageContaining("Circuit breaker is OPEN");
    }

    @Test
    void testCircuitRemainsClosedWithLowFailureRate() {
        TritonCircuitBreaker breaker =
                new TritonCircuitBreaker("http://localhost:8000", 0.5, Duration.ofSeconds(60), 3);

        // Send 10 requests with only 4 failures (40% failure rate, below 50% threshold)
        for (int i = 0; i < 10; i++) {
            breaker.allowRequest();
            if (i < 4) {
                breaker.recordFailure();
            } else {
                breaker.recordSuccess();
            }
        }

        // Circuit should still be CLOSED
        assertThat(breaker.getState()).isEqualTo(TritonCircuitBreaker.State.CLOSED);
        assertThat(breaker.allowRequest()).isTrue();
    }

    @Test
    void testCircuitDoesNotOpenWithFewRequests() {
        TritonCircuitBreaker breaker =
                new TritonCircuitBreaker("http://localhost:8000", 0.5, Duration.ofSeconds(60), 3);

        // Send only 5 requests with 100% failure rate
        // Circuit should NOT open because we need minimum 10 requests
        for (int i = 0; i < 5; i++) {
            breaker.allowRequest();
            breaker.recordFailure();
        }

        // Circuit should still be CLOSED (not enough samples)
        assertThat(breaker.getState()).isEqualTo(TritonCircuitBreaker.State.CLOSED);
        assertThat(breaker.getCurrentFailureRate()).isEqualTo(1.0);
    }

    @Test
    void testCircuitTransitionsToHalfOpenAfterTimeout() throws InterruptedException {
        TritonCircuitBreaker breaker =
                new TritonCircuitBreaker(
                        "http://localhost:8000", 0.5, Duration.ofMillis(100), 3);

        // Open the circuit
        for (int i = 0; i < 10; i++) {
            breaker.allowRequest();
            breaker.recordFailure();
        }

        assertThat(breaker.getState()).isEqualTo(TritonCircuitBreaker.State.OPEN);

        // Wait for timeout
        Thread.sleep(150);

        // Next request should transition to HALF_OPEN
        assertThat(breaker.allowRequest()).isTrue();
        assertThat(breaker.getState()).isEqualTo(TritonCircuitBreaker.State.HALF_OPEN);
    }

    @Test
    void testHalfOpenAllowsLimitedRequests() throws InterruptedException {
        TritonCircuitBreaker breaker =
                new TritonCircuitBreaker(
                        "http://localhost:8000", 0.5, Duration.ofMillis(100), 3);

        // Open the circuit
        for (int i = 0; i < 10; i++) {
            breaker.allowRequest();
            breaker.recordFailure();
        }

        // Wait for transition to HALF_OPEN
        Thread.sleep(150);

        // Should allow exactly 3 requests (halfOpenMaxRequests)
        assertThat(breaker.allowRequest()).isTrue(); // 1st request
        assertThat(breaker.allowRequest()).isTrue(); // 2nd request
        assertThat(breaker.allowRequest()).isTrue(); // 3rd request

        // 4th request should be rejected
        assertThatThrownBy(() -> breaker.allowRequest())
                .isInstanceOf(TritonCircuitBreakerOpenException.class)
                .hasMessageContaining("HALF_OPEN");
    }

    @Test
    void testHalfOpenClosesAfterSuccessfulProbes() throws InterruptedException {
        TritonCircuitBreaker breaker =
                new TritonCircuitBreaker(
                        "http://localhost:8000", 0.5, Duration.ofMillis(100), 3);

        // Open the circuit
        for (int i = 0; i < 10; i++) {
            breaker.allowRequest();
            breaker.recordFailure();
        }

        // Wait for transition to HALF_OPEN
        Thread.sleep(150);

        // Send 3 successful probe requests
        for (int i = 0; i < 3; i++) {
            breaker.allowRequest();
            breaker.recordSuccess();
        }

        // Circuit should now be CLOSED again
        assertThat(breaker.getState()).isEqualTo(TritonCircuitBreaker.State.CLOSED);
        assertThat(breaker.allowRequest()).isTrue();
    }

    @Test
    void testHalfOpenReopensOnFailure() throws InterruptedException {
        TritonCircuitBreaker breaker =
                new TritonCircuitBreaker(
                        "http://localhost:8000", 0.5, Duration.ofMillis(100), 3);

        // Open the circuit
        for (int i = 0; i < 10; i++) {
            breaker.allowRequest();
            breaker.recordFailure();
        }

        // Wait for transition to HALF_OPEN
        Thread.sleep(150);

        // First probe request succeeds
        breaker.allowRequest();
        breaker.recordSuccess();

        // Second probe request fails - should reopen circuit
        breaker.allowRequest();
        breaker.recordFailure();

        // Circuit should be OPEN again
        assertThat(breaker.getState()).isEqualTo(TritonCircuitBreaker.State.OPEN);
    }

    @Test
    void testManualReset() {
        TritonCircuitBreaker breaker =
                new TritonCircuitBreaker("http://localhost:8000", 0.5, Duration.ofSeconds(60), 3);

        // Open the circuit
        for (int i = 0; i < 10; i++) {
            breaker.allowRequest();
            breaker.recordFailure();
        }

        assertThat(breaker.getState()).isEqualTo(TritonCircuitBreaker.State.OPEN);

        // Manual reset
        breaker.reset();

        // Circuit should be CLOSED
        assertThat(breaker.getState()).isEqualTo(TritonCircuitBreaker.State.CLOSED);
        assertThat(breaker.getTotalRequests()).isEqualTo(0);
        assertThat(breaker.getFailedRequests()).isEqualTo(0);
    }

    @Test
    void testMetricsTracking() {
        TritonCircuitBreaker breaker =
                new TritonCircuitBreaker("http://localhost:8000", 0.5, Duration.ofSeconds(60), 3);

        // Record some requests
        for (int i = 0; i < 20; i++) {
            breaker.allowRequest();
            if (i % 3 == 0) {
                breaker.recordFailure();
            } else {
                breaker.recordSuccess();
            }
        }

        assertThat(breaker.getTotalRequests()).isEqualTo(20);
        assertThat(breaker.getFailedRequests()).isEqualTo(7); // Every 3rd request fails
        assertThat(breaker.getCurrentFailureRate()).isCloseTo(0.35, within(0.01));
    }

    private static org.assertj.core.data.Offset<Double> within(double offset) {
        return org.assertj.core.data.Offset.offset(offset);
    }
}
