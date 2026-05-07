/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.model.triton;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that the retry / default-value {@link TritonOptions} expose the contracts users rely on:
 * stable option keys (changing them silently breaks pipelines) and the documented default values.
 */
class TritonRetryOptionsTest {

    @Test
    void testMaxRetriesKeyAndDefault() {
        assertThat(TritonOptions.MAX_RETRIES.key()).isEqualTo("max-retries");
        assertThat(TritonOptions.MAX_RETRIES.defaultValue()).isZero();
    }

    @Test
    void testRetryInitialBackoffKeyAndDefault() {
        assertThat(TritonOptions.RETRY_INITIAL_BACKOFF.key()).isEqualTo("retry-initial-backoff");
        assertThat(TritonOptions.RETRY_INITIAL_BACKOFF.defaultValue())
                .isEqualTo(Duration.ofMillis(100));
    }

    @Test
    void testRetryMaxBackoffKeyAndDefault() {
        assertThat(TritonOptions.RETRY_MAX_BACKOFF.key()).isEqualTo("retry-max-backoff");
        assertThat(TritonOptions.RETRY_MAX_BACKOFF.defaultValue())
                .isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void testDefaultValueKeyAndNoDefault() {
        assertThat(TritonOptions.DEFAULT_VALUE.key()).isEqualTo("default-value");
        // When not configured, default-value must be null so the operator can distinguish
        // "fallback enabled" from "no fallback configured".
        assertThat(TritonOptions.DEFAULT_VALUE.defaultValue()).isNull();
    }

    @Test
    void testRetryInitialBackoffDefaultIsLessThanOrEqualToMax() {
        // Regression guard for a subtle misconfiguration: if the default of RETRY_INITIAL_BACKOFF
        // ever exceeded RETRY_MAX_BACKOFF's default, every out-of-the-box job would fail
        // validation in AbstractTritonModelFunction's constructor.
        assertThat(TritonOptions.RETRY_INITIAL_BACKOFF.defaultValue())
                .isLessThanOrEqualTo(TritonOptions.RETRY_MAX_BACKOFF.defaultValue());
    }
}
