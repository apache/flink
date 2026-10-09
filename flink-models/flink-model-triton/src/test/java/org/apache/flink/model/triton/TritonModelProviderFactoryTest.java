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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.ml.AsyncPredictRuntimeProvider;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TritonModelProviderFactory}. */
class TritonModelProviderFactoryTest {

    @Test
    void testFactoryIdentifier() {
        TritonModelProviderFactory factory = new TritonModelProviderFactory();
        assertThat(factory.factoryIdentifier()).isEqualTo(TritonModelProviderFactory.IDENTIFIER);
    }

    @Test
    void testRequiredOptions() {
        TritonModelProviderFactory factory = new TritonModelProviderFactory();
        assertThat(factory.requiredOptions())
                .hasSize(2)
                .containsExactlyInAnyOrder(TritonOptions.ENDPOINT, TritonOptions.MODEL_NAME);
    }

    @Test
    void testOptionalOptions() {
        TritonModelProviderFactory factory = new TritonModelProviderFactory();
        assertThat(factory.optionalOptions())
                .hasSize(20)
                .containsExactlyInAnyOrder(
                        TritonOptions.MODEL_VERSION,
                        TritonOptions.TIMEOUT,
                        TritonOptions.FLATTEN_BATCH_DIM,
                        TritonOptions.PRIORITY,
                        TritonOptions.SEQUENCE_ID,
                        TritonOptions.SEQUENCE_START,
                        TritonOptions.SEQUENCE_END,
                        TritonOptions.COMPRESSION,
                        TritonOptions.AUTH_TOKEN,
                        TritonOptions.CUSTOM_HEADERS,
                        TritonOptions.MAX_RETRIES,
                        TritonOptions.RETRY_INITIAL_BACKOFF,
                        TritonOptions.RETRY_MAX_BACKOFF,
                        TritonOptions.DEFAULT_VALUE,
                        TritonOptions.HEALTH_CHECK_ENABLED,
                        TritonOptions.HEALTH_CHECK_INTERVAL,
                        TritonOptions.CIRCUIT_BREAKER_ENABLED,
                        TritonOptions.CIRCUIT_BREAKER_FAILURE_THRESHOLD,
                        TritonOptions.CIRCUIT_BREAKER_TIMEOUT,
                        TritonOptions.CIRCUIT_BREAKER_HALF_OPEN_REQUESTS);
    }

    @Test
    void testCreateProviderWithHealthCheckAndCircuitBreaker() {
        final TritonModelProviderFactory factory = new TritonModelProviderFactory();
        final ResolvedSchema schema =
                ResolvedSchema.of(Column.physical("text", DataTypes.STRING()));
        final Map<String, String> options =
                Map.of(
                        "provider", "triton",
                        "endpoint", "http://localhost:8000",
                        "model-name", "test-model",
                        "health-check-enabled", "true",
                        "health-check-interval", "10 s",
                        "circuit-breaker-enabled", "true",
                        "circuit-breaker-failure-threshold", "0.25",
                        "circuit-breaker-timeout", "5 s",
                        "circuit-breaker-half-open-requests", "2");

        assertThat(
                        factory.createModelProvider(
                                FactoryMocks.createModelContext(schema, schema, options)))
                .isInstanceOf(AsyncPredictRuntimeProvider.class);
    }
}
