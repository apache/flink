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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.factories.utils.FactoryMocks;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Validates that retry-config validation in {@link AbstractTritonModelFunction}'s constructor is
 * gated on whether retries are actually enabled.
 *
 * <p>Background: previously the backoff-duration checks ran unconditionally, so a user who
 * explicitly disabled retries with {@code max-retries=0} would still be rejected if their
 * (otherwise unused) backoff settings were degenerate. Backoff durations only matter when {@code
 * maxRetries > 0}, so validating them with retries disabled is wrong.
 */
class TritonRetryConfigValidationTest {

    private static final ResolvedSchema INPUT_SCHEMA =
            ResolvedSchema.of(Column.physical("input", DataTypes.STRING()));

    private static final ResolvedSchema OUTPUT_SCHEMA =
            ResolvedSchema.of(Column.physical("output", DataTypes.STRING()));

    @Test
    void testZeroRetriesSkipsBackoffValidation() {
        // With retries disabled, retry-initial-backoff=0ms is meaningless but harmless: the
        // backoff is never consulted. The constructor must accept it instead of failing the job
        // submission for an unused setting.
        Map<String, String> options = baseOptions();
        options.put(TritonOptions.MAX_RETRIES.key(), "0");
        options.put(TritonOptions.RETRY_INITIAL_BACKOFF.key(), "0 ms");

        ModelProviderFactory.Context context =
                FactoryMocks.createModelContext(INPUT_SCHEMA, OUTPUT_SCHEMA, options);

        // Construction must succeed; previously this threw IllegalArgumentException.
        TritonInferenceModelFunction function =
                new TritonInferenceModelFunction(context, contextConfig(context));
        assertThat(function).isNotNull();
    }

    @Test
    void testZeroRetriesSkipsMaxBackoffValidation() {
        // Same rationale as above for retry-max-backoff. Note we also set the initial backoff
        // to a positive value to isolate the max-backoff check.
        Map<String, String> options = baseOptions();
        options.put(TritonOptions.MAX_RETRIES.key(), "0");
        options.put(TritonOptions.RETRY_INITIAL_BACKOFF.key(), "100 ms");
        options.put(TritonOptions.RETRY_MAX_BACKOFF.key(), "0 ms");

        ModelProviderFactory.Context context =
                FactoryMocks.createModelContext(INPUT_SCHEMA, OUTPUT_SCHEMA, options);

        TritonInferenceModelFunction function =
                new TritonInferenceModelFunction(context, contextConfig(context));
        assertThat(function).isNotNull();
    }

    @Test
    void testRetriesEnabledStillRejectsZeroBackoff() {
        // Regression guard for the gating: as soon as retries are enabled, backoff validation
        // must run. Otherwise a misconfigured initial-backoff would silently produce zero-delay
        // retry storms in production.
        Map<String, String> options = baseOptions();
        options.put(TritonOptions.MAX_RETRIES.key(), "3");
        options.put(TritonOptions.RETRY_INITIAL_BACKOFF.key(), "0 ms");

        ModelProviderFactory.Context context =
                FactoryMocks.createModelContext(INPUT_SCHEMA, OUTPUT_SCHEMA, options);

        assertThatThrownBy(() -> new TritonInferenceModelFunction(context, contextConfig(context)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(TritonOptions.RETRY_INITIAL_BACKOFF.key());
    }

    @Test
    void testRetriesEnabledStillRejectsMaxBackoffSmallerThanInitial() {
        Map<String, String> options = baseOptions();
        options.put(TritonOptions.MAX_RETRIES.key(), "3");
        options.put(TritonOptions.RETRY_INITIAL_BACKOFF.key(), "500 ms");
        options.put(TritonOptions.RETRY_MAX_BACKOFF.key(), "100 ms");

        ModelProviderFactory.Context context =
                FactoryMocks.createModelContext(INPUT_SCHEMA, OUTPUT_SCHEMA, options);

        assertThatThrownBy(() -> new TritonInferenceModelFunction(context, contextConfig(context)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(TritonOptions.RETRY_MAX_BACKOFF.key());
    }

    @Test
    void testNegativeMaxRetriesAlwaysRejected() {
        // The maxRetries >= 0 invariant guards the gate itself; without it, a negative value
        // would skip backoff validation AND blow up downstream (e.g. 1L << -1 in the backoff
        // calculation). Verify it's still enforced.
        Map<String, String> options = baseOptions();
        options.put(TritonOptions.MAX_RETRIES.key(), "-1");

        ModelProviderFactory.Context context =
                FactoryMocks.createModelContext(INPUT_SCHEMA, OUTPUT_SCHEMA, options);

        assertThatThrownBy(() -> new TritonInferenceModelFunction(context, contextConfig(context)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(TritonOptions.MAX_RETRIES.key());
    }

    private static Map<String, String> baseOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("provider", TritonModelProviderFactory.IDENTIFIER);
        options.put(TritonOptions.ENDPOINT.key(), "http://localhost:8000");
        options.put(TritonOptions.MODEL_NAME.key(), "test-model");
        return options;
    }

    private static Configuration contextConfig(ModelProviderFactory.Context context) {
        Configuration config = new Configuration();
        context.getCatalogModel().getOptions().forEach(config::setString);
        return config;
    }
}
