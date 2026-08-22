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
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.ml.AsyncPredictRuntimeProvider;
import org.apache.flink.table.ml.ModelProvider;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TritonModelProviderFactory}. */
class TritonModelProviderFactoryTest {

    private static final ResolvedSchema INPUT_SCHEMA =
            ResolvedSchema.of(Column.physical("input", DataTypes.STRING()));

    private static final ResolvedSchema OUTPUT_SCHEMA =
            ResolvedSchema.of(Column.physical("output", DataTypes.STRING()));

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
                .hasSize(16)
                .containsExactlyInAnyOrder(
                        TritonOptions.MODEL_VERSION,
                        TritonOptions.TIMEOUT,
                        TritonOptions.FLATTEN_BATCH_DIM,
                        TritonOptions.PRIORITY,
                        TritonOptions.SEQUENCE_ID,
                        TritonOptions.SEQUENCE_START,
                        TritonOptions.SEQUENCE_END,
                        TritonOptions.SEQUENCE_ID_AUTO_INCREMENT,
                        TritonOptions.SEQUENCE_ID_COUNTER_INIT_STRATEGY,
                        TritonOptions.COMPRESSION,
                        TritonOptions.AUTH_TOKEN,
                        TritonOptions.CUSTOM_HEADERS,
                        TritonOptions.MAX_RETRIES,
                        TritonOptions.RETRY_INITIAL_BACKOFF,
                        TritonOptions.RETRY_MAX_BACKOFF,
                        TritonOptions.DEFAULT_VALUE);
    }

    /**
     * End-to-end guard that the auto-increment options pass {@code helper.validate()} and reach the
     * function. Going through {@link FactoryMocks#createModelProvider} exercises the full SPI +
     * factory chain — the unit tests that construct the function directly would not catch a missing
     * entry in {@link TritonModelProviderFactory#optionalOptions()}.
     */
    @Test
    void testSequenceIdAutoIncrementOptionsPassFactoryValidation() {
        Map<String, String> options = new HashMap<>();
        options.put("provider", TritonModelProviderFactory.IDENTIFIER);
        options.put(TritonOptions.ENDPOINT.key(), "http://localhost:8000");
        options.put(TritonOptions.MODEL_NAME.key(), "test-model");
        options.put(TritonOptions.SEQUENCE_ID.key(), "job-123");
        options.put(TritonOptions.SEQUENCE_START.key(), "true");
        options.put(TritonOptions.SEQUENCE_END.key(), "true");
        options.put(TritonOptions.SEQUENCE_ID_AUTO_INCREMENT.key(), "true");
        options.put(TritonOptions.SEQUENCE_ID_COUNTER_INIT_STRATEGY.key(), "NANO_TIME");

        ModelProvider provider =
                FactoryMocks.createModelProvider(INPUT_SCHEMA, OUTPUT_SCHEMA, options);
        assertThat(provider).isNotNull().isInstanceOf(AsyncPredictRuntimeProvider.class);

        // Provider#createAsyncPredictFunction ignores its Context argument.
        AsyncPredictFunction function =
                ((AsyncPredictRuntimeProvider) provider).createAsyncPredictFunction(null);
        assertThat(function).isInstanceOf(TritonInferenceModelFunction.class);

        // Catches a silent drop where keys validate but the constructor never reads them.
        TritonInferenceModelFunction tritonFn = (TritonInferenceModelFunction) function;
        assertThat(tritonFn.isSequenceIdAutoIncrement()).isTrue();
        assertThat(tritonFn.getSequenceId()).isEqualTo("job-123");
    }
}
