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
import org.apache.flink.model.triton.TritonOptions.SequenceIdCounterInitStrategy;
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
 * Validates the constructor-time validation that {@link AbstractTritonModelFunction} performs for
 * the {@link TritonOptions#SEQUENCE_ID_AUTO_INCREMENT} feature.
 *
 * <p>Two invariants are guarded:
 *
 * <ol>
 *   <li>{@code sequence-id-auto-increment=true} is only meaningful with a base {@code sequence-id}
 *       — without it there is nothing to suffix.
 *   <li>Every generated ID is a one-shot stateful sequence, so both {@code sequence-start} and
 *       {@code sequence-end} must be true. Otherwise the Triton server-side sequence batcher would
 *       either receive uninitialized state (no start) or leak per-sequence slots (no end), neither
 *       of which is recoverable from the Flink side.
 * </ol>
 *
 * <p>Failing fast in the constructor — instead of at the first inference call — surfaces the
 * misconfiguration during job submission, where the user can still react. The validations are
 * narrow and worth testing because both error paths produce server-side resource exhaustion
 * symptoms that are extremely hard to diagnose from Flink logs.
 */
class TritonSequenceIdAutoIncrementTest {

    private static final ResolvedSchema INPUT_SCHEMA =
            ResolvedSchema.of(Column.physical("input", DataTypes.STRING()));

    private static final ResolvedSchema OUTPUT_SCHEMA =
            ResolvedSchema.of(Column.physical("output", DataTypes.STRING()));

    @Test
    void testAutoIncrementWithoutSequenceIdIsRejected() {
        // sequence-id is the prefix that auto-increment appends to. Without it, the generated
        // ID would be "-{subtask}-{counter}" — a leading dash that confuses log parsers and is
        // almost certainly user error rather than intent.
        Map<String, String> options = baseOptions();
        options.put(TritonOptions.SEQUENCE_ID_AUTO_INCREMENT.key(), "true");
        // Both start/end true so we isolate the missing-sequence-id failure mode.
        options.put(TritonOptions.SEQUENCE_START.key(), "true");
        options.put(TritonOptions.SEQUENCE_END.key(), "true");

        ModelProviderFactory.Context context =
                FactoryMocks.createModelContext(INPUT_SCHEMA, OUTPUT_SCHEMA, options);

        assertThatThrownBy(() -> new TritonInferenceModelFunction(context, contextConfig(context)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(TritonOptions.SEQUENCE_ID.key());
    }

    @Test
    void testAutoIncrementRequiresSequenceStart() {
        // sequence-start=false would make Triton route the request to model state that was
        // never initialized for this fresh sequence ID; the inference result would either be
        // garbage or an explicit error from the sequence batcher.
        Map<String, String> options = baseOptions();
        options.put(TritonOptions.SEQUENCE_ID.key(), "job-123");
        options.put(TritonOptions.SEQUENCE_ID_AUTO_INCREMENT.key(), "true");
        options.put(TritonOptions.SEQUENCE_START.key(), "false");
        options.put(TritonOptions.SEQUENCE_END.key(), "true");

        ModelProviderFactory.Context context =
                FactoryMocks.createModelContext(INPUT_SCHEMA, OUTPUT_SCHEMA, options);

        assertThatThrownBy(() -> new TritonInferenceModelFunction(context, contextConfig(context)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sequence-start");
    }

    @Test
    void testAutoIncrementRequiresSequenceEnd() {
        // The flip side of the test above. Without sequence-end the per-sequence state slot on
        // the Triton server is never released, so a long-running job will eventually exhaust
        // the configured max sequence count and start failing requests — a slow leak that
        // would only show up in production load.
        Map<String, String> options = baseOptions();
        options.put(TritonOptions.SEQUENCE_ID.key(), "job-123");
        options.put(TritonOptions.SEQUENCE_ID_AUTO_INCREMENT.key(), "true");
        options.put(TritonOptions.SEQUENCE_START.key(), "true");
        options.put(TritonOptions.SEQUENCE_END.key(), "false");

        ModelProviderFactory.Context context =
                FactoryMocks.createModelContext(INPUT_SCHEMA, OUTPUT_SCHEMA, options);

        assertThatThrownBy(() -> new TritonInferenceModelFunction(context, contextConfig(context)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sequence-end");
    }

    @Test
    void testAutoIncrementHappyPath() {
        // The combination the documentation actually recommends. Construction must succeed so
        // that the validations above cannot regress into rejecting a legitimate configuration.
        Map<String, String> options = baseOptions();
        options.put(TritonOptions.SEQUENCE_ID.key(), "job-123");
        options.put(TritonOptions.SEQUENCE_ID_AUTO_INCREMENT.key(), "true");
        options.put(TritonOptions.SEQUENCE_START.key(), "true");
        options.put(TritonOptions.SEQUENCE_END.key(), "true");

        ModelProviderFactory.Context context =
                FactoryMocks.createModelContext(INPUT_SCHEMA, OUTPUT_SCHEMA, options);

        TritonInferenceModelFunction function =
                new TritonInferenceModelFunction(context, contextConfig(context));
        assertThat(function).isNotNull();
    }

    @Test
    void testAutoIncrementDisabledDoesNotEnforceStartEnd() {
        // Regression guard: the start/end requirement must apply ONLY when auto-increment is
        // enabled. When auto-increment is off, the user retains full control over sequence
        // semantics and may legitimately use sequence-start/end=false (e.g. all records share
        // a single long-lived sequence whose start/end is signalled out-of-band).
        Map<String, String> options = baseOptions();
        options.put(TritonOptions.SEQUENCE_ID.key(), "job-123");
        // SEQUENCE_ID_AUTO_INCREMENT defaults to false.
        options.put(TritonOptions.SEQUENCE_START.key(), "false");
        options.put(TritonOptions.SEQUENCE_END.key(), "false");

        ModelProviderFactory.Context context =
                FactoryMocks.createModelContext(INPUT_SCHEMA, OUTPUT_SCHEMA, options);

        TritonInferenceModelFunction function =
                new TritonInferenceModelFunction(context, contextConfig(context));
        assertThat(function).isNotNull();
    }

    @Test
    void testCounterInitStrategyDefaultsToZero() {
        // Default-value contract: if the user does not configure
        // sequence-id-counter-init-strategy, ZERO must be selected to preserve the original
        // (pre-strategy) behaviour for existing configurations.
        SequenceIdCounterInitStrategy defaultStrategy =
                TritonOptions.SEQUENCE_ID_COUNTER_INIT_STRATEGY.defaultValue();
        assertThat(defaultStrategy).isEqualTo(SequenceIdCounterInitStrategy.ZERO);
    }

    @Test
    void testCounterInitStrategyAcceptsAllEnumValues() {
        // The strategy is only validated by the enum codec itself, but we still verify that
        // none of the enum constants accidentally trip the constructor (e.g. by interacting
        // with another validation rule).
        for (SequenceIdCounterInitStrategy strategy : SequenceIdCounterInitStrategy.values()) {
            Map<String, String> options = baseOptions();
            options.put(TritonOptions.SEQUENCE_ID.key(), "job-123");
            options.put(TritonOptions.SEQUENCE_ID_AUTO_INCREMENT.key(), "true");
            options.put(TritonOptions.SEQUENCE_START.key(), "true");
            options.put(TritonOptions.SEQUENCE_END.key(), "true");
            options.put(TritonOptions.SEQUENCE_ID_COUNTER_INIT_STRATEGY.key(), strategy.name());

            ModelProviderFactory.Context context =
                    FactoryMocks.createModelContext(INPUT_SCHEMA, OUTPUT_SCHEMA, options);
            TritonInferenceModelFunction function =
                    new TritonInferenceModelFunction(context, contextConfig(context));
            assertThat(function)
                    .as("strategy %s must construct successfully", strategy)
                    .isNotNull();
        }
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
