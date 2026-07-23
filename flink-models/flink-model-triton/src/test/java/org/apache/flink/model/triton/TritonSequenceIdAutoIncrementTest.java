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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.factories.utils.FactoryMocks;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests {@link AbstractTritonModelFunction}'s constructor-time validation and runtime behaviour for
 * {@link TritonOptions#SEQUENCE_ID_AUTO_INCREMENT}. Failing fast at job submission is preferred
 * over failing at the first inference call because the underlying error modes (unknown sequence
 * state, leaked Triton slots) surface as opaque server-side resource exhaustion in production.
 */
class TritonSequenceIdAutoIncrementTest {

    private static final ResolvedSchema INPUT_SCHEMA =
            ResolvedSchema.of(Column.physical("input", DataTypes.STRING()));

    private static final ResolvedSchema OUTPUT_SCHEMA =
            ResolvedSchema.of(Column.physical("output", DataTypes.STRING()));

    @Test
    void testAutoIncrementWithoutSequenceIdIsRejected() {
        // No base sequence-id → generated ID would start with a leading dash; almost certainly a
        // user error rather than intent.
        Map<String, String> options = baseOptions();
        options.put(TritonOptions.SEQUENCE_ID_AUTO_INCREMENT.key(), "true");
        // start/end true so we isolate the missing-sequence-id failure mode.
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
        // sequence-start=false routes to uninitialized server-side state for a fresh ID.
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
        // sequence-end=false leaks per-sequence slots — slow leak only visible under prod load.
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
        // Guards against the validations above regressing into rejecting a legitimate config.
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
        // start/end requirement applies ONLY when auto-increment is on; otherwise the user
        // retains full control (e.g. one long-lived sequence signalled out-of-band).
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
        // Default ZERO preserves pre-strategy behaviour for existing configurations.
        SequenceIdCounterInitStrategy defaultStrategy =
                TritonOptions.SEQUENCE_ID_COUNTER_INIT_STRATEGY.defaultValue();
        assertThat(defaultStrategy).isEqualTo(SequenceIdCounterInitStrategy.ZERO);
    }

    @Test
    void testCounterInitStrategyAcceptsAllEnumValues() {
        // Verify no enum constant accidentally trips another constructor validation.
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

    @Test
    void testNextEffectiveSequenceIdAdvancesOnEachCall() throws Exception {
        // Each call consumes one counter slot, yielding strictly monotonic IDs. This is what
        // lets the caller pre-generate once at the top of asyncPredict and thread through retries.
        TritonInferenceModelFunction function = newAutoIncrementFunction();
        seedAutoIncrementState(function, /* startCounter */ 0L, /* subtask */ 0);

        String id0 = function.nextEffectiveSequenceId();
        String id1 = function.nextEffectiveSequenceId();
        String id2 = function.nextEffectiveSequenceId();

        assertThat(id0).isEqualTo("job-123-0-0");
        assertThat(id1).isEqualTo("job-123-0-1");
        assertThat(id2).isEqualTo("job-123-0-2");
    }

    @Test
    void testNextEffectiveSequenceIdWithoutAutoIncrementIsStable() {
        // Auto-increment off → return base ID verbatim, no counter, no side effects.
        Map<String, String> options = baseOptions();
        options.put(TritonOptions.SEQUENCE_ID.key(), "job-123");
        ModelProviderFactory.Context context =
                FactoryMocks.createModelContext(INPUT_SCHEMA, OUTPUT_SCHEMA, options);
        TritonInferenceModelFunction function =
                new TritonInferenceModelFunction(context, contextConfig(context));

        assertThat(function.nextEffectiveSequenceId()).isEqualTo("job-123");
        assertThat(function.nextEffectiveSequenceId()).isEqualTo("job-123");
    }

    @Test
    void testBuildInferenceRequestDoesNotAdvanceCounter() throws Exception {
        // Regression guard: buildInferenceRequest runs on every retry attempt and must reuse the
        // caller-supplied ID — generating fresh IDs here would leak Triton sequence slots. See
        // nextEffectiveSequenceId() javadoc for the full slot-leak rationale.
        TritonInferenceModelFunction function = newAutoIncrementFunction();
        AtomicLong counter = seedAutoIncrementState(function, 7L, 0);

        String fixedId = "job-123-0-7";
        RowData row = GenericRowData.of(BinaryStringData.fromString("hello"));

        String firstBody = function.buildInferenceRequest(row, fixedId);
        String secondBody = function.buildInferenceRequest(row, fixedId);

        ObjectMapper mapper = new ObjectMapper();
        JsonNode first = mapper.readTree(firstBody);
        JsonNode second = mapper.readTree(secondBody);

        assertThat(first.get("id").asText())
                .as("first attempt uses the caller-supplied effective sequence ID")
                .isEqualTo(fixedId);
        assertThat(second.get("id").asText())
                .as("retry attempt must reuse the same ID — otherwise Triton slots leak")
                .isEqualTo(fixedId);
        assertThat(counter.get())
                .as("buildInferenceRequest must not advance the counter")
                .isEqualTo(7L);
    }

    private static TritonInferenceModelFunction newAutoIncrementFunction() {
        Map<String, String> options = baseOptions();
        options.put(TritonOptions.SEQUENCE_ID.key(), "job-123");
        options.put(TritonOptions.SEQUENCE_ID_AUTO_INCREMENT.key(), "true");
        options.put(TritonOptions.SEQUENCE_START.key(), "true");
        options.put(TritonOptions.SEQUENCE_END.key(), "true");
        ModelProviderFactory.Context context =
                FactoryMocks.createModelContext(INPUT_SCHEMA, OUTPUT_SCHEMA, options);
        return new TritonInferenceModelFunction(context, contextConfig(context));
    }

    /**
     * Seeds the transient state {@link AbstractTritonModelFunction#open} would populate. Reflection
     * (rather than calling {@code open()}) avoids depending on {@code MockStreamingRuntimeContext}
     * from the flink-streaming-java test-jar.
     */
    private static AtomicLong seedAutoIncrementState(
            TritonInferenceModelFunction function, long startCounter, int subtask)
            throws Exception {
        AtomicLong counter = new AtomicLong(startCounter);
        Field counterField = AbstractTritonModelFunction.class.getDeclaredField("sequenceCounter");
        counterField.setAccessible(true);
        counterField.set(function, counter);

        Field subtaskField = AbstractTritonModelFunction.class.getDeclaredField("subtaskIndex");
        subtaskField.setAccessible(true);
        subtaskField.setInt(function, subtask);

        return counter;
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
