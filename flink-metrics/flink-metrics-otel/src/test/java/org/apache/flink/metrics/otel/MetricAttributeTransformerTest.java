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

package org.apache.flink.metrics.otel;

import org.apache.flink.metrics.MetricConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Unit tests for {@link MetricAttributeTransformer}. */
class MetricAttributeTransformerTest {

    private static final String TEST_VALUE = "test-attribute-value";

    // Attribute that should be truncated to the global limit
    private static final String GLOBAL_TRUNCATED_KEY = "global_truncated_key";
    private static final int GLOBAL_LIMIT = 5;
    private static final String GLOBAL_TRUNCATED_EXPECTED = TEST_VALUE.substring(0, GLOBAL_LIMIT);

    // Attribute with a per-attribute limit smaller than the global limit
    private static final String CUSTOM_SMALL_KEY = "custom_small_key";
    private static final int CUSTOM_SMALL_LIMIT = 3;
    private static final String CUSTOM_SMALL_EXPECTED = TEST_VALUE.substring(0, CUSTOM_SMALL_LIMIT);

    // Attribute with a per-attribute limit larger than the global limit
    private static final String CUSTOM_LARGE_KEY = "custom_large_key";
    private static final int CUSTOM_LARGE_LIMIT = GLOBAL_LIMIT + CUSTOM_SMALL_LIMIT; // 8
    private static final String CUSTOM_LARGE_EXPECTED = TEST_VALUE.substring(0, CUSTOM_LARGE_LIMIT);

    // Attribute whose value is already shorter than the global limit
    private static final String NON_TRUNCATED_KEY = "non_truncated_key";
    private static final String NON_TRUNCATED_VALUE = TEST_VALUE.substring(0, GLOBAL_LIMIT - 1);

    // Attribute with a zero limit (should be dropped)
    private static final String ZERO_KEY = "zero_key";

    // Attribute with a negative limit (no truncation, overrides global)
    private static final String NEGATIVE_KEY = "negative_key";
    private static final int NEGATIVE_LIMIT = -GLOBAL_LIMIT;

    // Attribute with an Integer-typed config value — verifies MetricConfig.getInteger tolerates
    // non-String values stored directly in the Properties map.
    private static final String NUMERIC_KEY = "numeric_key";
    private static final int NUMERIC_LIMIT = 42;

    // Keys with invalid (non-integer) values that should be skipped
    private static final String INVALID_STRING_KEY = "invalid_string";
    private static final String INVALID_DURATION_KEY = "invalid_duration";

    @Test
    void testLimitsParsingFromConfiguration() {
        final MetricConfig cfg = buildFullTestConfig();
        final MetricAttributeTransformer transformer = new MetricAttributeTransformer(cfg);

        assertThat(transformer.getAttributeValueLimits())
                .containsEntry(MetricAttributeTransformer.GLOBAL_LIMIT_KEY, GLOBAL_LIMIT)
                .containsEntry(CUSTOM_SMALL_KEY, CUSTOM_SMALL_LIMIT)
                .containsEntry(CUSTOM_LARGE_KEY, CUSTOM_LARGE_LIMIT)
                .containsEntry(ZERO_KEY, 0)
                .containsEntry(NEGATIVE_KEY, NEGATIVE_LIMIT)
                .containsEntry(NUMERIC_KEY, NUMERIC_LIMIT)
                .doesNotContainKey(INVALID_STRING_KEY)
                .doesNotContainKey(INVALID_DURATION_KEY);
    }

    @Test
    void testAttributeValueLengthLimitsParsingAndTruncation() {
        final MetricConfig cfg = buildFullTestConfig();
        final MetricAttributeTransformer transformer = new MetricAttributeTransformer(cfg);

        final Map<String, String> input = new HashMap<>();
        input.put(CUSTOM_SMALL_KEY, TEST_VALUE);
        input.put(CUSTOM_LARGE_KEY, TEST_VALUE);
        input.put(GLOBAL_TRUNCATED_KEY, TEST_VALUE);
        input.put(NON_TRUNCATED_KEY, NON_TRUNCATED_VALUE);
        input.put(NEGATIVE_KEY, TEST_VALUE);
        input.put(ZERO_KEY, TEST_VALUE);

        final Map<String, String> result = transformer.transform("m", input);

        assertThat(result)
                .containsEntry(CUSTOM_SMALL_KEY, CUSTOM_SMALL_EXPECTED)
                .containsEntry(CUSTOM_LARGE_KEY, CUSTOM_LARGE_EXPECTED)
                .containsEntry(GLOBAL_TRUNCATED_KEY, GLOBAL_TRUNCATED_EXPECTED)
                .containsEntry(NON_TRUNCATED_KEY, NON_TRUNCATED_VALUE)
                .containsEntry(NEGATIVE_KEY, TEST_VALUE)
                .doesNotContainKey(ZERO_KEY);

        assertNotSame(input, result);
    }

    @Test
    void testNoTruncationWhenConfigIsEmpty() {
        final MetricConfig cfg = new MetricConfig();
        final MetricAttributeTransformer transformer = new MetricAttributeTransformer(cfg);

        final Map<String, String> input = new HashMap<>();
        input.put("k", "v");

        final Map<String, String> result = transformer.transform("m", input);

        assertSame(input, result);
    }

    @Test
    void testTruncationWithoutGlobalLimit() {
        final MetricConfig cfg = new MetricConfig();
        cfg.setProperty(
                OpenTelemetryReporterOptions.ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY
                        + CUSTOM_SMALL_KEY,
                String.valueOf(CUSTOM_SMALL_LIMIT));

        final MetricAttributeTransformer transformer = new MetricAttributeTransformer(cfg);

        final Map<String, String> input = new HashMap<>();
        input.put(CUSTOM_SMALL_KEY, TEST_VALUE);
        input.put(NON_TRUNCATED_KEY, TEST_VALUE);

        final Map<String, String> result = transformer.transform("m", input);

        assertThat(result)
                .containsEntry(CUSTOM_SMALL_KEY, CUSTOM_SMALL_EXPECTED)
                .containsEntry(NON_TRUNCATED_KEY, TEST_VALUE);
    }

    @Test
    void testEmptyAttributeValueIsPreservedUnderNonZeroLimit() {
        // Regression: an empty-string attribute value must survive any non-zero configured limit.
        // A naive min(limit, length) computation would collapse to limit=0 for empty values and
        // silently drop the attribute, changing exported series identity.
        final MetricAttributeTransformer transformer = buildTransformerWithGlobalLimit(100);
        final MetricConfig cfgWithPerAttr = new MetricConfig();
        cfgWithPerAttr.setProperty(
                OpenTelemetryReporterOptions.ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY + "k", "50");
        final MetricAttributeTransformer perAttrTransformer =
                new MetricAttributeTransformer(cfgWithPerAttr);

        final Map<String, String> input = new HashMap<>();
        input.put("k", "");

        assertThat(transformer.transform("m", input)).containsEntry("k", "");
        assertThat(perAttrTransformer.transform("m", input)).containsEntry("k", "");
    }

    @Test
    void testNullAttributeValueIsPreserved() {
        final MetricAttributeTransformer transformer = buildTransformerWithGlobalLimit(5);

        final Map<String, String> input = new HashMap<>();
        input.put("null_key", null);
        input.put("normal_key", "long_value_to_truncate");

        final Map<String, String> result = transformer.transform("m", input);

        assertThat(result).containsEntry("null_key", null).containsEntry("normal_key", "long_");
        assertNotSame(input, result);
    }

    @Test
    void testNullAttributeValueNoOpWhenOnlyNullsPresent() {
        final MetricAttributeTransformer transformer = buildTransformerWithGlobalLimit(5);

        final Map<String, String> input = new HashMap<>();
        input.put("null_key", null);

        final Map<String, String> result = transformer.transform("m", input);

        assertSame(input, result);
    }

    @Test
    void testEmptyAttributeNameIsRejected() {
        final MetricConfig cfg = new MetricConfig();
        // A config key with no attribute name suffix (the dot is the last character) must be
        // silently rejected; the parsed limits map must not contain an empty-string key.
        cfg.setProperty(
                OpenTelemetryReporterOptions.ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY, "10");

        final MetricAttributeTransformer transformer = new MetricAttributeTransformer(cfg);

        assertThat(transformer.getAttributeValueLimits()).doesNotContainKey("");
    }

    @ParameterizedTest
    @MethodSource("collisionCases")
    void testCollisionSemantics(
            final String caseName, final List<String> rawTaskNames, final long expectedCollisions) {
        final MetricAttributeTransformer transformer = buildTransformerWithGlobalLimit(3);

        for (final String taskName : rawTaskNames) {
            final Map<String, String> raw = new HashMap<>();
            raw.put("task_name", taskName);
            transformer.transform("m", raw);
        }

        assertThat(transformer.getCollisionCount()).as(caseName).isEqualTo(expectedCollisions);
    }

    static Stream<Arguments> collisionCases() {
        return Stream.of(
                Arguments.of("single raw — no collision", Arrays.asList("tsk_AAA"), 0L),
                Arguments.of(
                        "identical raws re-registered — no collision",
                        Arrays.asList("tsk_AAA", "tsk_AAA", "tsk_AAA"),
                        0L),
                Arguments.of(
                        "two distinct raws collapsing — one collision",
                        Arrays.asList("tsk_AAA", "tsk_BBB"),
                        1L),
                Arguments.of(
                        "multiple distinct raws at same slot — sentinel caps at one warning",
                        Arrays.asList("tsk_AAA", "tsk_BBB", "tsk_CCC", "tsk_DDD", "tsk_EEE"),
                        1L));
    }

    @Test
    void testNoOpTransformDoesNotOccupyTrackingSlot() {
        final MetricAttributeTransformer transformer = buildTransformerWithGlobalLimit(100);

        final Map<String, String> raw = new HashMap<>();
        // Value shorter than the limit — no truncation happens.
        raw.put("task_name", "short");
        transformer.transform("m", raw);

        assertThat(transformer.getTrackedSlotCount())
                .as("No-op transformation must not populate the collision tracker")
                .isZero();
    }

    @Test
    void testSlotTrackingIsBounded() {
        // Per-attribute limits: `task_name` truncates; `task_attempt_id` is kept in full (negative
        // limit), so its unique per-attempt value survives into the transformed map. Every
        // registration therefore lands on a distinct slot — exactly the failover-loop pattern.
        final int maxSlots = 32;
        final MetricConfig cfg = new MetricConfig();
        final String prefix = OpenTelemetryReporterOptions.ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY;
        cfg.setProperty(prefix + "task_name", "3");
        cfg.setProperty(prefix + "task_attempt_id", "-1");
        cfg.setProperty(
                OpenTelemetryReporterOptions.COLLISION_TRACKING_MAX_SLOTS_KEY,
                String.valueOf(maxSlots));
        final MetricAttributeTransformer transformer = new MetricAttributeTransformer(cfg);

        for (int i = 0; i < maxSlots * 4; i++) {
            final Map<String, String> raw = new HashMap<>();
            raw.put("task_attempt_id", "attempt-" + i);
            raw.put("task_name", "long_task_name_that_truncates");
            transformer.transform("m", raw);
        }

        assertThat(transformer.getTrackedSlotCount())
                .as("Slot tracker must saturate at the configured maximum under distinct-slot load")
                .isEqualTo(maxSlots);
    }

    @Test
    void testHotSlotSurvivesLruEviction() {
        // Confirms the tracker is access-ordered LRU, not insertion-ordered or arbitrary:
        // a slot that keeps being re-observed must outlast cold slots under cap pressure.
        final int maxSlots = 16;
        final MetricConfig cfg = new MetricConfig();
        final String prefix = OpenTelemetryReporterOptions.ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY;
        cfg.setProperty(prefix + "task_name", "3"); // truncate task_name to 3 chars
        cfg.setProperty(prefix + "task_attempt_id", "-1"); // keep per-attempt UUID in full
        cfg.setProperty(
                OpenTelemetryReporterOptions.COLLISION_TRACKING_MAX_SLOTS_KEY,
                String.valueOf(maxSlots));
        final MetricAttributeTransformer transformer = new MetricAttributeTransformer(cfg);

        // Prime the hot slot — "hot_AAA" truncates to "hot".
        final Map<String, String> hotRawA = new HashMap<>();
        hotRawA.put("task_name", "hot_AAA");
        transformer.transform("m", hotRawA);

        // Fill the tracker past capacity with cold, distinct slots. Between each cold
        // registration, re-touch the hot slot so it stays MRU.
        for (int i = 0; i < maxSlots * 4; i++) {
            final Map<String, String> coldRaw = new HashMap<>();
            coldRaw.put("task_attempt_id", "attempt-" + i);
            coldRaw.put("task_name", "long_task_name_that_truncates");
            transformer.transform("m", coldRaw);
            transformer.transform("m", hotRawA); // refresh LRU access order on the hot slot
        }

        // Register a second raw that collapses onto the same transformed slot as the hot one.
        // If the hot slot is still tracked, this fires a collision WARN. If it was evicted
        // (i.e., the map is not access-ordered LRU), no collision is recorded — the re-entry
        // would just insert a new SlotState.
        final Map<String, String> hotRawB = new HashMap<>();
        hotRawB.put("task_name", "hot_BBB");
        transformer.transform("m", hotRawB);

        assertThat(transformer.getCollisionCount())
                .as("Hot slot must survive LRU eviction and fire a collision on re-entry")
                .isEqualTo(1);
    }

    @Test
    void testNegativeCollisionTrackingMaxSlotsFallsBackToDefault() {
        final MetricConfig cfg = new MetricConfig();
        cfg.setProperty(
                OpenTelemetryReporterOptions.ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY + "*", "3");
        cfg.setProperty(OpenTelemetryReporterOptions.COLLISION_TRACKING_MAX_SLOTS_KEY, "-1");

        // Must not throw — invalid values log a WARN and fall back to the default, same as the
        // attribute-limit parser's behavior for malformed entries.
        final MetricAttributeTransformer transformer = new MetricAttributeTransformer(cfg);

        // Tracking is enabled with the default cap — two distinct raws collapsing must still WARN.
        final Map<String, String> firstRaw = new HashMap<>();
        firstRaw.put("task_name", "tsk_AAA");
        transformer.transform("m", firstRaw);
        final Map<String, String> secondRaw = new HashMap<>();
        secondRaw.put("task_name", "tsk_BBB");
        transformer.transform("m", secondRaw);

        assertThat(transformer.getCollisionCount())
                .as("Negative maxSlots must fall back to default, leaving tracking enabled")
                .isEqualTo(1);
    }

    @Test
    void testMalformedCollisionTrackingMaxSlotsFallsBackToDefault() {
        final MetricConfig cfg = new MetricConfig();
        cfg.setProperty(OpenTelemetryReporterOptions.COLLISION_TRACKING_MAX_SLOTS_KEY, "not-a-num");

        // Must not throw — malformed integers log a WARN and fall back to the default.
        final MetricAttributeTransformer transformer = new MetricAttributeTransformer(cfg);

        assertThat(transformer.getTrackedSlotCount())
                .as("Malformed maxSlots must fall back to default, leaving tracking enabled")
                .isZero();
    }

    @Test
    void testCollisionTrackingCanBeDisabled() {
        final MetricConfig cfg = new MetricConfig();
        cfg.setProperty(
                OpenTelemetryReporterOptions.ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY + "*", "3");
        cfg.setProperty(OpenTelemetryReporterOptions.COLLISION_TRACKING_MAX_SLOTS_KEY, "0");
        final MetricAttributeTransformer transformer = new MetricAttributeTransformer(cfg);

        // Two distinct raws collapsing to the same transformed slot would normally WARN; with
        // tracking disabled, nothing is recorded.
        for (final String taskName : Arrays.asList("tsk_AAA", "tsk_BBB")) {
            final Map<String, String> raw = new HashMap<>();
            raw.put("task_name", taskName);
            transformer.transform("m", raw);
        }

        assertThat(transformer.getCollisionCount())
                .as("No collisions must be recorded when tracking is disabled")
                .isZero();
        assertThat(transformer.getTrackedSlotCount())
                .as("Tracked slot count must be zero when tracking is disabled")
                .isZero();
    }

    private static MetricAttributeTransformer buildTransformerWithGlobalLimit(final int limit) {
        final MetricConfig cfg = new MetricConfig();
        cfg.setProperty(
                OpenTelemetryReporterOptions.ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY + "*",
                String.valueOf(limit));
        return new MetricAttributeTransformer(cfg);
    }

    @Test
    void testTruncationWithIntegerConfigValue() {
        final MetricConfig cfg = new MetricConfig();
        // Put an Integer object (not a String) to verify that MetricConfig.getInteger handles
        // non-String property values correctly.
        cfg.put(
                OpenTelemetryReporterOptions.ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY + NUMERIC_KEY,
                NUMERIC_LIMIT);

        final MetricAttributeTransformer transformer = new MetricAttributeTransformer(cfg);

        assertThat(transformer.getAttributeValueLimits()).containsEntry(NUMERIC_KEY, NUMERIC_LIMIT);
    }

    // -----------------------------------------------------------------------

    private static MetricConfig buildFullTestConfig() {
        final MetricConfig cfg = new MetricConfig();

        cfg.setProperty(
                OpenTelemetryReporterOptions.ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY
                        + MetricAttributeTransformer.GLOBAL_LIMIT_KEY,
                String.valueOf(GLOBAL_LIMIT));
        cfg.setProperty(
                OpenTelemetryReporterOptions.ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY
                        + CUSTOM_SMALL_KEY,
                String.valueOf(CUSTOM_SMALL_LIMIT));
        cfg.setProperty(
                OpenTelemetryReporterOptions.ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY
                        + CUSTOM_LARGE_KEY,
                String.valueOf(CUSTOM_LARGE_LIMIT));
        cfg.setProperty(
                OpenTelemetryReporterOptions.ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY + ZERO_KEY,
                String.valueOf(0));
        cfg.setProperty(
                OpenTelemetryReporterOptions.ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY
                        + NEGATIVE_KEY,
                String.valueOf(NEGATIVE_LIMIT));

        // Integer-typed value — should be parsed correctly.
        cfg.put(
                OpenTelemetryReporterOptions.ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY + NUMERIC_KEY,
                NUMERIC_LIMIT);

        // Non-numeric string — should be skipped.
        cfg.setProperty(
                OpenTelemetryReporterOptions.ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY
                        + INVALID_STRING_KEY,
                "3.5");

        // Non-string value — should be skipped.
        cfg.put(
                OpenTelemetryReporterOptions.ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY
                        + INVALID_DURATION_KEY,
                Duration.ofSeconds(1));

        return cfg;
    }
}
