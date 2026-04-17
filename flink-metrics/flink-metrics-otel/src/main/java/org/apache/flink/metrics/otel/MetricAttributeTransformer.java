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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.flink.metrics.otel.OpenTelemetryReporterOptions.ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY;
import static org.apache.flink.metrics.otel.OpenTelemetryReporterOptions.COLLISION_TRACKING_MAX_SLOTS;

/**
 * Applies per-attribute and global length limits to metric attribute values before they are
 * exported via the OpenTelemetry reporter, and best-effort detects cases where truncation caused
 * two distinct raw attribute maps to collapse to the same transformed output for a given metric
 * name (which results in ambiguous exported series at the downstream backend).
 *
 * <p>Configuration is prefix-based under {@value
 * OpenTelemetryReporterOptions#ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY}. The special key {@code *}
 * sets a global limit for all attributes not explicitly listed.
 *
 * <p>Semantics of individual limit values:
 *
 * <ul>
 *   <li>Positive: truncate the attribute value to that many characters.
 *   <li>Zero: drop the attribute entirely.
 *   <li>Negative: keep the full value (overrides a global cap).
 * </ul>
 *
 * <p><b>Collision detection</b> fires at most once per (name, transformed-attributes) slot when two
 * distinct raw attribute maps have been observed for that slot. The tracker stores only 64-bit
 * hashes, in a bounded LRU so that memory stays contained when attribute cardinality is high (e.g.
 * a per-attempt UUID). The cap is configured via {@link
 * OpenTelemetryReporterOptions#COLLISION_TRACKING_MAX_SLOTS}; {@code 0} disables tracking entirely.
 * On overflow the least-recently-touched slot is evicted, and a previously warned slot that later
 * gets evicted may fire its warning again on re-entry.
 *
 * <p>This class is <b>not thread-safe</b>. Callers are expected to serialize calls to {@link
 * #transform}.
 *
 * @see OpenTelemetryReporterOptions#ATTRIBUTE_VALUE_LENGTH_LIMITS
 */
@NotThreadSafe
final class MetricAttributeTransformer {

    private static final Logger LOG = LoggerFactory.getLogger(MetricAttributeTransformer.class);

    static final String GLOBAL_LIMIT_KEY = "*";

    /** Load factor for the tracking map; matches {@link java.util.HashMap}'s default. */
    private static final float TRACKING_MAP_LOAD_FACTOR = 0.75f;

    /** Upper bound on the tracking map's initial capacity. */
    private static final int TRACKING_MAP_INITIAL_CAPACITY = 1024;

    /** Access-ordered iteration so {@code removeEldestEntry} evicts the true LRU entry. */
    private static final boolean LRU_ACCESS_ORDER = true;

    /** Parsed attribute-to-limit mapping, including {@code *} for the global limit. */
    private final Map<String, Integer> attributeValueLimits;

    /**
     * Bounded LRU of slot-hash → tracking state, or {@code null} when tracking is disabled. The
     * slot-hash encodes (metricName, transformedVariables); {@link SlotState} records the first
     * raw-hash observed at that slot and whether the slot has already warned.
     */
    @Nullable private final Map<Long, SlotState> slotStates;

    private long collisionCount = 0;

    /**
     * Per-slot tracking state. Mutating {@code warned} in place avoids a per-warning allocation;
     * the immutable slot-hash key drives LRU ordering regardless.
     */
    private static final class SlotState {
        final long firstRawHash;
        boolean warned;

        SlotState(final long firstRawHash) {
            this.firstRawHash = firstRawHash;
        }
    }

    /**
     * Constructs a {@link MetricAttributeTransformer} by reading length-limit entries from the
     * supplied {@link MetricConfig}.
     */
    MetricAttributeTransformer(final MetricConfig metricConfig) {
        this.attributeValueLimits = parseAttributeLimits(metricConfig);
        if (!attributeValueLimits.isEmpty()) {
            LOG.info(
                    "Metric attribute transformer is configured with value length limits: {}",
                    attributeValueLimits);
        }
        this.slotStates = buildSlotStateMap(metricConfig);
    }

    @Nullable
    private static Map<Long, SlotState> buildSlotStateMap(final MetricConfig metricConfig) {
        final int maxSlots = readCollisionTrackingMaxSlots(metricConfig);
        if (maxSlots == 0) {
            return null;
        }
        return new LinkedHashMap<>(
                TRACKING_MAP_INITIAL_CAPACITY, TRACKING_MAP_LOAD_FACTOR, LRU_ACCESS_ORDER) {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<Long, SlotState> eldest) {
                return size() > maxSlots;
            }
        };
    }

    private static int readCollisionTrackingMaxSlots(final MetricConfig metricConfig) {
        final int defaultValue = COLLISION_TRACKING_MAX_SLOTS.defaultValue();
        final int parsed;
        try {
            parsed = metricConfig.getInteger(COLLISION_TRACKING_MAX_SLOTS.key(), defaultValue);
        } catch (final NumberFormatException e) {
            LOG.warn(
                    "Skipping invalid format for {} with value: {}; falling back to default {}",
                    COLLISION_TRACKING_MAX_SLOTS.key(),
                    metricConfig.get(COLLISION_TRACKING_MAX_SLOTS.key()),
                    defaultValue,
                    e);
            return defaultValue;
        }
        if (parsed < 0) {
            LOG.warn(
                    "Ignoring negative value {} for {}; falling back to default {}",
                    parsed,
                    COLLISION_TRACKING_MAX_SLOTS.key(),
                    defaultValue);
            return defaultValue;
        }
        return parsed;
    }

    /**
     * Applies configured length limits to {@code rawVariables} and best-effort tracks whether the
     * resulting (name, transformed-attributes) slot has been seen before with a different raw
     * input. Emits at most one WARN per slot.
     *
     * <p>If no limits are configured, or if truncation would produce a map equal to the input,
     * {@code rawVariables} is returned unchanged (reference-equal) and no tracking state is
     * touched.
     *
     * @param metricName the fully-qualified metric name
     * @param rawVariables the raw metric attribute map
     * @return the transformed attribute map to store alongside the metric
     */
    Map<String, String> transform(final String metricName, final Map<String, String> rawVariables) {
        if (attributeValueLimits.isEmpty()) {
            return rawVariables;
        }
        final Map<String, String> transformed = applyLimits(rawVariables);
        if (transformed.equals(rawVariables)) {
            // No-op truncation can't produce new collisions. Return the input to avoid retaining
            // the freshly-allocated transformed map.
            return rawVariables;
        }
        if (slotStates == null) {
            return transformed;
        }

        final long slotHash = slotHash(metricName, transformed);
        final long rawHash = attributesHash(rawVariables);
        final SlotState state = slotStates.get(slotHash);
        if (state == null) {
            slotStates.put(slotHash, new SlotState(rawHash));
            return transformed;
        }
        if (state.warned || state.firstRawHash == rawHash) {
            return transformed;
        }
        LOG.warn(
                "Truncation collision at metric '{}': this registration truncated {} and "
                        + "collapsed onto transformed attributes {} already seen for a "
                        + "different raw input. Exported series at this slot will be "
                        + "ambiguous at the downstream backend.",
                metricName,
                truncatedEntries(rawVariables, transformed),
                transformed);
        state.warned = true;
        collisionCount++;
        return transformed;
    }

    /**
     * Returns the subset of {@code raw} entries whose value was actually truncated. Dropped
     * attributes don't contribute to the collision signature for this warning.
     */
    private static Map<String, String> truncatedEntries(
            final Map<String, String> raw, final Map<String, String> transformed) {
        final Map<String, String> diff = new HashMap<>();
        for (Map.Entry<String, String> e : raw.entrySet()) {
            final String newValue = transformed.get(e.getKey());
            if (newValue != null && !newValue.equals(e.getValue())) {
                diff.put(e.getKey(), e.getValue());
            }
        }
        return diff;
    }

    /**
     * @return the cumulative number of collisions observed since construction.
     */
    @VisibleForTesting
    long getCollisionCount() {
        return collisionCount;
    }

    @VisibleForTesting
    int getTrackedSlotCount() {
        return slotStates == null ? 0 : slotStates.size();
    }

    @VisibleForTesting
    Map<String, Integer> getAttributeValueLimits() {
        return attributeValueLimits;
    }

    // -----------------------------------------------------------------------

    private Map<String, String> applyLimits(final Map<String, String> variables) {
        final int globalLimit =
                attributeValueLimits.getOrDefault(GLOBAL_LIMIT_KEY, Integer.MAX_VALUE);
        final Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, String> entry : variables.entrySet()) {
            final String key = entry.getKey();
            final String value = entry.getValue();
            final int configuredLimit = attributeValueLimits.getOrDefault(key, globalLimit);
            if (configuredLimit == 0) {
                // Drop the attribute entirely (user-requested via 0).
                continue;
            }
            if (configuredLimit > 0 && value.length() > configuredLimit) {
                result.put(key, value.substring(0, configuredLimit));
            } else {
                // No truncation needed, or negative limit (disables per-attribute truncation).
                result.put(key, value);
            }
        }
        return result;
    }

    /**
     * Reserved attribute key under which the metric name is folded into {@link #slotHash}, so the
     * metric name participates in the slot-hash exactly like any other attribute.
     */
    private static final String METRIC_NAME_PSEUDO_ATTRIBUTE = "__metric_name__";

    /** Bit-mask to extend an {@code int} hash into the low 32 bits of a {@code long}. */
    private static final long LOW_32_BITS_MASK = 0xFFFFFFFFL;

    /**
     * Order-independent 64-bit hash of (metricName, transformed). Folds the metric name as a
     * reserved pseudo-attribute, so it combines through the same logic as real attributes.
     */
    private static long slotHash(final String metricName, final Map<String, String> transformed) {
        return attributesHash(transformed) ^ entryHash(METRIC_NAME_PSEUDO_ATTRIBUTE, metricName);
    }

    /** Order-independent 64-bit hash of an attribute map. */
    private static long attributesHash(final Map<String, String> variables) {
        long h = 0;
        for (final Map.Entry<String, String> e : variables.entrySet()) {
            h ^= entryHash(e.getKey(), e.getValue());
        }
        return h;
    }

    /** Packs two 32-bit string hashes into one 64-bit value — key in the high half, value low. */
    private static long entryHash(final String key, final String value) {
        return (((long) key.hashCode()) << 32) ^ (value.hashCode() & LOW_32_BITS_MASK);
    }

    private static Map<String, Integer> parseAttributeLimits(final MetricConfig config) {
        final Map<String, Integer> limits = new HashMap<>();
        for (final Object keyObj : config.keySet()) {
            if (!(keyObj instanceof String)) {
                continue;
            }
            final String fullKey = (String) keyObj;
            if (!fullKey.startsWith(ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY)) {
                continue;
            }
            final String attributeName =
                    fullKey.substring(ATTRIBUTE_VALUE_LENGTH_LIMITS_PREFIX_KEY.length());
            if (attributeName.isEmpty()) {
                LOG.warn(
                        "Ignoring attribute value length limit with empty attribute name for key: {}",
                        fullKey);
                continue;
            }
            try {
                final int limit = config.getInteger(fullKey, Integer.MAX_VALUE);
                limits.put(attributeName, limit);
            } catch (final NumberFormatException e) {
                LOG.warn(
                        "Skipping invalid format for attribute length limit with key: {} and value: {}",
                        fullKey,
                        config.get(fullKey),
                        e);
            }
        }
        return limits;
    }
}
