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

package org.apache.flink.table.runtime.orderedmultisetstate;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.table.api.config.ExecutionConfigOptions;

import javax.annotation.Nullable;

import java.util.Optional;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_HIGH;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_LOW;
import static org.apache.flink.util.Preconditions.checkArgument;

public class StateSettings {
    private final OrderedMultiSetState.Strategy strategy;
    private final @Nullable Long adaptiveHighThresholdOverride;
    private final @Nullable Long adaptiveLowThresholdOverride;
    private final StateTtlConfig ttlConfig;
    private final TimeDomain ttlTimeDomain; // overrides the one from ttlConfig

    private StateSettings(
            TimeDomain ttlTimeDomain,
            OrderedMultiSetState.Strategy strategy,
            @Nullable Long adaptiveHighThresholdOverride,
            @Nullable Long adaptiveLowThresholdOverride,
            StateTtlConfig ttlConfig) {
        checkArgument(
                !ttlConfig.isEnabled(),
                "TTL is not supported"); // https://issues.apache.org/jira/browse/FLINK-38463
        this.ttlTimeDomain = ttlTimeDomain;
        this.strategy = strategy;
        this.adaptiveHighThresholdOverride = adaptiveHighThresholdOverride;
        this.adaptiveLowThresholdOverride = adaptiveLowThresholdOverride;
        this.ttlConfig = ttlConfig;
    }

    public static StateSettings defaults(TimeDomain ttlTimeDomain, StateTtlConfig ttlConfig) {
        return forValue(ttlTimeDomain, ttlConfig);
    }

    public static StateSettings forMap(TimeDomain ttlTimeDomain, StateTtlConfig ttlConfig) {
        return new StateSettings(
                ttlTimeDomain, OrderedMultiSetState.Strategy.MAP_STATE, null, null, ttlConfig);
    }

    public static StateSettings forValue(TimeDomain ttlTimeDomain, StateTtlConfig ttl) {
        return new StateSettings(
                ttlTimeDomain, OrderedMultiSetState.Strategy.VALUE_STATE, null, null, ttl);
    }

    public static StateSettings adaptive(
            TimeDomain ttlTimeDomain,
            @Nullable Long adaptiveHighThresholdOverride,
            @Nullable Long adaptiveLowThresholdOverride,
            StateTtlConfig ttl) {
        return new StateSettings(
                ttlTimeDomain,
                OrderedMultiSetState.Strategy.ADAPTIVE,
                adaptiveHighThresholdOverride,
                adaptiveLowThresholdOverride,
                ttl);
    }

    public static StateSettings forStrategy(
            ExecutionConfigOptions.SinkUpsertMaterializeStrategy sinkUpsertMaterializeStrategy,
            ReadableConfig config,
            StateTtlConfig ttlConfig) {
        switch (sinkUpsertMaterializeStrategy) {
            case MAP:
                return forMap(TimeDomain.PROCESSING_TIME, ttlConfig);
            case VALUE:
                return forValue(TimeDomain.PROCESSING_TIME, ttlConfig);
            case ADAPTIVE:
                return adaptive(
                        TimeDomain.PROCESSING_TIME,
                        config.getOptional(
                                        TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_HIGH)
                                .orElse(null),
                        config.getOptional(
                                        TABLE_EXEC_SINK_UPSERT_MATERIALIZE_ADAPTIVE_THRESHOLD_LOW)
                                .orElse(null),
                        ttlConfig);
            default:
                throw new IllegalStateException(
                        "Unknown upsertMaterializeStrategy: " + sinkUpsertMaterializeStrategy);
        }
    }

    public TimeDomain getTtlTimeDomain() {
        return ttlTimeDomain;
    }

    public OrderedMultiSetState.Strategy getStrategy() {
        return strategy;
    }

    public Optional<Long> getAdaptiveHighThresholdOverride() {
        return Optional.ofNullable(adaptiveHighThresholdOverride);
    }

    public Optional<Long> getAdaptiveLowThresholdOverride() {
        return Optional.ofNullable(adaptiveLowThresholdOverride);
    }

    public StateTtlConfig getTtlConfig() {
        return ttlConfig;
    }
}
