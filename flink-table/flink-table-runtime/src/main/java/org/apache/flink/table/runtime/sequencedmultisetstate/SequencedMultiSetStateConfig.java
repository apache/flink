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

package org.apache.flink.table.runtime.sequencedmultisetstate;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.table.runtime.sequencedmultisetstate.SequencedMultiSetState.Strategy;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Configuration for {@link SequencedMultiSetState}. */
public class SequencedMultiSetStateConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Strategy strategy;
    private final @Nullable Long adaptiveHighThresholdOverride;
    private final @Nullable Long adaptiveLowThresholdOverride;
    private final StateTtlConfig ttlConfig;
    private final TimeSelector ttlTimeSelector;

    public SequencedMultiSetStateConfig(
            Strategy strategy,
            @Nullable Long adaptiveHighThresholdOverride,
            @Nullable Long adaptiveLowThresholdOverride,
            StateTtlConfig ttlConfig,
            TimeDomain ttlTimeDomain) {
        this(
                strategy,
                adaptiveHighThresholdOverride,
                adaptiveLowThresholdOverride,
                ttlConfig,
                TimeSelector.getTimeDomain(ttlTimeDomain));
    }

    public SequencedMultiSetStateConfig(
            Strategy strategy,
            @Nullable Long adaptiveHighThresholdOverride,
            @Nullable Long adaptiveLowThresholdOverride,
            StateTtlConfig ttlConfig,
            TimeSelector ttlTimeSelector) {
        checkArgument(
                !ttlConfig.isEnabled(),
                "TTL is not supported"); // https://issues.apache.org/jira/browse/FLINK-38463
        this.strategy = strategy;
        this.adaptiveHighThresholdOverride = adaptiveHighThresholdOverride;
        this.adaptiveLowThresholdOverride = adaptiveLowThresholdOverride;
        this.ttlConfig = ttlConfig;
        this.ttlTimeSelector = ttlTimeSelector;
    }

    public static SequencedMultiSetStateConfig defaults(
            TimeDomain ttlTimeDomain, StateTtlConfig ttlConfig) {
        return forValue(ttlTimeDomain, ttlConfig);
    }

    public static SequencedMultiSetStateConfig forMap(
            TimeDomain ttlTimeDomain, StateTtlConfig ttlConfig) {
        return new SequencedMultiSetStateConfig(
                Strategy.MAP_STATE, null, null, ttlConfig, ttlTimeDomain);
    }

    public static SequencedMultiSetStateConfig forValue(
            TimeDomain ttlTimeDomain, StateTtlConfig ttl) {
        return new SequencedMultiSetStateConfig(
                Strategy.VALUE_STATE, null, null, ttl, ttlTimeDomain);
    }

    public static SequencedMultiSetStateConfig adaptive(
            TimeDomain ttlTimeDomain,
            @Nullable Long adaptiveHighThresholdOverride,
            @Nullable Long adaptiveLowThresholdOverride,
            StateTtlConfig ttl) {
        return new SequencedMultiSetStateConfig(
                Strategy.ADAPTIVE,
                adaptiveHighThresholdOverride,
                adaptiveLowThresholdOverride,
                ttl,
                ttlTimeDomain);
    }

    public TimeSelector getTimeSelector() {
        return ttlTimeSelector;
    }

    public Strategy getStrategy() {
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
