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

package org.apache.flink.table.watermark;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

/** Pojo class for watermark configs from table options or 'OPTIONS' hint. */
@Internal
public class WatermarkParams implements Serializable {
    private static final long serialVersionUID = 1L;

    private WatermarkEmitStrategy emitStrategy;
    private String alignGroupName;
    private Duration alignMaxDrift;
    private Duration alignUpdateInterval;
    private long sourceIdleTimeout;

    public WatermarkParams() {}

    public WatermarkParams(
            WatermarkEmitStrategy emitStrategy,
            String alignGroupName,
            Duration alignMaxDrift,
            Duration alignUpdateInterval,
            long sourceIdleTimeout) {
        this.emitStrategy = emitStrategy;
        this.alignGroupName = alignGroupName;
        this.alignMaxDrift = alignMaxDrift;
        this.alignUpdateInterval = alignUpdateInterval;
        this.sourceIdleTimeout = sourceIdleTimeout;
    }

    public WatermarkEmitStrategy getEmitStrategy() {
        return emitStrategy;
    }

    public void setEmitStrategy(WatermarkEmitStrategy emitStrategy) {
        this.emitStrategy = emitStrategy;
    }

    public String getAlignGroupName() {
        return alignGroupName;
    }

    public void setAlignGroupName(String alignGroupName) {
        this.alignGroupName = alignGroupName;
    }

    public Duration getAlignMaxDrift() {
        return alignMaxDrift;
    }

    public void setAlignMaxDrift(Duration alignMaxDrift) {
        this.alignMaxDrift = alignMaxDrift;
    }

    public Duration getAlignUpdateInterval() {
        return alignUpdateInterval;
    }

    public void setAlignUpdateInterval(Duration alignUpdateInterval) {
        this.alignUpdateInterval = alignUpdateInterval;
    }

    public long getSourceIdleTimeout() {
        return sourceIdleTimeout;
    }

    public void setSourceIdleTimeout(long sourceIdleTimeout) {
        this.sourceIdleTimeout = sourceIdleTimeout;
    }

    public boolean alignWatermarkEnabled() {
        return !StringUtils.isNullOrWhitespaceOnly(alignGroupName)
                && isLegalDuration(alignMaxDrift)
                && isLegalDuration(alignUpdateInterval);
    }

    private boolean isLegalDuration(Duration duration) {
        return duration != null && !duration.isNegative() && !duration.isZero();
    }

    public static WatermarkParamsBuilder builder() {
        return new WatermarkParamsBuilder();
    }

    @Override
    public String toString() {
        return "WatermarkParams{"
                + "emitStrategy="
                + emitStrategy
                + ", alignGroupName='"
                + alignGroupName
                + '\''
                + ", alignMaxDrift="
                + alignMaxDrift
                + ", alignUpdateInterval="
                + alignUpdateInterval
                + ", sourceIdleTimeout="
                + sourceIdleTimeout
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WatermarkParams that = (WatermarkParams) o;
        return sourceIdleTimeout == that.sourceIdleTimeout
                && emitStrategy == that.emitStrategy
                && Objects.equals(alignGroupName, that.alignGroupName)
                && Objects.equals(alignMaxDrift, that.alignMaxDrift)
                && Objects.equals(alignUpdateInterval, that.alignUpdateInterval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                emitStrategy,
                alignGroupName,
                alignMaxDrift,
                alignUpdateInterval,
                sourceIdleTimeout);
    }

    /** Builder of WatermarkHintParams. */
    @Internal
    public static class WatermarkParamsBuilder {
        private WatermarkEmitStrategy emitStrategy =
                FactoryUtil.WATERMARK_EMIT_STRATEGY.defaultValue();
        private String alignGroupName;
        private Duration alignMaxDrift = Duration.ZERO;
        private Duration alignUpdateInterval =
                FactoryUtil.WATERMARK_ALIGNMENT_UPDATE_INTERVAL.defaultValue();
        private long sourceIdleTimeout = -1;

        public WatermarkParamsBuilder emitStrategy(WatermarkEmitStrategy emitStrategy) {
            this.emitStrategy = emitStrategy;
            return this;
        }

        public WatermarkParamsBuilder alignGroupName(String alignGroupName) {
            this.alignGroupName = alignGroupName;
            return this;
        }

        public WatermarkParamsBuilder alignMaxDrift(Duration alignMaxDrift) {
            this.alignMaxDrift = alignMaxDrift;
            return this;
        }

        public WatermarkParamsBuilder alignUpdateInterval(Duration alignUpdateInterval) {
            this.alignUpdateInterval = alignUpdateInterval;
            return this;
        }

        public WatermarkParamsBuilder sourceIdleTimeout(long sourceIdleTimeout) {
            this.sourceIdleTimeout = sourceIdleTimeout;
            return this;
        }

        public WatermarkParams build() {
            return new WatermarkParams(
                    emitStrategy,
                    alignGroupName,
                    alignMaxDrift,
                    alignUpdateInterval,
                    sourceIdleTimeout);
        }
    }
}
