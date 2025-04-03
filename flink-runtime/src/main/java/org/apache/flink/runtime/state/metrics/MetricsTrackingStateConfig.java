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

package org.apache.flink.runtime.state.metrics;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/** Config to tracking state metric. */
@Internal
public abstract class MetricsTrackingStateConfig {

    private final MetricGroup metricGroup;

    private final boolean enabled;
    private final int sampleInterval;
    private final int historySize;
    private final boolean stateNameAsVariable;

    MetricsTrackingStateConfig(
            MetricGroup metricGroup,
            boolean enabled,
            int sampleInterval,
            int historySize,
            boolean stateNameAsVariable) {
        if (enabled) {
            Preconditions.checkNotNull(
                    metricGroup, "Metric group cannot be null if metrics tracking is enabled.");
            Preconditions.checkArgument(sampleInterval >= 1);
        }
        this.metricGroup = metricGroup;
        this.enabled = enabled;
        this.sampleInterval = sampleInterval;
        this.historySize = historySize;
        this.stateNameAsVariable = stateNameAsVariable;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public MetricGroup getMetricGroup() {
        return metricGroup;
    }

    public int getHistorySize() {
        return historySize;
    }

    public int getSampleInterval() {
        return sampleInterval;
    }

    public boolean isStateNameAsVariable() {
        return stateNameAsVariable;
    }

    public abstract static class Builder<
                    T extends MetricsTrackingStateConfig, B extends Builder<T, B>>
            implements Serializable {

        private static final long serialVersionUID = 1L;

        protected boolean enabled;
        protected int sampleInterval;
        protected int historySize;
        protected boolean stateNameAsVariable;
        protected MetricGroup metricGroup;

        public B setEnabled(boolean enabled) {
            this.enabled = enabled;
            return (B) this;
        }

        public B setSampleInterval(int sampleInterval) {
            this.sampleInterval = sampleInterval;
            return (B) this;
        }

        public B setHistorySize(int historySize) {
            this.historySize = historySize;
            return (B) this;
        }

        public B setStateNameAsVariable(boolean stateNameAsVariable) {
            this.stateNameAsVariable = stateNameAsVariable;
            return (B) this;
        }

        public B setMetricGroup(MetricGroup metricGroup) {
            this.metricGroup = metricGroup;
            return (B) this;
        }

        public abstract T build();
    }
}
