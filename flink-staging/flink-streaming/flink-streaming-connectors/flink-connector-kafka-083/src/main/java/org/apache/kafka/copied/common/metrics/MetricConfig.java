/**
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
package org.apache.kafka.copied.common.metrics;

import java.util.concurrent.TimeUnit;

/**
 * Configuration values for metrics
 */
public class MetricConfig {

    private Quota quota;
    private int samples;
    private long eventWindow;
    private long timeWindowMs;
    private TimeUnit unit;

    public MetricConfig() {
        super();
        this.quota = null;
        this.samples = 2;
        this.eventWindow = Long.MAX_VALUE;
        this.timeWindowMs = TimeUnit.MILLISECONDS.convert(30, TimeUnit.SECONDS);
        this.unit = TimeUnit.SECONDS;
    }

    public Quota quota() {
        return this.quota;
    }

    public MetricConfig quota(Quota quota) {
        this.quota = quota;
        return this;
    }

    public long eventWindow() {
        return eventWindow;
    }

    public MetricConfig eventWindow(long window) {
        this.eventWindow = window;
        return this;
    }

    public long timeWindowMs() {
        return timeWindowMs;
    }

    public MetricConfig timeWindow(long window, TimeUnit unit) {
        this.timeWindowMs = TimeUnit.MILLISECONDS.convert(window, unit);
        return this;
    }

    public int samples() {
        return this.samples;
    }

    public MetricConfig samples(int samples) {
        if (samples < 1)
            throw new IllegalArgumentException("The number of samples must be at least 1.");
        this.samples = samples;
        return this;
    }

    public TimeUnit timeUnit() {
        return unit;
    }

    public MetricConfig timeUnit(TimeUnit unit) {
        this.unit = unit;
        return this;
    }
}
