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

package org.apache.flink.metrics;

import javax.annotation.Nullable;

/**
 * A {@link EMAGaugeSampler} provides an exponential moving average view over a {@link Gauge}.
 *
 * <p>As other {@link View}s, advantage of this class is that it is updated in regular intervals by
 * a background thread.
 */
public class EMAGaugeSampler<T extends Number> implements Gauge<Double>, View {

    /** The underlying sampled metric. */
    private final Gauge<T> source;
    /** Alpha parameter of the exponential moving average. */
    private final double alpha;
    /** The index in the array for the current time. */
    @Nullable private double currentAverage;

    public EMAGaugeSampler(Gauge<T> source, double alpha) {
        this.source = source;
        this.alpha = alpha;
    }

    @Override
    public void update() {
        currentAverage = currentAverage * alpha + source.getValue().doubleValue() * (1.0 - alpha);
    }

    @Override
    public Double getValue() {
        return currentAverage;
    }
}
