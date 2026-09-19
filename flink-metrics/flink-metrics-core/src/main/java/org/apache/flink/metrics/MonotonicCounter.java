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

import org.apache.flink.annotation.PublicEvolving;

/**
 * A {@link Counter} whose count only ever increases; it is never decremented.
 *
 * <p>Reporters that want to export this as a monotonic/cumulative counter in the target monitoring
 * system (e.g. an OpenTelemetry monotonic {@code Sum} or a native Prometheus counter, enabling
 * automatic reset detection) should check for this marker interface with {@code instanceof} rather
 * than relying on {@link Metric#getMetricType()}, which continues to report {@link
 * MetricType#COUNTER} for a {@code MonotonicCounter} so that reporters unaware of this interface
 * keep treating it as a regular counter.
 *
 * <p>Decrementing is one of {@link Counter}'s optional operations (see {@link Counter#dec()}):
 * {@link #dec()} and {@link #dec(long)} throw {@link UnsupportedOperationException} rather than
 * silently violating monotonicity.
 */
@PublicEvolving
public interface MonotonicCounter extends Counter {

    /**
     * Always throws {@link UnsupportedOperationException}, since a {@code MonotonicCounter}'s count
     * only ever increases.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    default void dec() {
        throw new UnsupportedOperationException("MonotonicCounter does not support decrementing.");
    }

    /**
     * Always throws {@link UnsupportedOperationException}, since a {@code MonotonicCounter}'s count
     * only ever increases.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    default void dec(long n) {
        throw new UnsupportedOperationException("MonotonicCounter does not support decrementing.");
    }
}
