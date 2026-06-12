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

package org.apache.flink.core.plugin;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.MetricGroup;

/**
 * Opt-in contract for {@link Plugin}s that need a runtime-owned {@link MetricGroup}.
 *
 * <p>The runtime calls {@link #setMetricGroup(MetricGroup)} after {@link
 * Plugin#configure(org.apache.flink.configuration.Configuration)} and before the plugin emits
 * metrics.
 *
 * <p>{@code setMetricGroup} may be called more than once, for example when an embedded runtime is
 * restarted in the same JVM while retaining plugin instances. Re-applying the same {@link
 * MetricGroup} must be a no-op, and a different group must scope metrics created afterwards. The
 * runtime owns the group; implementations may call {@link MetricGroup#addGroup} to derive nested
 * scopes from it.
 */
@PublicEvolving
public interface MetricsAware {

    /**
     * Hands the plugin a runtime-owned {@link MetricGroup} to register its metrics against. See the
     * class-level two-phase init contract.
     *
     * @param metricGroup the group to register metrics under; never {@code null}.
     */
    void setMetricGroup(MetricGroup metricGroup);
}
