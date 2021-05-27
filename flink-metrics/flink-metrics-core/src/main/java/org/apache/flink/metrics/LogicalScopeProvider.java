/*
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

package org.apache.flink.metrics;

/**
 * Extension for metric groups that support logical scopes.
 *
 * <p>ATTENTION: This interface is *not* meant for the long-term; it merely removes the need for
 * reporters to depend on flink-runtime in order to access the logical scope. Once the logical scope
 * is properly exposed this interface *will* be removed.
 */
public interface LogicalScopeProvider {
    /**
     * Returns the logical scope for the metric group, for example {@code "taskmanager.job.task"},
     * with the given filter being applied to all scope components.
     *
     * @param filter filter to apply to all scope components
     * @return logical scope
     */
    String getLogicalScope(CharacterFilter filter);

    /**
     * Returns the logical scope for the metric group, for example {@code "taskmanager.job.task"},
     * with the given filter being applied to all scope components and the given delimiter being
     * used to concatenate scope components.
     *
     * @param filter filter to apply to all scope components
     * @param delimiter delimiter to use for concatenating scope components
     * @return logical scope
     */
    String getLogicalScope(CharacterFilter filter, char delimiter);

    /** Returns the underlying metric group. */
    MetricGroup getWrappedMetricGroup();

    /**
     * Casts the given metric group to a {@link LogicalScopeProvider}, if it implements the
     * interface.
     *
     * @param metricGroup metric group to cast
     * @return cast metric group
     * @throws IllegalStateException if the metric group did not implement the LogicalScopeProvider
     *     interface
     */
    static LogicalScopeProvider castFrom(MetricGroup metricGroup) throws IllegalStateException {
        if (metricGroup instanceof LogicalScopeProvider) {
            return (LogicalScopeProvider) metricGroup;
        } else {
            throw new IllegalStateException(
                    "The given metric group does not implement the LogicalScopeProvider interface.");
        }
    }
}
