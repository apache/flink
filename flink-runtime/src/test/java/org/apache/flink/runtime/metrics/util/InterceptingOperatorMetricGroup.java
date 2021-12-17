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

package org.apache.flink.runtime.metrics.util;

import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.metrics.groups.InternalOperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;

import java.util.HashMap;
import java.util.Map;

/** An {@link InternalOperatorMetricGroup} that exposes all registered metrics. */
public class InterceptingOperatorMetricGroup
        extends UnregisteredMetricGroups.UnregisteredOperatorMetricGroup {

    private Map<String, Metric> intercepted;

    /**
     * Returns the registered metric for the given name, or null if it was never registered.
     *
     * @param name metric name
     * @return registered metric for the given name, or null if it was never registered
     */
    public Metric get(String name) {
        return intercepted.get(name);
    }

    @Override
    protected void addMetric(String name, Metric metric) {
        if (intercepted == null) {
            intercepted = new HashMap<>();
        }
        intercepted.put(name, metric);
        super.addMetric(name, metric);
    }
}
