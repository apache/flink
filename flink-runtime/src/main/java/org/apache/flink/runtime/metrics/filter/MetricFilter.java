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

package org.apache.flink.runtime.metrics.filter;

import org.apache.flink.metrics.Metric;

/** A filter for metrics. */
public interface MetricFilter {

    /** Filter that accepts every metric. */
    MetricFilter NO_OP_FILTER = (metric, name, scope) -> true;

    /**
     * Filters a given metric.
     *
     * @param metric the metric to filter
     * @param name the name of the metric
     * @param logicalScope the logical scope of the metric
     * @return true, if the metric matches, false otherwise
     */
    boolean filter(Metric metric, String name, String logicalScope);
}
