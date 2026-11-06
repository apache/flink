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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.Collection;

import static java.util.Objects.requireNonNull;

/**
 * Response type for a collection of aggregated metrics.
 *
 * <p>As JSON this type will be represented as an array of metrics, i.e., the field <code>metrics
 * </code> will not show up. For example, a collection with a single metric will be represented as
 * follows:
 *
 * <pre>{@code
 * [{"id": "metricName", "min": "1"}]
 * }</pre>
 *
 * @see org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore
 */
public class AggregatedMetricsResponseBody extends ArrayList<AggregatedMetric>
        implements ResponseBody {

    private static final long serialVersionUID = -1170348478297775429L;

    // a default constructor is required for collection type marshalling
    public AggregatedMetricsResponseBody() {}

    public AggregatedMetricsResponseBody(Collection<AggregatedMetric> metrics) {
        super(requireNonNull(metrics, "metrics must not be null"));
    }

    @JsonIgnore
    public Collection<AggregatedMetric> getMetrics() {
        return this;
    }

    @Override
    @JsonIgnore
    public boolean isEmpty() {
        return super.isEmpty();
    }
}
