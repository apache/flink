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

import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/** Base {@link MessageParameters} class for aggregating metrics. */
public abstract class AbstractAggregatedMetricsParameters<M extends MessageQueryParameter<?>>
        extends MessageParameters {
    private final MetricsFilterParameter metrics = new MetricsFilterParameter();
    private final MetricsAggregationParameter aggs = new MetricsAggregationParameter();
    private final M selector;

    AbstractAggregatedMetricsParameters(M selector) {
        this.selector = selector;
    }

    @Override
    public Collection<MessageQueryParameter<?>> getQueryParameters() {
        return Collections.unmodifiableCollection(Arrays.asList(metrics, aggs, selector));
    }
}
