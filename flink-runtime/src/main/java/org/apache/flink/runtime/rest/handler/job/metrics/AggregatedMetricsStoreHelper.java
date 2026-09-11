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

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsAggregationParameter;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/** Helper for aggregating metric values from metric stores. */
final class AggregatedMetricsStoreHelper {

    private static final Logger LOG = LoggerFactory.getLogger(AggregatedMetricsStoreHelper.class);

    private AggregatedMetricsStoreHelper() {}

    /**
     * Returns a JSON string containing a list of all available metrics in the given stores.
     * Effectively this method maps the union of all key-sets to JSON.
     *
     * @param stores metrics
     * @return JSON string containing a list of all available metrics
     */
    static Collection<String> getAvailableMetrics(
            Collection<? extends MetricStore.ComponentMetricStore> stores) {
        Set<String> uniqueMetrics = CollectionUtil.newHashSetWithExpectedSize(32);
        for (MetricStore.ComponentMetricStore store : stores) {
            uniqueMetrics.addAll(store.metrics.keySet());
        }
        return uniqueMetrics;
    }

    /**
     * Extracts and aggregates all requested metrics from the given metric stores, and maps the
     * result to a JSON string.
     *
     * @param stores available metrics
     * @param requestedMetrics ids of requested metrics
     * @param requestedAggregations requested aggregation modes
     * @return JSON string containing the requested metrics
     */
    static AggregatedMetricsResponseBody getAggregatedMetricValues(
            Collection<? extends MetricStore.ComponentMetricStore> stores,
            List<String> requestedMetrics,
            List<MetricsAggregationParameter.AggregationMode> requestedAggregations) {
        final MetricAccumulatorFactory requestedAggregationsFactories =
                createMetricAccumulatorFactory(requestedAggregations);

        Collection<AggregatedMetric> aggregatedMetrics = new ArrayList<>(requestedMetrics.size());
        for (String requestedMetric : requestedMetrics) {
            final Collection<Double> values = new ArrayList<>(stores.size());
            try {
                for (MetricStore.ComponentMetricStore store : stores) {
                    String stringValue = store.metrics.get(requestedMetric);
                    if (stringValue != null) {
                        values.add(Double.valueOf(stringValue));
                    }
                }
            } catch (NumberFormatException nfe) {
                LOG.warn(
                        "The metric {} is not numeric and can't be aggregated.",
                        requestedMetric,
                        nfe);
                // metric is not numeric so we can't perform aggregations => ignore it
                continue;
            }
            if (!values.isEmpty()) {

                Iterator<Double> valuesIterator = values.iterator();
                MetricAccumulator acc =
                        requestedAggregationsFactories.get(requestedMetric, valuesIterator.next());
                valuesIterator.forEachRemaining(acc::add);

                aggregatedMetrics.add(acc.get());
            } else {
                return new AggregatedMetricsResponseBody(Collections.emptyList());
            }
        }
        return new AggregatedMetricsResponseBody(aggregatedMetrics);
    }

    private static MetricAccumulatorFactory createMetricAccumulatorFactory(
            List<MetricsAggregationParameter.AggregationMode> requestedAggregations) {
        DoubleAccumulator.DoubleMinimumFactory minimumFactory = null;
        DoubleAccumulator.DoubleMaximumFactory maximumFactory = null;
        DoubleAccumulator.DoubleAverageFactory averageFactory = null;
        DoubleAccumulator.DoubleSumFactory sumFactory = null;
        DoubleAccumulator.DoubleDataSkewFactory skewFactory = null;
        // by default we return all aggregations
        if (requestedAggregations.isEmpty()) {
            minimumFactory = DoubleAccumulator.DoubleMinimumFactory.get();
            maximumFactory = DoubleAccumulator.DoubleMaximumFactory.get();
            averageFactory = DoubleAccumulator.DoubleAverageFactory.get();
            sumFactory = DoubleAccumulator.DoubleSumFactory.get();
            skewFactory = DoubleAccumulator.DoubleDataSkewFactory.get();
        } else {
            for (MetricsAggregationParameter.AggregationMode aggregation : requestedAggregations) {
                switch (aggregation) {
                    case MIN:
                        minimumFactory = DoubleAccumulator.DoubleMinimumFactory.get();
                        break;
                    case MAX:
                        maximumFactory = DoubleAccumulator.DoubleMaximumFactory.get();
                        break;
                    case AVG:
                        averageFactory = DoubleAccumulator.DoubleAverageFactory.get();
                        break;
                    case SUM:
                        sumFactory = DoubleAccumulator.DoubleSumFactory.get();
                        break;
                    case SKEW:
                        skewFactory = DoubleAccumulator.DoubleDataSkewFactory.get();
                        break;
                    default:
                        LOG.warn("Unsupported aggregation specified: {}", aggregation);
                }
            }
        }
        return new MetricAccumulatorFactory(
                minimumFactory, maximumFactory, averageFactory, sumFactory, skewFactory);
    }

    private static class MetricAccumulatorFactory {

        @Nullable private final DoubleAccumulator.DoubleMinimumFactory minimumFactory;

        @Nullable private final DoubleAccumulator.DoubleMaximumFactory maximumFactory;

        @Nullable private final DoubleAccumulator.DoubleAverageFactory averageFactory;

        @Nullable private final DoubleAccumulator.DoubleSumFactory sumFactory;
        @Nullable private final DoubleAccumulator.DoubleDataSkewFactory dataSkewFactory;

        private MetricAccumulatorFactory(
                @Nullable DoubleAccumulator.DoubleMinimumFactory minimumFactory,
                @Nullable DoubleAccumulator.DoubleMaximumFactory maximumFactory,
                @Nullable DoubleAccumulator.DoubleAverageFactory averageFactory,
                @Nullable DoubleAccumulator.DoubleSumFactory sumFactory,
                @Nullable DoubleAccumulator.DoubleDataSkewFactory dataSkewFactory) {
            this.minimumFactory = minimumFactory;
            this.maximumFactory = maximumFactory;
            this.averageFactory = averageFactory;
            this.sumFactory = sumFactory;
            this.dataSkewFactory = dataSkewFactory;
        }

        MetricAccumulator get(String metricName, double init) {
            return new MetricAccumulator(
                    metricName,
                    minimumFactory == null ? null : minimumFactory.get(init),
                    maximumFactory == null ? null : maximumFactory.get(init),
                    averageFactory == null ? null : averageFactory.get(init),
                    sumFactory == null ? null : sumFactory.get(init),
                    dataSkewFactory == null ? null : dataSkewFactory.get(init));
        }
    }

    private static class MetricAccumulator {
        private final String metricName;

        @Nullable private final DoubleAccumulator min;
        @Nullable private final DoubleAccumulator max;
        @Nullable private final DoubleAccumulator avg;
        @Nullable private final DoubleAccumulator sum;
        @Nullable private final DoubleAccumulator skew;

        private MetricAccumulator(
                String metricName,
                @Nullable DoubleAccumulator min,
                @Nullable DoubleAccumulator max,
                @Nullable DoubleAccumulator avg,
                @Nullable DoubleAccumulator sum,
                @Nullable DoubleAccumulator.DoubleDataSkew skew) {
            this.metricName = Preconditions.checkNotNull(metricName);
            this.min = min;
            this.max = max;
            this.avg = avg;
            this.sum = sum;
            this.skew = skew;
        }

        void add(double value) {
            if (min != null) {
                min.add(value);
            }
            if (max != null) {
                max.add(value);
            }
            if (avg != null) {
                avg.add(value);
            }
            if (sum != null) {
                sum.add(value);
            }
            if (skew != null) {
                skew.add(value);
            }
        }

        AggregatedMetric get() {
            return new AggregatedMetric(
                    metricName,
                    min == null ? null : min.getValue(),
                    max == null ? null : max.getValue(),
                    avg == null ? null : avg.getValue(),
                    sum == null ? null : sum.getValue(),
                    skew == null ? null : skew.getValue());
        }
    }
}
