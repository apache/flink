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

package org.apache.flink.metrics.datadog;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

import java.util.List;

/**
 * Maps histograms to datadog gauges.
 *
 * <p>Note: We cannot map them to datadog histograms because the HTTP API does not support them.
 */
public class DHistogram {
    @VisibleForTesting static final String SUFFIX_AVG = ".avg";
    @VisibleForTesting static final String SUFFIX_COUNT = ".count";
    @VisibleForTesting static final String SUFFIX_MEDIAN = ".median";
    @VisibleForTesting static final String SUFFIX_95_PERCENTILE = ".95percentile";
    @VisibleForTesting static final String SUFFIX_MIN = ".min";
    @VisibleForTesting static final String SUFFIX_MAX = ".max";

    private final Histogram histogram;

    private final MetricMetaData metaDataAvg;
    private final MetricMetaData metaDataCount;
    private final MetricMetaData metaDataMedian;
    private final MetricMetaData metaData95Percentile;
    private final MetricMetaData metaDataMin;
    private final MetricMetaData metaDataMax;

    public DHistogram(
            Histogram histogram, String metricName, String host, List<String> tags, Clock clock) {
        this.histogram = histogram;
        this.metaDataAvg =
                new MetricMetaData(MetricType.gauge, metricName + SUFFIX_AVG, host, tags, clock);
        this.metaDataCount =
                new MetricMetaData(MetricType.gauge, metricName + SUFFIX_COUNT, host, tags, clock);
        this.metaDataMedian =
                new MetricMetaData(MetricType.gauge, metricName + SUFFIX_MEDIAN, host, tags, clock);
        this.metaData95Percentile =
                new MetricMetaData(
                        MetricType.gauge, metricName + SUFFIX_95_PERCENTILE, host, tags, clock);
        this.metaDataMin =
                new MetricMetaData(MetricType.gauge, metricName + SUFFIX_MIN, host, tags, clock);
        this.metaDataMax =
                new MetricMetaData(MetricType.gauge, metricName + SUFFIX_MAX, host, tags, clock);
    }

    public void addTo(DSeries series) {
        final HistogramStatistics statistics = histogram.getStatistics();

        // this selection is based on
        // https://docs.datadoghq.com/developers/metrics/types/?tab=histogram
        // we only exclude 'sum' (which is optional), because we cannot compute it
        // the semantics for count are also slightly different, because we don't reset it after a
        // report
        series.add(new StaticDMetric(statistics.getMean(), metaDataAvg));
        series.add(new StaticDMetric(histogram.getCount(), metaDataCount));
        series.add(new StaticDMetric(statistics.getQuantile(.5), metaDataMedian));
        series.add(new StaticDMetric(statistics.getQuantile(.95), metaData95Percentile));
        series.add(new StaticDMetric(statistics.getMin(), metaDataMin));
        series.add(new StaticDMetric(statistics.getMax(), metaDataMax));
    }
}
