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

package org.apache.flink.runtime.metrics.dump;

import org.junit.jupiter.api.Test;

import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_COUNTER;
import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_GAUGE;
import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_HISTOGRAM;
import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_METER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** Tests for the {@link MetricDump} classes. */
class MetricDumpTest {
    @Test
    void testDumpedCounter() {
        QueryScopeInfo info = new QueryScopeInfo.JobManagerQueryScopeInfo();

        MetricDump.CounterDump cd = new MetricDump.CounterDump(info, "counter", 4);

        assertThat(cd.name).isEqualTo("counter");
        assertThat(cd.count).isEqualTo(4);
        assertThat(cd.scopeInfo).isEqualTo(info);
        assertThat(cd.getCategory()).isEqualTo(METRIC_CATEGORY_COUNTER);
    }

    @Test
    void testDumpedGauge() {
        QueryScopeInfo info = new QueryScopeInfo.JobManagerQueryScopeInfo();

        MetricDump.GaugeDump gd = new MetricDump.GaugeDump(info, "gauge", "hello");

        assertThat(gd.name).isEqualTo("gauge");
        assertThat(gd.value).isEqualTo("hello");
        assertThat(gd.scopeInfo).isEqualTo(info);
        assertThat(gd.getCategory()).isEqualTo(METRIC_CATEGORY_GAUGE);
    }

    @Test
    void testDumpedHistogram() {
        QueryScopeInfo info = new QueryScopeInfo.JobManagerQueryScopeInfo();

        MetricDump.HistogramDump hd =
                new MetricDump.HistogramDump(info, "hist", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11);

        assertThat(hd.name).isEqualTo("hist");
        assertThat(hd.min).isOne();
        assertThat(hd.max).isEqualTo(2);
        assertThat(hd.mean).isCloseTo(3, within(0.1));
        assertThat(hd.median).isCloseTo(4, within(0.1));
        assertThat(hd.stddev).isCloseTo(5, within(0.1));
        assertThat(hd.p75).isCloseTo(6, within(0.1));
        assertThat(hd.p90).isCloseTo(7, within(0.1));
        assertThat(hd.p95).isCloseTo(8, within(0.1));
        assertThat(hd.p98).isCloseTo(9, within(0.1));
        assertThat(hd.p99).isCloseTo(10, within(0.1));
        assertThat(hd.p999).isCloseTo(11, within(0.1));
        assertThat(hd.scopeInfo).isEqualTo(info);
        assertThat(hd.getCategory()).isEqualTo(METRIC_CATEGORY_HISTOGRAM);
    }

    @Test
    void testDumpedMeter() {
        QueryScopeInfo info = new QueryScopeInfo.JobManagerQueryScopeInfo();

        MetricDump.MeterDump md = new MetricDump.MeterDump(info, "meter", 5.0);

        assertThat(md.name).isEqualTo("meter");
        assertThat(md.rate).isCloseTo(5.0, within(0.1));
        assertThat(md.scopeInfo).isEqualTo(info);
        assertThat(md.getCategory()).isEqualTo(METRIC_CATEGORY_METER);
    }
}
