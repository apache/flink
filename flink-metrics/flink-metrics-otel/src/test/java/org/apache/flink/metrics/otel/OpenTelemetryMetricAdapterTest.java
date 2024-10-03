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

package org.apache.flink.metrics.otel;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.metrics.util.TestHistogram;

import org.apache.flink.shaded.guava32.com.google.common.collect.ImmutableMap;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.metrics.data.SummaryPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableHistogramData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSumData;
import io.opentelemetry.sdk.resources.Resource;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link OpenTelemetryMetricAdapter}. */
public class OpenTelemetryMetricAdapterTest {

    private static final OpenTelemetryMetricAdapter.CollectionMetadata METADATA =
            new OpenTelemetryMetricAdapter.CollectionMetadata(
                    Resource.create(Attributes.empty()), 123L, 345L);

    private static final Map<String, String> VARIABLES = ImmutableMap.of("k1", "v1", "k2", "v2");

    @Test
    public void testCounter() {
        Optional<MetricData> metricData =
                OpenTelemetryMetricAdapter.convertCounter(
                        METADATA, 50L, 3L, new MetricMetadata("foo.bar.count", VARIABLES));

        assertThat(metricData.isPresent()).isTrue();
        assertThat(metricData.get().getName()).isEqualTo("foo.bar.count");
        assertThat(metricData.get().getLongSumData().getAggregationTemporality())
                .isEqualTo(AggregationTemporality.DELTA);
        assertThat(metricData.get().getLongSumData().isMonotonic()).isEqualTo(true);
        assertThat(metricData.get().getType()).isEqualTo(MetricDataType.LONG_SUM);
        assertThat(metricData.get().getLongSumData().getPoints().size()).isEqualTo(1);
        LongPointData data = metricData.get().getLongSumData().getPoints().iterator().next();
        assertThat(data.getValue()).isEqualTo(47L);
        assertThat(asStringMap(data.getAttributes())).isEqualTo(VARIABLES);
        assertThat(metricData.get().getDoubleSumData()).isEqualTo(ImmutableSumData.empty());
        assertThat(metricData.get().getLongGaugeData()).isEqualTo(ImmutableGaugeData.empty());
        assertThat(metricData.get().getDoubleGaugeData()).isEqualTo(ImmutableGaugeData.empty());
        assertThat(metricData.get().getHistogramData()).isEqualTo(ImmutableHistogramData.empty());
    }

    @Test
    public void testGaugeDouble() {
        Optional<MetricData> metricData =
                OpenTelemetryMetricAdapter.convertGauge(
                        METADATA, () -> 123.456d, new MetricMetadata("foo.bar.value", VARIABLES));

        assertThat(metricData.isPresent()).isTrue();
        assertThat(metricData.get().getName()).isEqualTo("foo.bar.value");
        assertThat(metricData.get().getType()).isEqualTo(MetricDataType.DOUBLE_GAUGE);
        assertThat(metricData.get().getDoubleGaugeData().getPoints().size()).isEqualTo(1);
        DoublePointData data = metricData.get().getDoubleGaugeData().getPoints().iterator().next();
        assertThat(data.getValue()).isEqualTo(123.456d);
        assertThat(asStringMap(data.getAttributes())).isEqualTo(VARIABLES);
        assertThat(metricData.get().getLongSumData()).isEqualTo(ImmutableSumData.empty());
        assertThat(metricData.get().getDoubleSumData()).isEqualTo(ImmutableSumData.empty());
        assertThat(metricData.get().getLongGaugeData()).isEqualTo(ImmutableGaugeData.empty());
        assertThat(metricData.get().getHistogramData()).isEqualTo(ImmutableHistogramData.empty());
    }

    @Test
    public void testGaugeLong() {
        Optional<MetricData> metricData =
                OpenTelemetryMetricAdapter.convertGauge(
                        METADATA, () -> 125L, new MetricMetadata("foo.bar.value", VARIABLES));

        assertThat(metricData.isPresent()).isTrue();
        assertThat(metricData.get().getName()).isEqualTo("foo.bar.value");
        assertThat(metricData.get().getType()).isEqualTo(MetricDataType.LONG_GAUGE);
        assertThat(metricData.get().getLongGaugeData().getPoints().size()).isEqualTo(1);
        LongPointData data = metricData.get().getLongGaugeData().getPoints().iterator().next();
        assertThat(data.getValue()).isEqualTo(125L);
        assertThat(asStringMap(data.getAttributes())).isEqualTo(VARIABLES);
        assertThat(metricData.get().getLongSumData()).isEqualTo(ImmutableSumData.empty());
        assertThat(metricData.get().getDoubleSumData()).isEqualTo(ImmutableSumData.empty());
        assertThat(metricData.get().getDoubleGaugeData()).isEqualTo(ImmutableGaugeData.empty());
        assertThat(metricData.get().getHistogramData()).isEqualTo(ImmutableHistogramData.empty());
    }

    @Test
    public void testMeter() {
        Counter counter = new SimpleCounter();
        MeterView meter = new MeterView(counter);
        counter.inc(345L);
        meter.update();
        List<MetricData> metricData =
                OpenTelemetryMetricAdapter.convertMeter(
                        METADATA,
                        meter,
                        counter.getCount(),
                        20L,
                        new MetricMetadata("foo.bar.value", VARIABLES));

        assertThat(metricData.size()).isEqualTo(2);
        assertThat(metricData.get(0).getName()).isEqualTo("foo.bar.value.count");
        assertThat(metricData.get(0).getType()).isEqualTo(MetricDataType.LONG_SUM);
        assertThat(metricData.get(0).getLongSumData().getAggregationTemporality())
                .isEqualTo(AggregationTemporality.DELTA);
        assertThat(metricData.get(0).getLongSumData().getPoints().size()).isEqualTo(1);
        LongPointData data = metricData.get(0).getLongSumData().getPoints().iterator().next();
        assertThat(data.getValue()).isEqualTo(325L);
        assertThat(asStringMap(data.getAttributes())).isEqualTo(VARIABLES);
        assertThat(metricData.get(0).getDoubleSumData()).isEqualTo(ImmutableSumData.empty());
        assertThat(metricData.get(0).getLongGaugeData()).isEqualTo(ImmutableGaugeData.empty());
        assertThat(metricData.get(0).getDoubleGaugeData()).isEqualTo(ImmutableGaugeData.empty());
        assertThat(metricData.get(0).getHistogramData()).isEqualTo(ImmutableHistogramData.empty());

        assertThat(metricData.get(1).getName()).isEqualTo("foo.bar.value.rate");
        assertThat(metricData.get(1).getType()).isEqualTo(MetricDataType.DOUBLE_GAUGE);
        assertThat(metricData.get(1).getDoubleGaugeData().getPoints().size()).isEqualTo(1);
        DoublePointData data2 =
                metricData.get(1).getDoubleGaugeData().getPoints().iterator().next();
        // 345L / 60 seconds
        assertThat(data2.getValue()).isEqualTo(5.75d);
        assertThat(asStringMap(data2.getAttributes())).isEqualTo(VARIABLES);
        assertThat(metricData.get(1).getLongSumData()).isEqualTo(ImmutableSumData.empty());
        assertThat(metricData.get(1).getDoubleSumData()).isEqualTo(ImmutableSumData.empty());
        assertThat(metricData.get(1).getLongGaugeData()).isEqualTo(ImmutableGaugeData.empty());
        assertThat(metricData.get(1).getHistogramData()).isEqualTo(ImmutableHistogramData.empty());
    }

    @Test
    public void testHistogram() {
        TestHistogram histogram = new TestHistogram();
        Optional<MetricData> metricData =
                OpenTelemetryMetricAdapter.convertHistogram(
                        METADATA, histogram, new MetricMetadata("foo.bar.histogram", VARIABLES));

        assertThat(metricData.isPresent()).isTrue();
        assertThat(metricData.get().getName()).isEqualTo("foo.bar.histogram");
        assertThat(metricData.get().getType()).isEqualTo(MetricDataType.SUMMARY);
        assertThat(metricData.get().getSummaryData().getPoints().size()).isEqualTo(1);

        SummaryPointData summaryPointData =
                metricData.get().getSummaryData().getPoints().iterator().next();

        assertThat(summaryPointData.getCount()).isEqualTo(1);
        assertThat(summaryPointData.getSum()).isEqualTo(4d);

        assertThat(summaryPointData.getValues().get(0).getQuantile()).isEqualTo(0);
        assertThat(summaryPointData.getValues().get(0).getValue()).isEqualTo(7);

        assertThat(summaryPointData.getValues().get(1).getQuantile()).isEqualTo(0.5);
        assertThat(summaryPointData.getValues().get(1).getValue()).isEqualTo(0.5);

        assertThat(summaryPointData.getValues().get(2).getQuantile()).isEqualTo(0.75);
        assertThat(summaryPointData.getValues().get(2).getValue()).isEqualTo(0.75);

        assertThat(summaryPointData.getValues().get(3).getQuantile()).isEqualTo(0.95);
        assertThat(summaryPointData.getValues().get(3).getValue()).isEqualTo(0.95);

        assertThat(summaryPointData.getValues().get(4).getQuantile()).isEqualTo(0.99);
        assertThat(summaryPointData.getValues().get(4).getValue()).isEqualTo(0.99);

        assertThat(summaryPointData.getValues().get(5).getQuantile()).isEqualTo(1);
        assertThat(summaryPointData.getValues().get(5).getValue()).isEqualTo(6);

        assertThat(asStringMap(summaryPointData.getAttributes())).isEqualTo(VARIABLES);
    }

    private Map<String, String> asStringMap(Attributes attributes) {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<AttributeKey<?>, Object> entry : attributes.asMap().entrySet()) {
            map.put(entry.getKey().getKey(), (String) entry.getValue());
        }
        return map;
    }
}
