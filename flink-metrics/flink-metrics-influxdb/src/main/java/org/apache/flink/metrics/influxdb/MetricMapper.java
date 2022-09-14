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

package org.apache.flink.metrics.influxdb;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;

import org.influxdb.dto.Point;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

class MetricMapper {

    static Point map(MeasurementInfo info, Instant timestamp, Gauge<?> gauge) {
        Point.Builder builder = builder(info, timestamp);
        Object value = gauge.getValue();
        if (value instanceof Number) {
            builder.addField("value", (Number) value);
        } else {
            builder.addField("value", String.valueOf(value));
        }
        return builder.build();
    }

    static Point map(MeasurementInfo info, Instant timestamp, Counter counter) {
        return builder(info, timestamp).addField("count", counter.getCount()).build();
    }

    static Point map(MeasurementInfo info, Instant timestamp, Histogram histogram) {
        HistogramStatistics statistics = histogram.getStatistics();
        return builder(info, timestamp)
                .addField("count", statistics.size())
                .addField("min", statistics.getMin())
                .addField("max", statistics.getMax())
                .addField("mean", statistics.getMean())
                .addField("stddev", statistics.getStdDev())
                .addField("p50", statistics.getQuantile(.50))
                .addField("p75", statistics.getQuantile(.75))
                .addField("p95", statistics.getQuantile(.95))
                .addField("p98", statistics.getQuantile(.98))
                .addField("p99", statistics.getQuantile(.99))
                .addField("p999", statistics.getQuantile(.999))
                .build();
    }

    static Point map(MeasurementInfo info, Instant timestamp, Meter meter) {
        return builder(info, timestamp)
                .addField("count", meter.getCount())
                .addField("rate", meter.getRate())
                .build();
    }

    private static Point.Builder builder(MeasurementInfo info, Instant timestamp) {
        return Point.measurement(info.getName())
                .tag(info.getTags())
                .time(timestamp.toEpochMilli(), TimeUnit.MILLISECONDS);
    }
}
