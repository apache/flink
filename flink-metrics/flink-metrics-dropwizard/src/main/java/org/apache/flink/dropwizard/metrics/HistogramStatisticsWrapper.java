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

package org.apache.flink.dropwizard.metrics;

import org.apache.flink.metrics.HistogramStatistics;

import com.codahale.metrics.Snapshot;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;

/**
 * Wrapper to use Flink's {@link HistogramStatistics} as a Dropwizard {@link Snapshot}. This is
 * necessary to report Flink's histograms via the Dropwizard {@link com.codahale.metrics.Reporter}.
 */
class HistogramStatisticsWrapper extends Snapshot {

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private final HistogramStatistics histogramStatistics;

    HistogramStatisticsWrapper(HistogramStatistics histogramStatistics) {
        this.histogramStatistics = histogramStatistics;
    }

    @Override
    public double getValue(double quantile) {
        return histogramStatistics.getQuantile(quantile);
    }

    @Override
    public long[] getValues() {
        return histogramStatistics.getValues();
    }

    @Override
    public int size() {
        return histogramStatistics.size();
    }

    @Override
    public long getMax() {
        return histogramStatistics.getMax();
    }

    @Override
    public double getMean() {
        return histogramStatistics.getMean();
    }

    @Override
    public long getMin() {
        return histogramStatistics.getMin();
    }

    @Override
    public double getStdDev() {
        return histogramStatistics.getStdDev();
    }

    @Override
    public void dump(OutputStream output) {
        try (PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(output, UTF_8))) {

            for (Long value : histogramStatistics.getValues()) {
                printWriter.printf("%d%n", value);
            }
        }
    }
}
