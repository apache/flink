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

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

/**
 * Wrapper to use a Dropwizard {@link com.codahale.metrics.Histogram} as a Flink {@link Histogram}.
 */
public class DropwizardHistogramWrapper implements Histogram {

    private final com.codahale.metrics.Histogram dropwizardHistogram;

    public DropwizardHistogramWrapper(com.codahale.metrics.Histogram dropwizardHistogram) {
        this.dropwizardHistogram = dropwizardHistogram;
    }

    public com.codahale.metrics.Histogram getDropwizardHistogram() {
        return dropwizardHistogram;
    }

    @Override
    public void update(long value) {
        dropwizardHistogram.update(value);
    }

    @Override
    public long getCount() {
        return dropwizardHistogram.getCount();
    }

    @Override
    public HistogramStatistics getStatistics() {
        return new DropwizardHistogramStatistics(dropwizardHistogram.getSnapshot());
    }
}
