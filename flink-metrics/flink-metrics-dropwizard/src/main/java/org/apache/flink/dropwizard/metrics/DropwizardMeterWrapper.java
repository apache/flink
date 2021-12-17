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

import org.apache.flink.metrics.Meter;

/** Wrapper to use a Dropwizard {@link com.codahale.metrics.Meter} as a Flink {@link Meter}. */
public class DropwizardMeterWrapper implements Meter {

    private final com.codahale.metrics.Meter meter;

    public DropwizardMeterWrapper(com.codahale.metrics.Meter meter) {
        this.meter = meter;
    }

    public com.codahale.metrics.Meter getDropwizardMeter() {
        return meter;
    }

    @Override
    public void markEvent() {
        meter.mark();
    }

    @Override
    public void markEvent(long n) {
        meter.mark(n);
    }

    @Override
    public double getRate() {
        return meter.getOneMinuteRate();
    }

    @Override
    public long getCount() {
        return meter.getCount();
    }
}
