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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** Tests for the {@link DCounter}. */
public class DCounterTest extends TestLogger {

    @Test
    public void testGetMetricValue() {
        final Counter backingCounter = new SimpleCounter();
        final DCounter counter =
                new DCounter(
                        backingCounter, "counter", "localhost", Collections.emptyList(), () -> 0);

        // sane initial state
        assertEquals(0L, counter.getMetricValue());
        counter.ackReport();
        assertEquals(0L, counter.getMetricValue());

        // value is compared against initial state 0
        backingCounter.inc(10);
        assertEquals(10L, counter.getMetricValue());

        // last value was not acked, should still be compared against initial state 0
        backingCounter.inc(10);
        assertEquals(20L, counter.getMetricValue());

        // last value (20) acked, now target of comparison
        counter.ackReport();
        assertEquals(0L, counter.getMetricValue());

        // we now compare against the acked value
        backingCounter.inc(10);
        assertEquals(10L, counter.getMetricValue());

        // properly handle decrements
        backingCounter.dec(10);
        assertEquals(0L, counter.getMetricValue());
    }

    @Test
    public void testGetMetricValueForGauge() {
        Object[] gaugeValue = new Object[1];

        final DGauge<Object> gauge =
                new DGauge<>(
                        () -> gaugeValue[0],
                        "gauge",
                        "localhost",
                        Collections.emptyList(),
                        () -> 0);

        // sane initial state
        assertNull(gauge.getMetricValue());
        gauge.ackReport();

        // true becomes 1
        gaugeValue[0] = true;
        assertEquals(gauge.getMetricValue(), 1);
        gauge.ackReport();

        // false becomes 0
        gaugeValue[0] = false;
        assertEquals(gauge.getMetricValue(), 0);
        gauge.ackReport();

        // integers stay the same
        gaugeValue[0] = 42;
        assertEquals(gauge.getMetricValue(), 42);
        gauge.ackReport();

        // floats stay the same
        gaugeValue[0] = 3.142;
        assertEquals(gauge.getMetricValue(), 3.142);
        gauge.ackReport();
    }
}
