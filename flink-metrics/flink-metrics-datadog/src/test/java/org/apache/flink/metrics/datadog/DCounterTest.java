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

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DCounter}. */
class DCounterTest {

    @Test
    void testGetMetricValue() {
        final Counter backingCounter = new SimpleCounter();
        final DCounter counter =
                new DCounter(
                        backingCounter, "counter", "localhost", Collections.emptyList(), () -> 0);

        // sane initial state
        assertThat(counter.getMetricValue()).isEqualTo(0L);
        counter.ackReport();
        assertThat(counter.getMetricValue()).isEqualTo(0L);

        // value is compared against initial state 0
        backingCounter.inc(10);
        assertThat(counter.getMetricValue()).isEqualTo(10L);

        // last value was not acked, should still be compared against initial state 0
        backingCounter.inc(10);
        assertThat(counter.getMetricValue()).isEqualTo(20L);

        // last value (20) acked, now target of comparison
        counter.ackReport();
        assertThat(counter.getMetricValue()).isEqualTo(0L);

        // we now compare against the acked value
        backingCounter.inc(10);
        assertThat(counter.getMetricValue()).isEqualTo(10L);

        // properly handle decrements
        backingCounter.dec(10);
        assertThat(counter.getMetricValue()).isEqualTo(0L);
    }
}
