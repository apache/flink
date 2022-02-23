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
import org.apache.flink.metrics.util.TestMeter;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Tests for the FlinkMeterWrapper. */
class FlinkMeterWrapperTest {

    private static final double DELTA = 0.0001;

    @Test
    void testWrapper() {
        Meter meter = new TestMeter();

        FlinkMeterWrapper wrapper = new FlinkMeterWrapper(meter);
        assertThat(wrapper.getMeanRate()).isEqualTo(0);
        assertThat(wrapper.getOneMinuteRate()).isEqualTo(5);
        assertThat(wrapper.getFiveMinuteRate()).isEqualTo(0);
        assertThat(wrapper.getFifteenMinuteRate()).isEqualTo(0);
        assertThat(wrapper.getCount()).isEqualTo(100L);
    }

    @Test
    void testMarkOneEvent() {
        Meter meter = mock(Meter.class);

        FlinkMeterWrapper wrapper = new FlinkMeterWrapper(meter);
        wrapper.mark();

        verify(meter).markEvent();
    }

    @Test
    void testMarkSeveralEvents() {
        Meter meter = mock(Meter.class);

        FlinkMeterWrapper wrapper = new FlinkMeterWrapper(meter);
        wrapper.mark(5);

        verify(meter).markEvent(5);
    }
}
