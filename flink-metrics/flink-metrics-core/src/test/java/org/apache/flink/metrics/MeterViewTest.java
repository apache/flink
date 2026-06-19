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

package org.apache.flink.metrics;

import org.junit.jupiter.api.Test;

import static org.apache.flink.metrics.View.UPDATE_INTERVAL_SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

/** Tests for the MeterView. */
class MeterViewTest {
    @Test
    void testGetCount() {
        Counter c = new SimpleCounter();
        c.inc(5);
        Meter m = new MeterView(c);

        assertThat(m.getCount()).isEqualTo(5);
    }

    @Test
    void testMarkEvent() {
        Counter c = new SimpleCounter();
        Meter m = new MeterView(c);

        assertThat(m.getCount()).isEqualTo(0);
        m.markEvent();
        assertThat(m.getCount()).isEqualTo(1);
        m.markEvent(2);
        assertThat(m.getCount()).isEqualTo(3);
    }

    @Test
    void testGetRate() {
        Counter c = new SimpleCounter();
        MeterView m = new MeterView(c);

        // values = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        for (int x = 0; x < 12; x++) {
            m.markEvent(10);
            m.update();
        }
        // values = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120]
        assertThat(m.getRate()).isEqualTo(2.0, offset(0.1)); // 120 - 0 / 60

        for (int x = 0; x < 12; x++) {
            m.markEvent(10);
            m.update();
        }
        // values = [130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230, 240, 120]
        assertThat(m.getRate()).isEqualTo(2.0, offset(0.1)); // 240 - 120 / 60

        for (int x = 0; x < 6; x++) {
            m.markEvent(20);
            m.update();
        }
        // values = [280, 300, 320, 340, 360, 180, 190, 200, 210, 220, 230, 240, 260]
        assertThat(m.getRate()).isEqualTo(3.0, offset(0.1)); // 360 - 180 / 60

        for (int x = 0; x < 6; x++) {
            m.markEvent(20);
            m.update();
        }
        // values = [280, 300, 320, 340, 360, 380, 400, 420, 440, 460, 480, 240, 260]
        assertThat(m.getRate()).isEqualTo(4.0, offset(0.1)); // 480 - 240 / 60

        for (int x = 0; x < 6; x++) {
            m.update();
        }
        // values = [480, 480, 480, 480, 360, 380, 400, 420, 440, 460, 480, 480, 480]
        assertThat(m.getRate()).isEqualTo(2.0, offset(0.1)); // 480 - 360 / 60

        for (int x = 0; x < 6; x++) {
            m.update();
        }
        // values = [480, 480, 480, 480, 480, 480, 480, 480, 480, 480, 480, 480, 480]
        assertThat(m.getRate()).isEqualTo(0.0, offset(0.1)); // 480 - 480 / 60
    }

    @Test
    void testTimeSpanBelowUpdateRate() {
        int timeSpanInSeconds = 1;
        // make sure that the chosen timespan is actually lower than update rate
        assertThat(timeSpanInSeconds).isLessThan(UPDATE_INTERVAL_SECONDS);
        MeterView m = new MeterView(timeSpanInSeconds);
        m.markEvent();
        m.update();
        assertThat(m.getRate()).isEqualTo(0.2);
    }
}
