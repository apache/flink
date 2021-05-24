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

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.apache.flink.metrics.View.UPDATE_INTERVAL_SECONDS;
import static org.junit.Assert.assertEquals;

/** Tests for the MeterView. */
public class MeterViewTest extends TestLogger {
    @Test
    public void testGetCount() {
        Counter c = new SimpleCounter();
        c.inc(5);
        Meter m = new MeterView(c);

        assertEquals(5, m.getCount());
    }

    @Test
    public void testMarkEvent() {
        Counter c = new SimpleCounter();
        Meter m = new MeterView(c);

        assertEquals(0, m.getCount());
        m.markEvent();
        assertEquals(1, m.getCount());
        m.markEvent(2);
        assertEquals(3, m.getCount());
    }

    @Test
    public void testGetRate() {
        Counter c = new SimpleCounter();
        MeterView m = new MeterView(c);

        // values = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        for (int x = 0; x < 12; x++) {
            m.markEvent(10);
            m.update();
        }
        // values = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120]
        assertEquals(2.0, m.getRate(), 0.1); // 120 - 0 / 60

        for (int x = 0; x < 12; x++) {
            m.markEvent(10);
            m.update();
        }
        // values = [130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230, 240, 120]
        assertEquals(2.0, m.getRate(), 0.1); // 240 - 120 / 60

        for (int x = 0; x < 6; x++) {
            m.markEvent(20);
            m.update();
        }
        // values = [280, 300, 320, 340, 360, 180, 190, 200, 210, 220, 230, 240, 260]
        assertEquals(3.0, m.getRate(), 0.1); // 360 - 180 / 60

        for (int x = 0; x < 6; x++) {
            m.markEvent(20);
            m.update();
        }
        // values = [280, 300, 320, 340, 360, 380, 400, 420, 440, 460, 480, 240, 260]
        assertEquals(4.0, m.getRate(), 0.1); // 480 - 240 / 60

        for (int x = 0; x < 6; x++) {
            m.update();
        }
        // values = [480, 480, 480, 480, 360, 380, 400, 420, 440, 460, 480, 480, 480]
        assertEquals(2.0, m.getRate(), 0.1); // 480 - 360 / 60

        for (int x = 0; x < 6; x++) {
            m.update();
        }
        // values = [480, 480, 480, 480, 480, 480, 480, 480, 480, 480, 480, 480, 480]
        assertEquals(0.0, m.getRate(), 0.1); // 480 - 480 / 60
    }

    @Test
    public void testTimeSpanBelowUpdateRate() {
        int timeSpanInSeconds = 1;
        MeterView m = new MeterView(timeSpanInSeconds);
        assert timeSpanInSeconds < UPDATE_INTERVAL_SECONDS;
        m.markEvent();
        m.update();
        assertEquals(0.2, m.getRate(), 0.0);
    }
}
