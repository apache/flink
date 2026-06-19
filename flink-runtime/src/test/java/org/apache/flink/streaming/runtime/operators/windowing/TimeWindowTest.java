/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TimeWindow}. */
class TimeWindowTest {
    @Test
    void testGetWindowStartWithOffset() {
        // [-21, -14), [-14, -7), [-7, 0), [0, 7), [7, 14), [14, 21)...
        long offset = 0;
        assertThat(TimeWindow.getWindowStartWithOffset(-8, offset, 7)).isEqualTo(-14);
        assertThat(TimeWindow.getWindowStartWithOffset(-7, offset, 7)).isEqualTo(-7);
        assertThat(TimeWindow.getWindowStartWithOffset(-6, offset, 7)).isEqualTo(-7);
        assertThat(TimeWindow.getWindowStartWithOffset(-1, offset, 7)).isEqualTo(-7);
        assertThat(TimeWindow.getWindowStartWithOffset(1, offset, 7)).isZero();
        assertThat(TimeWindow.getWindowStartWithOffset(6, offset, 7)).isZero();
        assertThat(TimeWindow.getWindowStartWithOffset(7, offset, 7)).isEqualTo(7);
        assertThat(TimeWindow.getWindowStartWithOffset(8, offset, 7)).isEqualTo(7);

        // [-11, -4), [-4, 3), [3, 10), [10, 17)...
        offset = 3;
        assertThat(TimeWindow.getWindowStartWithOffset(-10, offset, 7)).isEqualTo(-11);
        assertThat(TimeWindow.getWindowStartWithOffset(-9, offset, 7)).isEqualTo(-11);
        assertThat(TimeWindow.getWindowStartWithOffset(-3, offset, 7)).isEqualTo(-4);
        assertThat(TimeWindow.getWindowStartWithOffset(-2, offset, 7)).isEqualTo(-4);
        assertThat(TimeWindow.getWindowStartWithOffset(-1, offset, 7)).isEqualTo(-4);
        assertThat(TimeWindow.getWindowStartWithOffset(1, offset, 7)).isEqualTo(-4);
        assertThat(TimeWindow.getWindowStartWithOffset(2, offset, 7)).isEqualTo(-4);
        assertThat(TimeWindow.getWindowStartWithOffset(3, offset, 7)).isEqualTo(3);
        assertThat(TimeWindow.getWindowStartWithOffset(9, offset, 7)).isEqualTo(3);
        assertThat(TimeWindow.getWindowStartWithOffset(10, offset, 7)).isEqualTo(10);

        // [-16, -9), [-9, -2), [-2, 5), [5, 12), [12, 19)...
        offset = -2;
        assertThat(TimeWindow.getWindowStartWithOffset(-12, offset, 7)).isEqualTo(-16);
        assertThat(TimeWindow.getWindowStartWithOffset(-7, offset, 7)).isEqualTo(-9);
        assertThat(TimeWindow.getWindowStartWithOffset(-4, offset, 7)).isEqualTo(-9);
        assertThat(TimeWindow.getWindowStartWithOffset(-3, offset, 7)).isEqualTo(-9);
        assertThat(TimeWindow.getWindowStartWithOffset(2, offset, 7)).isEqualTo(-2);
        assertThat(TimeWindow.getWindowStartWithOffset(-1, offset, 7)).isEqualTo(-2);
        assertThat(TimeWindow.getWindowStartWithOffset(1, offset, 7)).isEqualTo(-2);
        assertThat(TimeWindow.getWindowStartWithOffset(-2, offset, 7)).isEqualTo(-2);
        assertThat(TimeWindow.getWindowStartWithOffset(3, offset, 7)).isEqualTo(-2);
        assertThat(TimeWindow.getWindowStartWithOffset(4, offset, 7)).isEqualTo(-2);
        assertThat(TimeWindow.getWindowStartWithOffset(7, offset, 7)).isEqualTo(5);
        assertThat(TimeWindow.getWindowStartWithOffset(12, offset, 7)).isEqualTo(12);

        // for GMT+08:00
        offset = -TimeUnit.HOURS.toMillis(8);
        long size = TimeUnit.DAYS.toMillis(1);
        assertThat(TimeWindow.getWindowStartWithOffset(1470902048450L, offset, size))
                .isEqualTo(1470844800000L);
    }

    private boolean intersects(TimeWindow w0, TimeWindow w1) {
        assertThat(w0.intersects(w1)).isEqualTo(w1.intersects(w0));
        return w0.intersects(w1);
    }

    @Test
    void testIntersect() {
        // must intersect with itself
        TimeWindow window = new TimeWindow(10, 20);
        assertThat(window.intersects(window)).isTrue();

        // windows are next to each other
        assertThat(intersects(window, new TimeWindow(20, 30))).isTrue();

        // there is distance between the windows
        assertThat(intersects(window, new TimeWindow(21, 30))).isFalse();

        // overlaps by one
        assertThat(intersects(window, new TimeWindow(19, 22))).isTrue();
    }
}
