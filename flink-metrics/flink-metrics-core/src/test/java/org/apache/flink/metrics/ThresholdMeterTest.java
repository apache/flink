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

import org.apache.flink.metrics.ThresholdMeter.ThresholdExceedException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;

/** Test time stamp based threshold meter. */
public class ThresholdMeterTest extends TestLogger {

    @Test(expected = ThresholdExceedException.class)
    public void testMaximumFailureCheck() {
        ThresholdMeter rater = new ThresholdMeter(5, Duration.ofSeconds(10));

        for (int i = 0; i < 6; i++) {
            rater.markEvent();
        }

        assertEquals(6, rater.getCount());
        rater.checkAgainstThreshold();
    }

    @Test(expected = ThresholdExceedException.class)
    public void testRateRecordMultipleEvents() throws InterruptedException {
        ThresholdMeter rater = new ThresholdMeter(5, Duration.ofMillis(120));

        for (int i = 0; i < 3; i++) {
            rater.markEvent(2);
            Thread.sleep(30);
        }

        assertEquals(6, rater.getCount());
        rater.checkAgainstThreshold();
    }

    @Test
    public void testMovingRate() throws InterruptedException {
        ThresholdMeter rater = new ThresholdMeter(5, Duration.ofMillis(120));

        for (int i = 0; i < 6; i++) {
            rater.markEvent();
            Thread.sleep(30);
        }

        assertEquals(6, rater.getCount());
        rater.checkAgainstThreshold();
    }
}
