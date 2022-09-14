/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.watermarkstatus;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link WatermarkStatus}. */
public class WatermarkStatusTest {

    @Test(expected = IllegalArgumentException.class)
    public void testIllegalCreationThrowsException() {
        new WatermarkStatus(32);
    }

    @Test
    public void testEquals() {
        WatermarkStatus idleStatus = new WatermarkStatus(WatermarkStatus.IDLE_STATUS);
        WatermarkStatus activeStatus = new WatermarkStatus(WatermarkStatus.ACTIVE_STATUS);

        assertEquals(WatermarkStatus.IDLE, idleStatus);
        assertTrue(idleStatus.isIdle());
        assertFalse(idleStatus.isActive());

        assertEquals(WatermarkStatus.ACTIVE, activeStatus);
        assertTrue(activeStatus.isActive());
        assertFalse(activeStatus.isIdle());
    }

    @Test
    public void testTypeCasting() {
        WatermarkStatus status = WatermarkStatus.ACTIVE;

        assertTrue(status.isWatermarkStatus());
        assertFalse(status.isRecord());
        assertFalse(status.isWatermark());
        assertFalse(status.isLatencyMarker());

        try {
            status.asWatermark();
            fail("should throw an exception");
        } catch (Exception e) {
            // expected
        }

        try {
            status.asRecord();
            fail("should throw an exception");
        } catch (Exception e) {
            // expected
        }

        try {
            status.asLatencyMarker();
            fail("should throw an exception");
        } catch (Exception e) {
            // expected
        }
    }
}
