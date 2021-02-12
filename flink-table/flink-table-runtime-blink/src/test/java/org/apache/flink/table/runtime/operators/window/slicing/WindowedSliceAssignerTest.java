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

package org.apache.flink.table.runtime.operators.window.slicing;

import org.junit.Test;

import java.time.Duration;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for {@link SliceAssigners.WindowedSliceAssigner}. */
public class WindowedSliceAssignerTest extends SliceAssignerTestBase {

    @Test
    public void testSliceAssignment() {
        SliceAssigner assigner = SliceAssigners.windowed(0, Duration.ofSeconds(5));

        assertEquals(0L, assignSliceEnd(assigner, 0L));
        assertEquals(5000L, assignSliceEnd(assigner, 5000L));
        assertEquals(10000L, assignSliceEnd(assigner, 10000L));
    }

    @Test
    public void testGetWindowStart() {
        SliceAssigner assigner = SliceAssigners.windowed(0, Duration.ofSeconds(5));

        assertEquals(-5000L, assigner.getWindowStart(0L));
        assertEquals(0L, assigner.getWindowStart(1000L));
        assertEquals(0L, assigner.getWindowStart(2000L));
        assertEquals(0L, assigner.getWindowStart(3000L));
        assertEquals(0L, assigner.getWindowStart(4000L));
        assertEquals(0L, assigner.getWindowStart(5000L));
        assertEquals(5000L, assigner.getWindowStart(6000L));
        assertEquals(5000L, assigner.getWindowStart(10000L));
    }

    @Test
    public void testExpiredSlices() {
        SliceAssigner assigner = SliceAssigners.windowed(0, Duration.ofSeconds(5));

        assertEquals(Collections.singletonList(0L), expiredSlices(assigner, 0L));
        assertEquals(Collections.singletonList(5000L), expiredSlices(assigner, 5000L));
        assertEquals(Collections.singletonList(10000L), expiredSlices(assigner, 10000L));
    }

    @Test
    public void testEventTime() {
        SliceAssigner assigner =
                SliceAssigners.hopping(0, Duration.ofSeconds(5), Duration.ofSeconds(1));
        assertTrue(assigner.isEventTime());
    }

    @Test
    public void testInvalidParameters() {
        assertErrorMessage(
                () -> SliceAssigners.windowed(-1, Duration.ofSeconds(5)),
                "Windowed slice assigner must have a positive window end index.");

        assertErrorMessage(
                () -> SliceAssigners.windowed(0, Duration.ofSeconds(-5)),
                "Windowed Window parameters must satisfy size > 0, but got size -5000ms.");

        // should pass
        SliceAssigners.tumbling(1, Duration.ofSeconds(10));
    }
}
