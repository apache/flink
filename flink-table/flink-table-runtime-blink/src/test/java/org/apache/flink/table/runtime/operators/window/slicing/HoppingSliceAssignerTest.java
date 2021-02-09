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
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link SliceAssigners.HoppingSliceAssigner}. */
public class HoppingSliceAssignerTest extends SliceAssignerTestBase {

    @Test
    public void testSliceAssignment() {
        SliceAssigner assigner =
                SliceAssigners.hopping(0, Duration.ofSeconds(5), Duration.ofSeconds(1));

        assertEquals(1000L, assignSliceEnd(assigner, 0L));
        assertEquals(5000L, assignSliceEnd(assigner, 4999L));
        assertEquals(6000L, assignSliceEnd(assigner, 5000L));
    }

    @Test
    public void testSliceAssignmentWithOffset() {
        SliceAssigner assigner =
                SliceAssigners.hopping(0, Duration.ofSeconds(5), Duration.ofSeconds(1))
                        .withOffset(Duration.ofMillis(100));

        assertEquals(1100L, assignSliceEnd(assigner, 100L));
        assertEquals(5100L, assignSliceEnd(assigner, 5099L));
        assertEquals(6100L, assignSliceEnd(assigner, 5100L));
    }

    @Test
    public void testGetWindowStart() {
        SliceAssigner assigner =
                SliceAssigners.hopping(0, Duration.ofSeconds(5), Duration.ofSeconds(1));

        assertEquals(-5000L, assigner.getWindowStart(0L));
        assertEquals(-4000L, assigner.getWindowStart(1000L));
        assertEquals(-3000L, assigner.getWindowStart(2000L));
        assertEquals(-2000L, assigner.getWindowStart(3000L));
        assertEquals(-1000L, assigner.getWindowStart(4000L));
        assertEquals(0L, assigner.getWindowStart(5000L));
        assertEquals(1000L, assigner.getWindowStart(6000L));
        assertEquals(5000L, assigner.getWindowStart(10000L));
    }

    @Test
    public void testExpiredSlices() {
        SliceAssigner assigner =
                SliceAssigners.hopping(0, Duration.ofSeconds(5), Duration.ofSeconds(1));

        assertEquals(Collections.singletonList(-4000L), expiredSlices(assigner, 0L));
        assertEquals(Collections.singletonList(1000L), expiredSlices(assigner, 5000L));
        assertEquals(Collections.singletonList(6000L), expiredSlices(assigner, 10000L));
    }

    @Test
    public void testMerge() throws Exception {
        SliceAssigners.HoppingSliceAssigner assigner =
                SliceAssigners.hopping(0, Duration.ofSeconds(5), Duration.ofSeconds(1));

        assertNull(mergeResultSlice(assigner, 0L));
        assertEquals(
                Arrays.asList(0L, -1000L, -2000L, -3000L, -4000L), toBeMergedSlices(assigner, 0L));

        assertNull(mergeResultSlice(assigner, 5000L));
        assertEquals(
                Arrays.asList(5000L, 4000L, 3000L, 2000L, 1000L),
                toBeMergedSlices(assigner, 5000L));

        assertNull(mergeResultSlice(assigner, 6000L));
        assertEquals(
                Arrays.asList(6000L, 5000L, 4000L, 3000L, 2000L),
                toBeMergedSlices(assigner, 6000L));
    }

    @Test
    public void testNextTriggerWindow() {
        SliceAssigners.HoppingSliceAssigner assigner =
                SliceAssigners.hopping(0, Duration.ofSeconds(5), Duration.ofSeconds(1));

        assertEquals(Optional.of(1000L), assigner.nextTriggerWindow(0L, () -> false));
        assertEquals(Optional.of(2000L), assigner.nextTriggerWindow(1000L, () -> false));
        assertEquals(Optional.of(3000L), assigner.nextTriggerWindow(2000L, () -> false));
        assertEquals(Optional.of(4000L), assigner.nextTriggerWindow(3000L, () -> false));
        assertEquals(Optional.of(5000L), assigner.nextTriggerWindow(4000L, () -> false));
        assertEquals(Optional.of(6000L), assigner.nextTriggerWindow(5000L, () -> false));
        assertEquals(Optional.of(7000L), assigner.nextTriggerWindow(6000L, () -> false));

        assertEquals(Optional.empty(), assigner.nextTriggerWindow(0L, () -> true));
        assertEquals(Optional.empty(), assigner.nextTriggerWindow(1000L, () -> true));
        assertEquals(Optional.empty(), assigner.nextTriggerWindow(2000L, () -> true));
        assertEquals(Optional.empty(), assigner.nextTriggerWindow(3000L, () -> true));
        assertEquals(Optional.empty(), assigner.nextTriggerWindow(4000L, () -> true));
        assertEquals(Optional.empty(), assigner.nextTriggerWindow(5000L, () -> true));
        assertEquals(Optional.empty(), assigner.nextTriggerWindow(6000L, () -> true));
    }

    @Test
    public void testEventTime() {
        SliceAssigner assigner1 =
                SliceAssigners.hopping(0, Duration.ofSeconds(5), Duration.ofSeconds(1));
        assertTrue(assigner1.isEventTime());

        SliceAssigner assigner2 =
                SliceAssigners.hopping(-1, Duration.ofSeconds(5), Duration.ofSeconds(1));
        assertFalse(assigner2.isEventTime());
    }

    @Test
    public void testInvalidParameters() {
        assertErrorMessage(
                () -> SliceAssigners.hopping(0, Duration.ofSeconds(-2), Duration.ofSeconds(1)),
                "Hopping Window must satisfy slide > 0 and size > 0, but got slide 1000ms and size -2000ms.");

        assertErrorMessage(
                () -> SliceAssigners.hopping(0, Duration.ofSeconds(2), Duration.ofSeconds(-1)),
                "Hopping Window must satisfy slide > 0 and size > 0, but got slide -1000ms and size 2000ms.");

        assertErrorMessage(
                () -> SliceAssigners.hopping(0, Duration.ofSeconds(5), Duration.ofSeconds(2)),
                "Slicing Hopping Window requires size must be an integral multiple of slide, but got size 5000ms and slide 2000ms.");

        // should pass
        SliceAssigners.hopping(0, Duration.ofSeconds(10), Duration.ofSeconds(5))
                .withOffset(Duration.ofSeconds(-1));
    }
}
