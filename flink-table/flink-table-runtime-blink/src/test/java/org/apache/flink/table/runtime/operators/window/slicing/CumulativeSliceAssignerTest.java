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
import static org.junit.Assert.assertTrue;

/** Tests for {@link SliceAssigners.CumulativeSliceAssigner}. */
public class CumulativeSliceAssignerTest extends SliceAssignerTestBase {

    @Test
    public void testSliceAssignment() {
        SliceAssigner assigner =
                SliceAssigners.cumulative(0, Duration.ofSeconds(5), Duration.ofSeconds(1));

        assertEquals(1000L, assignSliceEnd(assigner, 0L));
        assertEquals(5000L, assignSliceEnd(assigner, 4999L));
        assertEquals(6000L, assignSliceEnd(assigner, 5000L));
    }

    @Test
    public void testSliceAssignmentWithOffset() {
        SliceAssigner assigner =
                SliceAssigners.cumulative(0, Duration.ofSeconds(5), Duration.ofSeconds(1))
                        .withOffset(Duration.ofMillis(100));

        assertEquals(1100L, assignSliceEnd(assigner, 100L));
        assertEquals(5100L, assignSliceEnd(assigner, 5099L));
        assertEquals(6100L, assignSliceEnd(assigner, 5100L));
    }

    @Test
    public void testGetWindowStart() {
        SliceAssigner assigner =
                SliceAssigners.cumulative(0, Duration.ofSeconds(5), Duration.ofSeconds(1));

        assertEquals(-5000L, assigner.getWindowStart(0L));
        assertEquals(0L, assigner.getWindowStart(1000L));
        assertEquals(0L, assigner.getWindowStart(2000L));
        assertEquals(0L, assigner.getWindowStart(3000L));
        assertEquals(0L, assigner.getWindowStart(4000L));
        assertEquals(0L, assigner.getWindowStart(5000L));
        assertEquals(5000L, assigner.getWindowStart(6000L));
        assertEquals(5000L, assigner.getWindowStart(8000L));
    }

    @Test
    public void testExpiredSlices() {
        SliceAssigner assigner =
                SliceAssigners.cumulative(0, Duration.ofSeconds(5), Duration.ofSeconds(1));

        // reuse the first slice, skip to cleanup it
        assertEquals(Collections.emptyList(), expiredSlices(assigner, 1000L));

        assertEquals(Collections.singletonList(2000L), expiredSlices(assigner, 2000L));
        assertEquals(Collections.singletonList(3000L), expiredSlices(assigner, 3000L));
        assertEquals(Collections.singletonList(4000L), expiredSlices(assigner, 4000L));
        assertEquals(Arrays.asList(5000L, 1000L), expiredSlices(assigner, 5000L));

        // reuse the first slice, skip to cleanup it
        assertEquals(Collections.emptyList(), expiredSlices(assigner, 6000L));

        assertEquals(Arrays.asList(10000L, 6000L), expiredSlices(assigner, 10000L));
        assertEquals(Arrays.asList(0L, -4000L), expiredSlices(assigner, 0L));
    }

    @Test
    public void testMerge() throws Exception {
        SliceAssigners.CumulativeSliceAssigner assigner =
                SliceAssigners.cumulative(0, Duration.ofSeconds(5), Duration.ofSeconds(1));

        assertEquals(Long.valueOf(1000L), mergeResultSlice(assigner, 1000L));
        assertEquals(Collections.emptyList(), toBeMergedSlices(assigner, 1000L)); // the first slice

        assertEquals(Long.valueOf(1000L), mergeResultSlice(assigner, 2000L));
        assertEquals(Collections.singletonList(2000L), toBeMergedSlices(assigner, 2000L));

        assertEquals(Long.valueOf(1000L), mergeResultSlice(assigner, 3000L));
        assertEquals(Collections.singletonList(3000L), toBeMergedSlices(assigner, 3000L));

        assertEquals(Long.valueOf(1000L), mergeResultSlice(assigner, 4000L));
        assertEquals(Collections.singletonList(4000L), toBeMergedSlices(assigner, 4000L));

        assertEquals(Long.valueOf(1000L), mergeResultSlice(assigner, 5000L));
        assertEquals(Collections.singletonList(5000L), toBeMergedSlices(assigner, 5000L));

        assertEquals(Long.valueOf(6000L), mergeResultSlice(assigner, 6000L));
        assertEquals(Collections.emptyList(), toBeMergedSlices(assigner, 6000L)); // the first slice

        assertEquals(Long.valueOf(6000L), mergeResultSlice(assigner, 8000L));
        assertEquals(Collections.singletonList(8000L), toBeMergedSlices(assigner, 8000L));

        assertEquals(Long.valueOf(6000L), mergeResultSlice(assigner, 10000L));
        assertEquals(Collections.singletonList(10000L), toBeMergedSlices(assigner, 10000L));

        assertEquals(Long.valueOf(-4000L), mergeResultSlice(assigner, 0L));
        assertEquals(Collections.singletonList(0L), toBeMergedSlices(assigner, 0L));
    }

    @Test
    public void testNextTriggerWindow() {
        SliceAssigners.CumulativeSliceAssigner assigner =
                SliceAssigners.cumulative(0, Duration.ofSeconds(5), Duration.ofSeconds(1));

        assertEquals(Optional.empty(), assigner.nextTriggerWindow(0L, () -> false));
        assertEquals(Optional.of(2000L), assigner.nextTriggerWindow(1000L, () -> false));
        assertEquals(Optional.of(3000L), assigner.nextTriggerWindow(2000L, () -> false));
        assertEquals(Optional.of(4000L), assigner.nextTriggerWindow(3000L, () -> false));
        assertEquals(Optional.of(5000L), assigner.nextTriggerWindow(4000L, () -> false));
        assertEquals(Optional.empty(), assigner.nextTriggerWindow(5000L, () -> false));
        assertEquals(Optional.of(7000L), assigner.nextTriggerWindow(6000L, () -> false));

        assertEquals(Optional.empty(), assigner.nextTriggerWindow(0L, () -> true));
        assertEquals(Optional.of(2000L), assigner.nextTriggerWindow(1000L, () -> true));
        assertEquals(Optional.of(3000L), assigner.nextTriggerWindow(2000L, () -> true));
        assertEquals(Optional.of(4000L), assigner.nextTriggerWindow(3000L, () -> true));
        assertEquals(Optional.of(5000L), assigner.nextTriggerWindow(4000L, () -> true));
        assertEquals(Optional.empty(), assigner.nextTriggerWindow(5000L, () -> true));
        assertEquals(Optional.of(7000L), assigner.nextTriggerWindow(6000L, () -> true));
    }

    @Test
    public void testEventTime() {
        SliceAssigner assigner1 =
                SliceAssigners.cumulative(0, Duration.ofSeconds(5), Duration.ofSeconds(1));
        assertTrue(assigner1.isEventTime());

        SliceAssigner assigner2 =
                SliceAssigners.cumulative(-1, Duration.ofSeconds(5), Duration.ofSeconds(1));
        assertFalse(assigner2.isEventTime());
    }

    @Test
    public void testInvalidParameters() {
        assertErrorMessage(
                () -> SliceAssigners.cumulative(0, Duration.ofSeconds(-5), Duration.ofSeconds(1)),
                "Cumulative Window parameters must satisfy maxSize > 0 and step > 0, but got maxSize -5000ms and step 1000ms.");

        assertErrorMessage(
                () -> SliceAssigners.cumulative(0, Duration.ofSeconds(5), Duration.ofSeconds(-1)),
                "Cumulative Window parameters must satisfy maxSize > 0 and step > 0, but got maxSize 5000ms and step -1000ms.");

        assertErrorMessage(
                () -> SliceAssigners.cumulative(0, Duration.ofSeconds(5), Duration.ofSeconds(2)),
                "Cumulative Window requires maxSize must be an integral multiple of step, but got maxSize 5000ms and step 2000ms.");

        // should pass
        SliceAssigners.hopping(0, Duration.ofSeconds(10), Duration.ofSeconds(2))
                .withOffset(Duration.ofSeconds(-1));
    }
}
