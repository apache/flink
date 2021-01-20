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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link SliceAssigners.TumblingSliceAssigner}. */
public class TumblingSliceAssignerTest extends SliceAssignerTestBase {

    @Test
    public void testSliceAssignment() {
        SliceAssigner assigner = SliceAssigners.tumbling(0, Duration.ofSeconds(5));

        assertEquals(5000L, assignSliceEnd(assigner, 0L));
        assertEquals(5000L, assignSliceEnd(assigner, 4999L));
        assertEquals(10000L, assignSliceEnd(assigner, 5000L));
    }

    @Test
    public void testSliceAssignmentWithOffset() {
        SliceAssigner assigner =
                SliceAssigners.tumbling(0, Duration.ofSeconds(5))
                        .withOffset(Duration.ofMillis(100));

        assertEquals(5100L, assignSliceEnd(assigner, 100L));
        assertEquals(5100L, assignSliceEnd(assigner, 5099L));
        assertEquals(10100L, assignSliceEnd(assigner, 5100L));
    }

    @Test
    public void testGetWindowStart() {
        SliceAssigner assigner = SliceAssigners.tumbling(0, Duration.ofSeconds(5));

        assertEquals(-5000L, assigner.getWindowStart(0L));
        assertEquals(0L, assigner.getWindowStart(5000L));
        assertEquals(5000L, assigner.getWindowStart(10000L));
    }

    @Test
    public void testExpiredSlices() {
        SliceAssigner assigner = SliceAssigners.tumbling(0, Duration.ofSeconds(5));

        assertEquals(Collections.singletonList(0L), expiredSlices(assigner, 0L));
        assertEquals(Collections.singletonList(5000L), expiredSlices(assigner, 5000L));
        assertEquals(Collections.singletonList(10000L), expiredSlices(assigner, 10000L));
    }

    @Test
    public void testEventTime() {
        SliceAssigner assigner1 = SliceAssigners.tumbling(0, Duration.ofSeconds(5));
        assertTrue(assigner1.isEventTime());

        SliceAssigner assigner2 = SliceAssigners.tumbling(-1, Duration.ofSeconds(5));
        assertFalse(assigner2.isEventTime());
    }

    @Test
    public void testInvalidParameters() {
        assertErrorMessage(
                () -> SliceAssigners.tumbling(0, Duration.ofSeconds(-1)),
                "Tumbling Window parameters must satisfy size > 0, but got size -1000ms.");

        assertErrorMessage(
                () ->
                        SliceAssigners.tumbling(0, Duration.ofSeconds(10))
                                .withOffset(Duration.ofSeconds(20)),
                "Tumbling Window parameters must satisfy abs(offset) < size, bot got size 10000ms and offset 20000ms.");

        // should pass
        SliceAssigners.tumbling(0, Duration.ofSeconds(10)).withOffset(Duration.ofSeconds(-1));
    }
}
