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

package org.apache.flink.table.runtime.operators.window.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.window.TimeWindow;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Duration;

import static org.apache.flink.table.runtime.operators.window.WindowTestUtils.timeWindow;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for {@link TumblingWindowAssigner}. */
public class TumblingWindowAssignerTest {

    private static final RowData ELEMENT = GenericRowData.of("String");
    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testWindowAssignment() {
        TumblingWindowAssigner assigner = TumblingWindowAssigner.of(Duration.ofMillis(5000));

        assertThat(assigner.assignWindows(ELEMENT, 0L), contains(timeWindow(0, 5000)));
        assertThat(assigner.assignWindows(ELEMENT, 4999L), contains(timeWindow(0, 5000)));
        assertThat(assigner.assignWindows(ELEMENT, 5000L), contains(timeWindow(5000, 10000)));
    }

    @Test
    public void testWindowAssignmentWithOffset() {
        TumblingWindowAssigner assigner =
                TumblingWindowAssigner.of(Duration.ofMillis(5000))
                        .withOffset(Duration.ofMillis(100));

        assertThat(assigner.assignWindows(ELEMENT, 100L), contains(timeWindow(100, 5100)));
        assertThat(assigner.assignWindows(ELEMENT, 5099L), contains(timeWindow(100, 5100)));
        assertThat(assigner.assignWindows(ELEMENT, 5100L), contains(timeWindow(5100, 10100)));
    }

    @Test
    public void testInvalidParameters() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("size > 0");
        TumblingWindowAssigner.of(Duration.ofSeconds(-1));

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("size > 0");
        TumblingWindowAssigner.of(Duration.ofSeconds(10)).withOffset(Duration.ofSeconds(20));

        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("size > 0");
        TumblingWindowAssigner.of(Duration.ofSeconds(10)).withOffset(Duration.ofSeconds(-1));
    }

    @Test
    public void testProperties() {
        TumblingWindowAssigner assigner = TumblingWindowAssigner.of(Duration.ofMillis(5000));

        assertTrue(assigner.isEventTime());
        assertEquals(
                new TimeWindow.Serializer(), assigner.getWindowSerializer(new ExecutionConfig()));

        assertTrue(assigner.withEventTime().isEventTime());
        assertFalse(assigner.withProcessingTime().isEventTime());
    }
}
