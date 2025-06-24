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

package org.apache.flink.table.runtime.operators.window.tvf.slicing;

import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SliceAssigners.WindowedSliceAssigner}. */
@ExtendWith(ParameterizedTestExtension.class)
class WindowedSliceAssignerTest extends SliceAssignerTestBase {

    private ZoneId shiftTimeZone;

    @Parameters(name = "timezone = {0}")
    private static Collection<ZoneId> parameters() {
        return Arrays.asList(ZoneId.of("America/Los_Angeles"), ZoneId.of("Asia/Shanghai"));
    }

    private SliceAssigner tumbleAssigner;
    private SliceAssigner hopAssigner;
    private SliceAssigner cumulateAssigner;

    @BeforeEach
    void setUp() {
        this.tumbleAssigner = SliceAssigners.tumbling(-1, shiftTimeZone, Duration.ofHours(4));
        this.hopAssigner =
                SliceAssigners.hopping(0, shiftTimeZone, Duration.ofHours(5), Duration.ofHours(1));
        this.cumulateAssigner =
                SliceAssigners.cumulative(
                        0, shiftTimeZone, Duration.ofHours(5), Duration.ofHours(1));
    }

    @TestTemplate
    void testSliceAssignment() {
        SliceAssigner assigner = SliceAssigners.windowed(0, tumbleAssigner);

        assertThat(assignSliceEnd(assigner, utcMills("1970-01-01T00:00:00")))
                .isEqualTo(utcMills("1970-01-01T00:00:00"));
        assertThat(assignSliceEnd(assigner, utcMills("1970-01-01T05:00:00")))
                .isEqualTo(utcMills("1970-01-01T05:00:00"));
        assertThat(assignSliceEnd(assigner, utcMills("1970-01-01T10:00:00")))
                .isEqualTo(utcMills("1970-01-01T10:00:00"));
    }

    @TestTemplate
    void testGetWindowStartForTumble() {
        SliceAssigner assigner = SliceAssigners.windowed(0, tumbleAssigner);

        assertThat(assigner.getWindowStart(utcMills("1970-01-01T00:00:00")))
                .isEqualTo(utcMills("1969-12-31T20:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T04:00:00")))
                .isEqualTo(utcMills("1970-01-01T00:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T08:00:00")))
                .isEqualTo(utcMills("1970-01-01T04:00:00"));
    }

    @TestTemplate
    void testGetWindowStartForHop() {
        SliceAssigner assigner = SliceAssigners.windowed(0, hopAssigner);

        assertThat(assigner.getWindowStart(utcMills("1970-01-01T00:00:00")))
                .isEqualTo(utcMills("1969-12-31T19:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T01:00:00")))
                .isEqualTo(utcMills("1969-12-31T20:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T02:00:00")))
                .isEqualTo(utcMills("1969-12-31T21:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T03:00:00")))
                .isEqualTo(utcMills("1969-12-31T22:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T04:00:00")))
                .isEqualTo(utcMills("1969-12-31T23:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T05:00:00")))
                .isEqualTo(utcMills("1970-01-01T00:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T06:00:00")))
                .isEqualTo(utcMills("1970-01-01T01:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T10:00:00")))
                .isEqualTo(utcMills("1970-01-01T05:00:00"));
    }

    @TestTemplate
    void testGetWindowStartForCumulate() {
        SliceAssigner assigner = SliceAssigners.windowed(0, cumulateAssigner);

        assertThat(assigner.getWindowStart(utcMills("1970-01-01T00:00:00")))
                .isEqualTo(utcMills("1969-12-31T19:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T01:00:00")))
                .isEqualTo(utcMills("1970-01-01T00:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T02:00:00")))
                .isEqualTo(utcMills("1970-01-01T00:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T03:00:00")))
                .isEqualTo(utcMills("1970-01-01T00:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T04:00:00")))
                .isEqualTo(utcMills("1970-01-01T00:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T05:00:00")))
                .isEqualTo(utcMills("1970-01-01T00:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T06:00:00")))
                .isEqualTo(utcMills("1970-01-01T05:00:00"));
        assertThat(assigner.getWindowStart(utcMills("1970-01-01T10:00:00")))
                .isEqualTo(utcMills("1970-01-01T05:00:00"));
    }

    @TestTemplate
    void testExpiredSlices() {
        SliceAssigner assigner = SliceAssigners.windowed(0, tumbleAssigner);

        assertThat(expiredSlices(assigner, utcMills("1970-01-01T00:00:00")))
                .containsExactly(utcMills("1970-01-01T00:00:00"));
        assertThat(expiredSlices(assigner, utcMills("1970-01-01T04:00:00")))
                .containsExactly(utcMills("1970-01-01T04:00:00"));
        assertThat(expiredSlices(assigner, utcMills("1970-01-01T10:00:00")))
                .containsExactly(utcMills("1970-01-01T10:00:00"));
    }

    @TestTemplate
    void testEventTime() {
        SliceAssigner assigner = SliceAssigners.windowed(0, tumbleAssigner);
        assertThat(assigner.isEventTime()).isTrue();
    }

    @TestTemplate
    void testInvalidParameters() {
        assertErrorMessage(
                () -> SliceAssigners.windowed(-1, tumbleAssigner),
                "Windowed slice assigner must have a positive window end index.");

        // should pass
        SliceAssigners.windowed(1, tumbleAssigner);
    }

    private long localMills(String timestampStr) {
        return localMills(timestampStr, shiftTimeZone);
    }
}
