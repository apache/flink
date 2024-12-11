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

package org.apache.flink.table.utils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.TimeZone;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link org.apache.flink.table.utils.DateTimeUtils}. */
class DateTimeUtilsTest {

    @ParameterizedTest
    @MethodSource("ceilParameters")
    void testCeilWithDSTTransition(TestSpec testSpec) {
        assertThat(testSpec.expected.toInstant())
                .isEqualTo(
                        Instant.ofEpochMilli(
                                DateTimeUtils.timestampCeil(
                                        testSpec.timeUnitRange,
                                        testSpec.input.toEpochSecond() * 1000,
                                        testSpec.timeZone)));
    }

    @ParameterizedTest
    @MethodSource("floorParameters")
    void testFloorWithDSTTransition(TestSpec testSpec) {
        assertThat(testSpec.expected.toInstant())
                .isEqualTo(
                        Instant.ofEpochMilli(
                                DateTimeUtils.timestampFloor(
                                        testSpec.timeUnitRange,
                                        testSpec.input.toEpochSecond() * 1000,
                                        testSpec.timeZone)));
    }

    // ----------------------------------------------------------------------------------------------

    private static final class TestSpec {
        private final ZonedDateTime input;
        private final TimeZone timeZone;
        private final DateTimeUtils.TimeUnitRange timeUnitRange;
        private final ZonedDateTime expected;

        public TestSpec(
                LocalDateTime input,
                TimeZone timeZone,
                DateTimeUtils.TimeUnitRange timeUnitRange,
                LocalDateTime expected) {
            this.input = ZonedDateTime.of(input, timeZone.toZoneId());
            this.timeZone = timeZone;
            this.timeUnitRange = timeUnitRange;
            this.expected = ZonedDateTime.of(expected, timeZone.toZoneId());
        }

        @Override
        public String toString() {
            return "TestSpec{"
                    + "timeUnitRange="
                    + timeUnitRange
                    + ", input="
                    + input
                    + ", timeZone="
                    + timeZone
                    + ", expected="
                    + expected
                    + '}';
        }
    }

    static Stream<TestSpec> floorParameters() {
        return Stream.of(
                new TestSpec(
                        LocalDateTime.of(2021, 3, 31, 11, 22, 33, 555555555),
                        TimeZone.getTimeZone("Europe/Rome"),
                        DateTimeUtils.TimeUnitRange.YEAR,
                        LocalDateTime.of(2021, 1, 1, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 3, 31, 23, 34, 56, 567123567),
                        TimeZone.getTimeZone("America/Los_Angeles"),
                        DateTimeUtils.TimeUnitRange.YEAR,
                        LocalDateTime.of(2021, 1, 1, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 3, 31, 0, 0, 0, 0),
                        TimeZone.getTimeZone("Europe/Rome"),
                        DateTimeUtils.TimeUnitRange.QUARTER,
                        LocalDateTime.of(2021, 1, 1, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 3, 31, 23, 59, 59, 999999999),
                        TimeZone.getTimeZone("America/Los_Angeles"),
                        DateTimeUtils.TimeUnitRange.QUARTER,
                        LocalDateTime.of(2021, 1, 1, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 3, 31, 0, 0, 0, 0),
                        TimeZone.getTimeZone("Europe/Rome"),
                        DateTimeUtils.TimeUnitRange.MONTH,
                        LocalDateTime.of(2021, 3, 1, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 3, 31, 18, 56, 23, 123123123),
                        TimeZone.getTimeZone("America/Los_Angeles"),
                        DateTimeUtils.TimeUnitRange.MONTH,
                        LocalDateTime.of(2021, 3, 1, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 3, 31, 18, 49, 57, 123789123),
                        TimeZone.getTimeZone("Europe/Rome"),
                        DateTimeUtils.TimeUnitRange.WEEK,
                        LocalDateTime.of(2021, 3, 28, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 3, 31, 21, 59, 34, 987987987),
                        TimeZone.getTimeZone("America/Los_Angeles"),
                        DateTimeUtils.TimeUnitRange.WEEK,
                        LocalDateTime.of(2021, 3, 28, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 3, 31, 23, 54, 59, 999_999_999),
                        TimeZone.getTimeZone("Europe/Rome"),
                        DateTimeUtils.TimeUnitRange.DAY,
                        LocalDateTime.of(2021, 3, 31, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 3, 31, 23, 23, 45, 987312),
                        TimeZone.getTimeZone("America/Los_Angeles"),
                        DateTimeUtils.TimeUnitRange.DAY,
                        LocalDateTime.of(2021, 3, 31, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 3, 31, 1, 34, 2, 123456),
                        TimeZone.getTimeZone("Europe/Rome"),
                        DateTimeUtils.TimeUnitRange.HOUR,
                        LocalDateTime.of(2021, 3, 31, 1, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 3, 31, 1, 23, 56, 987431),
                        TimeZone.getTimeZone("America/Los_Angeles"),
                        DateTimeUtils.TimeUnitRange.HOUR,
                        LocalDateTime.of(2021, 3, 31, 1, 0, 0, 0)));
    }

    static Stream<TestSpec> ceilParameters() {
        return Stream.of(
                new TestSpec(
                        LocalDateTime.of(2021, 3, 31, 4, 39, 34, 56731),
                        TimeZone.getTimeZone("Europe/Rome"),
                        DateTimeUtils.TimeUnitRange.YEAR,
                        LocalDateTime.of(2022, 1, 1, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 3, 31, 4, 39, 34, 56731),
                        TimeZone.getTimeZone("America/Los_Angeles"),
                        DateTimeUtils.TimeUnitRange.YEAR,
                        LocalDateTime.of(2022, 1, 1, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 10, 25, 16, 23, 34, 123456),
                        TimeZone.getTimeZone("Europe/Rome"),
                        DateTimeUtils.TimeUnitRange.QUARTER,
                        LocalDateTime.of(2022, 1, 1, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 10, 25, 16, 23, 34, 123456),
                        TimeZone.getTimeZone("America/Los_Angeles"),
                        DateTimeUtils.TimeUnitRange.QUARTER,
                        LocalDateTime.of(2022, 1, 1, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 10, 25, 0, 0, 0, 0),
                        TimeZone.getTimeZone("Europe/Rome"),
                        DateTimeUtils.TimeUnitRange.MONTH,
                        LocalDateTime.of(2021, 11, 1, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 10, 25, 0, 0, 0, 0),
                        TimeZone.getTimeZone("America/Los_Angeles"),
                        DateTimeUtils.TimeUnitRange.MONTH,
                        LocalDateTime.of(2021, 11, 1, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 10, 25, 6, 32, 56, 987654321),
                        TimeZone.getTimeZone("Europe/Rome"),
                        DateTimeUtils.TimeUnitRange.WEEK,
                        LocalDateTime.of(2021, 10, 31, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 10, 25, 6, 32, 56, 987654321),
                        TimeZone.getTimeZone("America/Los_Angeles"),
                        DateTimeUtils.TimeUnitRange.WEEK,
                        LocalDateTime.of(2021, 10, 31, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 10, 25, 3, 4, 45, 123456789),
                        TimeZone.getTimeZone("Europe/Rome"),
                        DateTimeUtils.TimeUnitRange.DAY,
                        LocalDateTime.of(2021, 10, 26, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 10, 25, 3, 4, 45, 123456789),
                        TimeZone.getTimeZone("America/Los_Angeles"),
                        DateTimeUtils.TimeUnitRange.DAY,
                        LocalDateTime.of(2021, 10, 26, 0, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 10, 25, 0, 30, 45, 123657),
                        TimeZone.getTimeZone("Europe/Rome"),
                        DateTimeUtils.TimeUnitRange.HOUR,
                        LocalDateTime.of(2021, 10, 25, 1, 0, 0, 0)),
                new TestSpec(
                        LocalDateTime.of(2021, 10, 25, 0, 30, 45, 123657),
                        TimeZone.getTimeZone("America/Los_Angeles"),
                        DateTimeUtils.TimeUnitRange.HOUR,
                        LocalDateTime.of(2021, 10, 25, 1, 0, 0, 0)));
    }
}
