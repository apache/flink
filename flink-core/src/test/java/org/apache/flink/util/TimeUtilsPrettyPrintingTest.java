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

package org.apache.flink.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TimeUtils#formatWithHighestUnit(Duration)}. */
class TimeUtilsPrettyPrintingTest {

    private static Stream<Arguments> testDurationAndExpectedString() {
        return Stream.of(
                Arguments.of(Duration.ofMinutes(3).plusSeconds(30), "210 s"),
                Arguments.of(Duration.ofNanos(100), "100 ns"),
                Arguments.of(Duration.ofSeconds(120), "2 min"),
                Arguments.of(Duration.ofMillis(200), "200 ms"),
                Arguments.of(Duration.ofHours(1).plusSeconds(3), "3603 s"),
                Arguments.of(Duration.ofSeconds(0), "0 ms"),
                Arguments.of(Duration.ofMillis(60000), "1 min"),
                Arguments.of(Duration.ofHours(23), "23 h"),
                Arguments.of(Duration.ofMillis(-1), "-1 ms"),
                Arguments.of(Duration.ofMillis(TimeUnit.DAYS.toMillis(1)), "1 d"),
                Arguments.of(Duration.ofHours(24), "1 d"));
    }

    @ParameterizedTest
    @MethodSource("testDurationAndExpectedString")
    void testFormatting(Duration duration, String expectedString) {
        assertThat(TimeUtils.formatWithHighestUnit(duration)).isEqualTo(expectedString);
    }
}
