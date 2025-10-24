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

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.ValidationException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;

import static org.apache.flink.table.catalog.IntervalFreshness.convertFreshnessToCron;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link IntervalFreshness}. */
public class IntervalFreshnessTest {

    @ParameterizedTest
    @ValueSource(strings = {"2efedd", "2.5", "-2", "0", "12345678901234567890"})
    void testIllegalIntervalFreshness(String invalidInput) {
        assertThatThrownBy(() -> IntervalFreshness.ofMinute(invalidInput))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        String.format(
                                "The freshness interval currently only supports positive integer type values. But was: %s",
                                invalidInput));
    }

    @Test
    void testConvertDurationToFreshnessInterval() {
        // verify second
        IntervalFreshness actualSeconds = IntervalFreshness.fromDuration(Duration.ofSeconds(20));
        assertThat(actualSeconds).isEqualTo(IntervalFreshness.ofSecond("20"));

        // verify minute
        IntervalFreshness actualMinutes = IntervalFreshness.fromDuration(Duration.ofMinutes(3));
        assertThat(actualMinutes).isEqualTo(IntervalFreshness.ofMinute("3"));

        // verify hour
        IntervalFreshness actualHour = IntervalFreshness.fromDuration(Duration.ofHours(1));
        assertThat(actualHour).isEqualTo(IntervalFreshness.ofHour("1"));

        // verify day
        IntervalFreshness actualDay = IntervalFreshness.fromDuration(Duration.ofDays(2));
        assertThat(actualDay).isEqualTo(IntervalFreshness.ofDay("2"));
    }

    @Test
    void testConvertFreshnessToDuration() {
        // verify second
        Duration actualSecond = IntervalFreshness.ofSecond("20").toDuration();
        assertThat(actualSecond).isEqualTo(Duration.ofSeconds(20));

        // verify minute
        Duration actualMinute = IntervalFreshness.ofMinute("3").toDuration();
        assertThat(actualMinute).isEqualTo(Duration.ofMinutes(3));

        // verify hour
        Duration actualHour = IntervalFreshness.ofHour("3").toDuration();
        assertThat(actualHour).isEqualTo(Duration.ofHours(3));

        // verify day
        Duration actualDay = IntervalFreshness.ofDay("3").toDuration();
        assertThat(actualDay).isEqualTo(Duration.ofDays(3));
    }

    @Test
    void testConvertSecondFreshnessToCronExpression() {
        // verify illegal freshness
        assertThatThrownBy(() -> convertFreshnessToCron(IntervalFreshness.ofSecond("90")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "In full refresh mode, freshness must be less than 60 when the time unit is SECOND.");

        assertThatThrownBy(() -> convertFreshnessToCron(IntervalFreshness.ofSecond("32")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "In full refresh mode, only freshness that are factors of 60 are currently supported when the time unit is SECOND.");

        String actual1 = convertFreshnessToCron(IntervalFreshness.ofSecond("30"));
        assertThat(actual1).isEqualTo("0/30 * * * * ? *");

        String actual2 = convertFreshnessToCron(IntervalFreshness.ofSecond("5"));
        assertThat(actual2).isEqualTo("0/5 * * * * ? *");
    }

    @Test
    void testConvertMinuteFreshnessToCronExpression() {
        // verify illegal freshness
        assertThatThrownBy(() -> convertFreshnessToCron(IntervalFreshness.ofMinute("90")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "In full refresh mode, freshness must be less than 60 when the time unit is MINUTE.");

        assertThatThrownBy(() -> convertFreshnessToCron(IntervalFreshness.ofMinute("32")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "In full refresh mode, only freshness that are factors of 60 are currently supported when the time unit is MINUTE.");

        String actual1 = convertFreshnessToCron(IntervalFreshness.ofMinute("30"));
        assertThat(actual1).isEqualTo("0 0/30 * * * ? *");

        String actual2 = convertFreshnessToCron(IntervalFreshness.ofMinute("5"));
        assertThat(actual2).isEqualTo("0 0/5 * * * ? *");

        String actual3 = convertFreshnessToCron(IntervalFreshness.ofMinute("1"));
        assertThat(actual3).isEqualTo("0 0/1 * * * ? *");
    }

    @Test
    void testConvertHourFreshnessToCronExpression() {
        // verify illegal freshness
        assertThatThrownBy(() -> convertFreshnessToCron(IntervalFreshness.ofHour("24")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "In full refresh mode, freshness must be less than 24 when the time unit is HOUR.");

        assertThatThrownBy(() -> convertFreshnessToCron(IntervalFreshness.ofHour("14")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "In full refresh mode, only freshness that are factors of 24 are currently supported when the time unit is HOUR.");

        String actual1 = convertFreshnessToCron(IntervalFreshness.ofHour("12"));
        assertThat(actual1).isEqualTo("0 0 0/12 * * ? *");

        String actual2 = convertFreshnessToCron(IntervalFreshness.ofHour("4"));
        assertThat(actual2).isEqualTo("0 0 0/4 * * ? *");

        String actual3 = convertFreshnessToCron(IntervalFreshness.ofHour("1"));
        assertThat(actual3).isEqualTo("0 0 0/1 * * ? *");
    }

    @Test
    void testConvertDayFreshnessToCronExpression() {
        // verify illegal freshness
        assertThatThrownBy(() -> convertFreshnessToCron(IntervalFreshness.ofDay("2")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "In full refresh mode, freshness must be 1 when the time unit is DAY.");
        String actual1 = convertFreshnessToCron(IntervalFreshness.ofDay("1"));
        assertThat(actual1).isEqualTo("0 0 0 * * ? *");
    }
}
