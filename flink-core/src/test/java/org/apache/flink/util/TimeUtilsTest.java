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

import org.apache.flink.api.common.time.Time;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TimeUtils}. */
class TimeUtilsTest {

    @Test
    void testParseDurationNanos() {
        assertThat(TimeUtils.parseDuration("424562ns").getNano()).isEqualTo(424562);
        assertThat(TimeUtils.parseDuration("424562nano").getNano()).isEqualTo(424562);
        assertThat(TimeUtils.parseDuration("424562nanos").getNano()).isEqualTo(424562);
        assertThat(TimeUtils.parseDuration("424562nanosecond").getNano()).isEqualTo(424562);
        assertThat(TimeUtils.parseDuration("424562nanoseconds").getNano()).isEqualTo(424562);
        assertThat(TimeUtils.parseDuration("424562 ns").getNano()).isEqualTo(424562);
    }

    @Test
    void testParseDurationMicros() {
        assertThat(TimeUtils.parseDuration("565731µs").getNano()).isEqualTo(565731 * 1000L);
        assertThat(TimeUtils.parseDuration("565731micro").getNano()).isEqualTo(565731 * 1000L);
        assertThat(TimeUtils.parseDuration("565731micros").getNano()).isEqualTo(565731 * 1000L);
        assertThat(TimeUtils.parseDuration("565731microsecond").getNano())
                .isEqualTo(565731 * 1000L);
        assertThat(TimeUtils.parseDuration("565731microseconds").getNano())
                .isEqualTo(565731 * 1000L);
        assertThat(TimeUtils.parseDuration("565731 µs").getNano()).isEqualTo(565731 * 1000L);
    }

    @Test
    void testParseDurationMillis() {
        assertThat(TimeUtils.parseDuration("1234").toMillis()).isEqualTo(1234);
        assertThat(TimeUtils.parseDuration("1234ms").toMillis()).isEqualTo(1234);
        assertThat(TimeUtils.parseDuration("1234milli").toMillis()).isEqualTo(1234);
        assertThat(TimeUtils.parseDuration("1234millis").toMillis()).isEqualTo(1234);
        assertThat(TimeUtils.parseDuration("1234millisecond").toMillis()).isEqualTo(1234);
        assertThat(TimeUtils.parseDuration("1234milliseconds").toMillis()).isEqualTo(1234);
        assertThat(TimeUtils.parseDuration("1234 ms").toMillis()).isEqualTo(1234);
    }

    @Test
    void testParseDurationSeconds() {
        assertThat(TimeUtils.parseDuration("667766s").getSeconds()).isEqualTo(667766);
        assertThat(TimeUtils.parseDuration("667766sec").getSeconds()).isEqualTo(667766);
        assertThat(TimeUtils.parseDuration("667766secs").getSeconds()).isEqualTo(667766);
        assertThat(TimeUtils.parseDuration("667766second").getSeconds()).isEqualTo(667766);
        assertThat(TimeUtils.parseDuration("667766seconds").getSeconds()).isEqualTo(667766);
        assertThat(TimeUtils.parseDuration("667766 s").getSeconds()).isEqualTo(667766);
    }

    @Test
    void testParseDurationMinutes() {
        assertThat(TimeUtils.parseDuration("7657623m").toMinutes()).isEqualTo(7657623);
        assertThat(TimeUtils.parseDuration("7657623min").toMinutes()).isEqualTo(7657623);
        assertThat(TimeUtils.parseDuration("7657623minute").toMinutes()).isEqualTo(7657623);
        assertThat(TimeUtils.parseDuration("7657623minutes").toMinutes()).isEqualTo(7657623);
        assertThat(TimeUtils.parseDuration("7657623 min").toMinutes()).isEqualTo(7657623);
    }

    @Test
    void testParseDurationHours() {
        assertThat(TimeUtils.parseDuration("987654h").toHours()).isEqualTo(987654);
        assertThat(TimeUtils.parseDuration("987654hour").toHours()).isEqualTo(987654);
        assertThat(TimeUtils.parseDuration("987654hours").toHours()).isEqualTo(987654);
        assertThat(TimeUtils.parseDuration("987654 h").toHours()).isEqualTo(987654);
    }

    @Test
    void testParseDurationDays() {
        assertThat(TimeUtils.parseDuration("987654d").toDays()).isEqualTo(987654);
        assertThat(TimeUtils.parseDuration("987654day").toDays()).isEqualTo(987654);
        assertThat(TimeUtils.parseDuration("987654days").toDays()).isEqualTo(987654);
        assertThat(TimeUtils.parseDuration("987654 d").toDays()).isEqualTo(987654);
    }

    @Test
    void testParseDurationUpperCase() {
        assertThat(TimeUtils.parseDuration("1 NS").toNanos()).isOne();
        assertThat(TimeUtils.parseDuration("1 MICRO").toNanos()).isEqualTo(1000L);
        assertThat(TimeUtils.parseDuration("1 MS").toMillis()).isOne();
        assertThat(TimeUtils.parseDuration("1 S").getSeconds()).isOne();
        assertThat(TimeUtils.parseDuration("1 MIN").toMinutes()).isOne();
        assertThat(TimeUtils.parseDuration("1 H").toHours()).isOne();
        assertThat(TimeUtils.parseDuration("1 D").toDays()).isOne();
    }

    @Test
    void testParseDurationTrim() {
        assertThat(TimeUtils.parseDuration("      155      ").toMillis()).isEqualTo(155L);
        assertThat(TimeUtils.parseDuration("      155      ms   ").toMillis()).isEqualTo(155L);
    }

    @Test
    void testParseDurationInvalid() {
        // null
        assertThatThrownBy(() -> TimeUtils.parseDuration(null))
                .isInstanceOf(NullPointerException.class);

        // empty
        assertThatThrownBy(() -> TimeUtils.parseDuration(""))
                .isInstanceOf(IllegalArgumentException.class);

        // blank
        assertThatThrownBy(() -> TimeUtils.parseDuration("     "))
                .isInstanceOf(IllegalArgumentException.class);

        // no number
        assertThatThrownBy(() -> TimeUtils.parseDuration("foobar or fubar or foo bazz"))
                .isInstanceOf(IllegalArgumentException.class);

        // wrong unit
        assertThatThrownBy(() -> TimeUtils.parseDuration("16 gjah"))
                .isInstanceOf(IllegalArgumentException.class);

        // multiple numbers
        assertThatThrownBy(() -> TimeUtils.parseDuration("16 16 17 18 ms"))
                .isInstanceOf(IllegalArgumentException.class);

        // negative number
        assertThatThrownBy(() -> TimeUtils.parseDuration("-100 ms"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testParseDurationNumberOverflow() {
        assertThatThrownBy(() -> TimeUtils.parseDuration("100000000000000000000000000000000 ms"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testGetStringInMillis() {
        assertThat(TimeUtils.getStringInMillis(Duration.ofMillis(4567L))).isEqualTo("4567ms");
        assertThat(TimeUtils.getStringInMillis(Duration.ofSeconds(4567L))).isEqualTo("4567000ms");
        assertThat(TimeUtils.getStringInMillis(Duration.of(4567L, ChronoUnit.MICROS)))
                .isEqualTo("4ms");
    }

    @Test
    void testToDuration() {
        final Time time = Time.of(1337, TimeUnit.MICROSECONDS);
        final Duration duration = TimeUtils.toDuration(time);

        assertThat(duration.toNanos()).isEqualTo(time.getUnit().toNanos(time.getSize()));
    }
}
