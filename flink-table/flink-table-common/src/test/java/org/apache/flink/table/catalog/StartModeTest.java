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
import org.apache.flink.table.catalog.Interval.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StartMode}. */
class StartModeTest {

    @Test
    void acceptsOffsetOfIntegerMaxValueSeconds() {
        assertThat(StartMode.fromNow(Duration.ofSeconds(Integer.MAX_VALUE)).getInterval())
                .isEqualTo(Interval.of(Integer.MAX_VALUE, TimeUnit.SECOND));
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(
            delimiter = '|',
            value = {
                "PT0S          | The start mode offset must be positive, but was: PT0S.",
                "PT-1S         | The start mode offset must be positive, but was: PT-1S.",
                "PT1.5S        | The start mode offset must be a whole number of seconds, but was: PT1.5S.",
                "PT2147483648S | The start mode offset must be at most 2147483647 seconds, but was: PT596523H14M8S."
            })
    void rejectsInvalidOffset(Duration offset, String expectedMessage) {
        assertThatThrownBy(() -> StartMode.fromNow(offset))
                .isInstanceOf(ValidationException.class)
                .hasMessage(expectedMessage);
    }
}
