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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.table.types.inference.utils.CallContextMock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link PtfArgumentValidators}. */
class PtfArgumentValidatorsTest {

    @Test
    void testNullIsValid() {
        assertThat(PtfArgumentValidators.validateStateTtl(context(true, true, null), 0)).isEmpty();
    }

    @Test
    void testNonLiteralIsRejected() {
        assertThat(PtfArgumentValidators.validateStateTtl(context(false, false, null), 0))
                .hasValueSatisfying(
                        message -> assertThat(message).contains("must be a constant INTERVAL"));
    }

    @Test
    void testNegativeIsRejected() {
        assertThat(
                        PtfArgumentValidators.validateStateTtl(
                                context(true, false, Duration.ofHours(-1)), 0))
                .hasValueSatisfying(
                        message -> assertThat(message).contains("must not be negative"));
    }

    @Test
    void testZeroAndPositiveAreValid() {
        assertThat(PtfArgumentValidators.validateStateTtl(context(true, false, Duration.ZERO), 0))
                .isEmpty();
        assertThat(
                        PtfArgumentValidators.validateStateTtl(
                                context(true, false, Duration.ofHours(1)), 0))
                .isEmpty();
    }

    private static CallContextMock context(
            final boolean literal, final boolean isNull, final Duration value) {
        final CallContextMock mock = new CallContextMock();
        mock.argumentLiterals = List.of(literal);
        mock.argumentNulls = List.of(isNull);
        mock.argumentValues = List.of(Optional.ofNullable(value));
        return mock;
    }
}
