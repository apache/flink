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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.runtime.rest.messages.ConversionException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TopNQueryParameter}. */
class TopNQueryParameterTest {

    private final TopNQueryParameter parameter = new TopNQueryParameter();

    @Test
    void testAcceptsValuesInRange() throws Exception {
        assertThat(parameter.convertStringToValue("1")).isEqualTo(1);
        assertThat(parameter.convertStringToValue("5")).isEqualTo(5);
        assertThat(parameter.convertStringToValue(Integer.toString(TopNQueryParameter.MAX_TOP_N)))
                .isEqualTo(TopNQueryParameter.MAX_TOP_N);
    }

    @Test
    void testRejectsNonInteger() {
        assertThatThrownBy(() -> parameter.convertStringToValue("abc"))
                .isInstanceOf(ConversionException.class)
                .hasMessageContaining("integer");
    }

    @Test
    void testRejectsZeroAndNegative() {
        assertThatThrownBy(() -> parameter.convertStringToValue("0"))
                .isInstanceOf(ConversionException.class)
                .hasMessageContaining(">= 1");
        assertThatThrownBy(() -> parameter.convertStringToValue("-1"))
                .isInstanceOf(ConversionException.class)
                .hasMessageContaining(">= 1");
    }

    @Test
    void testRejectsValuesAboveMax() {
        assertThatThrownBy(
                        () ->
                                parameter.convertStringToValue(
                                        Integer.toString(TopNQueryParameter.MAX_TOP_N + 1)))
                .isInstanceOf(ConversionException.class)
                .hasMessageContaining("<= " + TopNQueryParameter.MAX_TOP_N);
    }

    @Test
    void testRoundTripConversion() throws Exception {
        final Integer parsed = parameter.convertStringToValue("7");
        assertThat(parameter.convertValueToString(parsed)).isEqualTo("7");
    }
}
