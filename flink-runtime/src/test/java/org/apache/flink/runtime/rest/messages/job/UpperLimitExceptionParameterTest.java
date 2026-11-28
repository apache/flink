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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.rest.messages.ConversionException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link UpperLimitExceptionParameter}. */
class UpperLimitExceptionParameterTest {

    private final UpperLimitExceptionParameter parameter = new UpperLimitExceptionParameter();

    @Test
    void testConversionFromStringAcceptsZero() throws Exception {
        assertThat(parameter.convertStringToValue("0")).isZero();
    }

    @Test
    void testConversionFromStringAcceptsPositiveNumber() throws Exception {
        assertThat(parameter.convertStringToValue("5")).isEqualTo(5);
    }

    @Test
    void testConversionFromStringRejectsNegativeNumber() {
        assertThatThrownBy(() -> parameter.convertStringToValue("-1"))
                .isInstanceOf(ConversionException.class)
                .hasMessage("maxExceptions must not be negative, was: -1");
    }

    @Test
    void testConversionFromStringRejectsNonNumber() {
        assertThatThrownBy(() -> parameter.convertStringToValue("abc"))
                .isInstanceOf(NumberFormatException.class);
    }
}
