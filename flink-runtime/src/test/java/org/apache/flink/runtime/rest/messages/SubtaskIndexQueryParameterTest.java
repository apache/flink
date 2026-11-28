/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SubtaskIndexQueryParameter}. */
class SubtaskIndexQueryParameterTest {

    private final SubtaskIndexQueryParameter param = new SubtaskIndexQueryParameter();

    @Test
    void keyIsSubtaskIndex() {
        assertThat(param.getKey()).isEqualTo("subtaskindex");
    }

    @Test
    void parameterIsOptional() {
        assertThat(param.isMandatory()).isFalse();
    }

    @Test
    void acceptsZeroAndPositiveValues() throws Exception {
        assertThat(param.convertStringToValue("0")).isZero();
        assertThat(param.convertStringToValue("3")).isEqualTo(3);
        assertThat(param.convertStringToValue("2147483647")).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void rejectsNegativeValueWithConversionException() {
        assertThatThrownBy(() -> param.convertStringToValue("-1"))
                .isInstanceOf(ConversionException.class)
                .hasMessage("subtaskindex must be positive, was: -1");
    }

    @Test
    void rejectsNonNumericValueWithNumberFormatException() {
        // HandlerRequest wraps any conversion failure in a HandlerRequestException.
        assertThatThrownBy(() -> param.convertStringToValue("abc"))
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    void convertValueToStringReturnsDecimalString() {
        assertThat(param.convertValueToString(7)).isEqualTo("7");
    }
}
