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

import org.apache.flink.runtime.rest.messages.FlameGraphTypeQueryParameter.Type;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlameGraphTypeQueryParameter}. */
class FlameGraphTypeQueryParameterTest {

    private final FlameGraphTypeQueryParameter param = new FlameGraphTypeQueryParameter();

    @Test
    void keyIsType() {
        assertThat(param.getKey()).isEqualTo("type");
    }

    @Test
    void parameterIsOptional() {
        assertThat(param.isMandatory()).isFalse();
    }

    @Test
    void parsesValuesCaseInsensitively() {
        assertThat(param.convertStringToValue("full")).isEqualTo(Type.FULL);
        assertThat(param.convertStringToValue("FULL")).isEqualTo(Type.FULL);
        assertThat(param.convertStringToValue("on_cpu")).isEqualTo(Type.ON_CPU);
        assertThat(param.convertStringToValue("Off_Cpu")).isEqualTo(Type.OFF_CPU);
    }

    @Test
    void rejectsUnknownValueWithIllegalArgumentException() {
        // HandlerRequest wraps any conversion failure in a HandlerRequestException.
        assertThatThrownBy(() -> param.convertStringToValue("nonsense"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void convertValueToStringIsLowerCase() {
        assertThat(param.convertValueToString(Type.FULL)).isEqualTo("full");
        assertThat(param.convertValueToString(Type.ON_CPU)).isEqualTo("on_cpu");
        assertThat(param.convertValueToString(Type.OFF_CPU)).isEqualTo("off_cpu");
    }
}
