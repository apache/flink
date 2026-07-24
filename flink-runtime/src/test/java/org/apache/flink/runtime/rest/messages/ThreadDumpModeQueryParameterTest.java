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

/** Tests for {@link ThreadDumpModeQueryParameter}. */
class ThreadDumpModeQueryParameterTest {

    private final ThreadDumpModeQueryParameter param = new ThreadDumpModeQueryParameter();

    @Test
    void keyIsMode() {
        assertThat(param.getKey()).isEqualTo("mode");
    }

    @Test
    void parameterIsOptional() {
        assertThat(param.isMandatory()).isFalse();
    }

    @Test
    void parsesLiteCaseInsensitively() {
        assertThat(param.convertStringToValue("lite")).isEqualTo(ThreadDumpMode.LITE);
        assertThat(param.convertStringToValue("LITE")).isEqualTo(ThreadDumpMode.LITE);
        assertThat(param.convertStringToValue(" Lite ")).isEqualTo(ThreadDumpMode.LITE);
    }

    @Test
    void parsesFullCaseInsensitively() {
        assertThat(param.convertStringToValue("full")).isEqualTo(ThreadDumpMode.FULL);
        assertThat(param.convertStringToValue("FULL")).isEqualTo(ThreadDumpMode.FULL);
    }

    @Test
    void rejectsUnknownValueWithIllegalArgumentException() {
        // The handler layer (RestHandlerUtils#convertQueryParameter) turns this into a 400.
        assertThatThrownBy(() -> param.convertStringToValue("nonsense"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void convertValueToStringIsLowerCase() {
        assertThat(param.convertValueToString(ThreadDumpMode.LITE)).isEqualTo("lite");
        assertThat(param.convertValueToString(ThreadDumpMode.FULL)).isEqualTo("full");
    }
}
