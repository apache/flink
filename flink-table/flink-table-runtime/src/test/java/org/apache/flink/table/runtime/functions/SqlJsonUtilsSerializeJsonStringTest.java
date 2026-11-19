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

package org.apache.flink.table.runtime.functions;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SqlJsonUtils#serializeJsonString}. */
class SqlJsonUtilsSerializeJsonStringTest {

    @ParameterizedTest
    @ValueSource(
            strings = {
                "",
                "HeLLo WoRLD",
                "HÉLLo WÖRLD 😀",
                "\177/",
                "\"",
                "\\",
                "\b\f\n\r\t",
                "\0\001\037",
                "x\"y\\z\nw",
                // above 0xFF, with a char to escape after it
                "ĢŜĊ",
                "Ģ\n",
                // one '?' byte for two chars, so the bytes are shorter than the string
                "😀\n",
                // Latin-1 but not ASCII, with a char to escape after it
                "É\n",
                // quote and backslash past the start, found by indexOf
                "xxxxxxxx\"xxxxxxxx",
                "xxxxxxxx\\xxxxxxxx",
                // control chars in the SWAR word loop and in the tail
                "xxxxxxxx\001xxxxxxxx",
                "xxxxxxxxxxxxxxxx\n"
            })
    void testSameAsObjectMapper(String value) {
        assertThat(SqlJsonUtils.serializeJsonString(value))
                .isEqualTo(
                        SqlJsonUtils.serializeJson(SqlJsonUtils.getNodeFactory().textNode(value)));
    }
}
