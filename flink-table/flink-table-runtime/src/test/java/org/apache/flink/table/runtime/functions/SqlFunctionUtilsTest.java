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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SqlFunctionUtils}. */
class SqlFunctionUtilsTest {

    /**
     * A SQL literal cannot carry an unpaired surrogate, but one can still reach the function from a
     * UDF or a connector. It is a character of its own there, so it is neither skipped nor treated
     * as half of a pair.
     */
    @Test
    void testOverlayCountsAnUnpairedSurrogateAsOneCharacter() {
        assertThat(SqlFunctionUtils.overlay("a\ud83db", "X", 2, 1)).isEqualTo("aXb");
        assertThat(SqlFunctionUtils.overlay("a\ude00b", "X", 2, 1)).isEqualTo("aXb");
        assertThat(SqlFunctionUtils.overlay("a\ud83db", "X", 1, 1)).isEqualTo("X\ud83db");
        assertThat(SqlFunctionUtils.overlay("a\ud83db", "X", 3, 1)).isEqualTo("a\ud83dX");
    }

    @Test
    void testOverlayOnAStringHoldingOnlySurrogates() {
        assertThat(SqlFunctionUtils.overlay("\ud83d\ud83d", "X", 1, 1)).isEqualTo("X\ud83d");
        assertThat(SqlFunctionUtils.overlay("\ud83d\ud83d", "X", 2, 1)).isEqualTo("\ud83dX");
        assertThat(SqlFunctionUtils.overlay("\ude00\ude00", "X", 2, 1)).isEqualTo("\ude00X");
        assertThat(SqlFunctionUtils.overlay("\ud83d\ud83d", "X", 3, 1)).isEqualTo("\ud83d\ud83d");

        // the same two chars as a well-formed pair are one character, not two
        assertThat(SqlFunctionUtils.overlay("😀", "X", 1, 1)).isEqualTo("X");
        assertThat(SqlFunctionUtils.overlay("😀", "X", 2, 1)).isEqualTo("😀");
    }

    @Test
    void testOverlayMeasuresTheReplacementInCharacters() {
        // without FOR, the replaced length is the character count of the replacement
        assertThat(SqlFunctionUtils.overlay("abc", "\ud83d", 2)).isEqualTo("a\ud83dc");
        assertThat(SqlFunctionUtils.overlay("abc", "😀", 2)).isEqualTo("a😀c");
        assertThat(SqlFunctionUtils.overlay("a\ud83db", "😀", 2, 1)).isEqualTo("a😀b");
    }
}
