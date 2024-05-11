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

import org.junit.jupiter.api.Test;

import static org.apache.flink.util.TernaryBoolean.FALSE;
import static org.apache.flink.util.TernaryBoolean.TRUE;
import static org.apache.flink.util.TernaryBoolean.UNDEFINED;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link TernaryBoolean} class. */
class TernaryBooleanTest {

    @Test
    void testWithDefault() {
        assertThat(TRUE.getOrDefault(true)).isTrue();
        assertThat(TRUE.getOrDefault(false)).isTrue();

        assertThat(FALSE.getOrDefault(true)).isFalse();
        assertThat(FALSE.getOrDefault(false)).isFalse();

        assertThat(UNDEFINED.getOrDefault(true)).isTrue();
        assertThat(UNDEFINED.getOrDefault(false)).isFalse();
    }

    @Test
    void testResolveUndefined() {
        assertThat(TRUE.resolveUndefined(true)).isEqualTo(TRUE);
        assertThat(TRUE.resolveUndefined(false)).isEqualTo(TRUE);

        assertThat(FALSE.resolveUndefined(true)).isEqualTo(FALSE);
        assertThat(FALSE.resolveUndefined(false)).isEqualTo(FALSE);

        assertThat(UNDEFINED.resolveUndefined(true)).isEqualTo(TRUE);
        assertThat(UNDEFINED.resolveUndefined(false)).isEqualTo(FALSE);
    }

    @Test
    void testToBoolean() {
        assertThat(TRUE.getAsBoolean()).isSameAs(Boolean.TRUE);
        assertThat(FALSE.getAsBoolean()).isSameAs(Boolean.FALSE);
        assertThat(UNDEFINED.getAsBoolean()).isNull();
    }

    @Test
    void testFromBoolean() {
        assertThat(TernaryBoolean.fromBoolean(true)).isEqualTo(TRUE);
        assertThat(TernaryBoolean.fromBoolean(false)).isEqualTo(FALSE);
    }

    @Test
    void testFromBoxedBoolean() {
        assertThat(TernaryBoolean.fromBoxedBoolean(Boolean.TRUE)).isEqualTo(TRUE);
        assertThat(TernaryBoolean.fromBoxedBoolean(Boolean.FALSE)).isEqualTo(FALSE);
        assertThat(TernaryBoolean.fromBoxedBoolean(null)).isEqualTo(UNDEFINED);
    }
}
