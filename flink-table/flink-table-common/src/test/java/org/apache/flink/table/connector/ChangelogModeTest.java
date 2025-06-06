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

package org.apache.flink.table.connector;

import org.apache.flink.types.RowKind;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for base methods of {@link ChangelogMode}. */
class ChangelogModeTest {

    public static Stream<Arguments> toStringParams() {
        return Stream.of(
                Arguments.of(ChangelogMode.upsert(), "[INSERT, UPDATE_AFTER, ~DELETE]"),
                Arguments.of(ChangelogMode.upsert(false), "[INSERT, UPDATE_AFTER, DELETE]"),
                Arguments.of(ChangelogMode.insertOnly(), "[INSERT]"),
                Arguments.of(ChangelogMode.all(), "[INSERT, UPDATE_BEFORE, UPDATE_AFTER, DELETE]"),
                Arguments.of(
                        ChangelogMode.newBuilder()
                                .addContainedKind(RowKind.INSERT)
                                .addContainedKind(RowKind.UPDATE_BEFORE)
                                .addContainedKind(RowKind.UPDATE_AFTER)
                                .addContainedKind(RowKind.DELETE)
                                .keyOnlyDeletes(true)
                                .build(),
                        "[INSERT, UPDATE_BEFORE, UPDATE_AFTER, ~DELETE]"));
    }

    @ParameterizedTest
    @MethodSource("toStringParams")
    void testToString(ChangelogMode mode, String expected) {
        assertThat(mode.toString()).isEqualTo(expected);
    }
}
