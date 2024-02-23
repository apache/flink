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

package org.apache.flink.streaming.api.functions.source.datagen;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SequenceGenerator}. */
public class SequenceGeneratorTest {

    @Test
    public void testStartGreaterThanEnd() {
        final long end = 0;
        final long start = Long.MAX_VALUE;
        assertThatThrownBy(() -> SequenceGenerator.longGenerator(start, end))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The start value (%s) cannot be greater than the end value (%s).",
                        start, end);
    }

    @Test
    public void testTooLargeRange() {
        final long start = 0;
        final long end = Long.MAX_VALUE;
        assertThatThrownBy(() -> SequenceGenerator.longGenerator(start, end))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The total size of range (%s, %s) exceeds the maximum limit: Long.MAX_VALUE - 1.",
                        start, end);
    }
}
