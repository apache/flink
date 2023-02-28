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

import org.assertj.core.api.Assertions;
import org.junit.Test;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SequenceGenerator}. */
public class SequenceGeneratorTest {

    @Test
    public void testStartGreaterThanEnd() {
        assertThatThrownBy(
                        () -> {
                            final long start = 30;
                            final long end = 20;
                            SequenceGenerator.longGenerator(start, end);
                        })
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "The start value cannot be greater than the end value."));
    }

    @Test
    public void testTotalQuantity() {
        assertThatThrownBy(
                        () -> {
                            final long start = 0;
                            final long end = Long.MAX_VALUE;
                            SequenceGenerator.longGenerator(start, end);
                        })
                .satisfies(
                        anyCauseMatches(
                                IllegalArgumentException.class,
                                "The total quantity exceeds the maximum limit: Long.MAX_VALUE - 1."));

        final long start1 = 0;
        final long end1 = Long.MAX_VALUE - 1;
        SequenceGenerator<Long> generator1 = SequenceGenerator.longGenerator(start1, end1);
        Assertions.assertThat(generator1.getEnd() - generator1.getStart())
                .describedAs("The maximum total should be: Long.MAX_VALUE - 1.")
                .isEqualTo(Long.MAX_VALUE - 1);

        final long start2 = 33;
        final long end2 = 44;
        SequenceGenerator<Long> generator2 = SequenceGenerator.longGenerator(start2, end2);
        Assertions.assertThat(generator2.getEnd() - generator2.getStart())
                .describedAs("The total quantity should be equal.")
                .isEqualTo(end2 - start2);
    }
}
