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

package org.apache.flink.connector.testframe.utils;

import org.apache.flink.streaming.api.CheckpointingMode;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.testframe.utils.CollectIteratorAssertions.assertThat;

/** Unit tests for {@link CollectIteratorAssertTest}. */
public class CollectIteratorAssertTest {
    @Nested
    class MultipleSplitDataMatcherTest {
        private final List<String> splitA = Arrays.asList("alpha", "beta", "gamma");
        private final List<String> splitB = Arrays.asList("one", "two", "three");
        private final List<String> splitC = Arrays.asList("1", "2", "3");
        private final List<List<String>> testDataCollection = Arrays.asList(splitA, splitB, splitC);

        @Test
        public void testDataMatcherWithExactlyOnceSemantic() {
            final List<String> result = unionLists(splitA, splitB, splitC);
            assertThat(result.iterator())
                    .matchesRecordsFromSource(testDataCollection, CheckpointingMode.EXACTLY_ONCE);
        }

        @Test
        public void testDataMatcherWithAtLeastOnceSemantic() {
            final List<String> result = unionLists(splitA, splitB, splitC, splitA);
            assertThat(result.iterator())
                    .matchesRecordsFromSource(testDataCollection, CheckpointingMode.AT_LEAST_ONCE);
        }

        @Test
        public void testResultLessThanExpected() {
            final ArrayList<String> splitATestDataWithoutLast = new ArrayList<>(splitA);
            splitATestDataWithoutLast.remove(splitA.size() - 1);
            final List<String> result = unionLists(splitATestDataWithoutLast, splitB, splitC);
            Assertions.assertThatThrownBy(
                            () ->
                                    assertThat(result.iterator())
                                            .matchesRecordsFromSource(
                                                    testDataCollection,
                                                    CheckpointingMode.EXACTLY_ONCE))
                    .hasMessageContaining(
                            "Expected to have exactly 9 records in result, but only received 8 records\n"
                                    + "Current progress of multiple split test data validation:\n"
                                    + "Split 0 (2/3): \n"
                                    + "alpha\n"
                                    + "beta\n"
                                    + "gamma\t<----\n"
                                    + "Split 1 (3/3): \n"
                                    + "one\n"
                                    + "two\n"
                                    + "three\n"
                                    + "Split 2 (3/3): \n"
                                    + "1\n"
                                    + "2\n"
                                    + "3\n");
        }

        @Test
        public void testResultMoreThanExpected() {
            final List<String> result = unionLists(splitA, splitB, splitC);
            result.add("delta");
            Assertions.assertThatThrownBy(
                            () ->
                                    assertThat(result.iterator())
                                            .matchesRecordsFromSource(
                                                    testDataCollection,
                                                    CheckpointingMode.EXACTLY_ONCE))
                    .hasMessageContaining(
                            "Expected to have exactly 9 records in result, but received more records\n"
                                    + "Current progress of multiple split test data validation:\n"
                                    + "Split 0 (3/3): \n"
                                    + "alpha\n"
                                    + "beta\n"
                                    + "gamma\n"
                                    + "Split 1 (3/3): \n"
                                    + "one\n"
                                    + "two\n"
                                    + "three\n"
                                    + "Split 2 (3/3): \n"
                                    + "1\n"
                                    + "2\n"
                                    + "3\n");
        }

        @Test
        public void testOutOfOrder() {
            List<String> reverted = new ArrayList<>(splitC);
            Collections.reverse(reverted);
            final List<String> result = unionLists(splitA, splitB, reverted);
            Assertions.assertThatThrownBy(
                            () ->
                                    assertThat(result.iterator())
                                            .matchesRecordsFromSource(
                                                    testDataCollection,
                                                    CheckpointingMode.EXACTLY_ONCE))
                    .hasMessageContaining(
                            "Unexpected record '3' at position 6\n"
                                    + "Current progress of multiple split test data validation:\n"
                                    + "Split 0 (3/3): \n"
                                    + "alpha\n"
                                    + "beta\n"
                                    + "gamma\n"
                                    + "Split 1 (3/3): \n"
                                    + "one\n"
                                    + "two\n"
                                    + "three\n"
                                    + "Split 2 (0/3): \n"
                                    + "1\t<----\n"
                                    + "2\n"
                                    + "3\n"
                                    + "Remaining received elements after the unexpected one: \n"
                                    + "2\n"
                                    + "1\n");
        }
    }

    @SafeVarargs
    private final <T> List<T> unionLists(List<T>... lists) {
        return Stream.of(lists).flatMap(Collection::stream).collect(Collectors.toList());
    }
}
