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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Unit test for {@link TestDataMatchers}. */
public class TestDataMatchersTest {
    @Nested
    class MultipleSplitDataMatcherTest {
        private final List<String> splitA = Arrays.asList("alpha", "beta", "gamma");
        private final List<String> splitB = Arrays.asList("one", "two", "three");
        private final List<String> splitC = Arrays.asList("1", "2", "3");
        private final List<List<String>> testDataCollection = Arrays.asList(splitA, splitB, splitC);

        @Test
        public void testPositiveCase() {
            final List<String> result = unionLists(splitA, splitB, splitC);
            assertThat(result.iterator())
                    .satisfies(
                            TestDataMatchers.matchesMultipleSplitTestData(
                                    testDataCollection, CheckpointingMode.EXACTLY_ONCE));
        }

        @Test
        public void testResultLessThanExpected() throws Exception {
            final ArrayList<String> splitATestDataWithoutLast = new ArrayList<>(splitA);
            splitATestDataWithoutLast.remove(splitA.size() - 1);
            final List<String> result = unionLists(splitATestDataWithoutLast, splitB, splitC);
            final TestDataMatchers.MultipleSplitDataMatcher<String> matcher =
                    TestDataMatchers.matchesMultipleSplitTestData(
                            testDataCollection, CheckpointingMode.EXACTLY_ONCE);
            assertMatcherFailedWithDescription(
                    result.iterator(),
                    matcher,
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
        public void testResultMoreThanExpected() throws Exception {
            final List<String> result = unionLists(splitA, splitB, splitC);
            result.add("delta");
            final TestDataMatchers.MultipleSplitDataMatcher<String> matcher =
                    TestDataMatchers.matchesMultipleSplitTestData(
                            testDataCollection, CheckpointingMode.EXACTLY_ONCE);
            assertMatcherFailedWithDescription(
                    result.iterator(),
                    matcher,
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
        public void testOutOfOrder() throws Exception {
            List<String> reverted = new ArrayList<>(splitC);
            Collections.reverse(reverted);
            final List<String> result = unionLists(splitA, splitB, reverted);
            final TestDataMatchers.MultipleSplitDataMatcher<String> matcher =
                    TestDataMatchers.matchesMultipleSplitTestData(
                            testDataCollection, CheckpointingMode.EXACTLY_ONCE);
            String expectedDescription =
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
                            + "1\n";
            assertMatcherFailedWithDescription(result.iterator(), matcher, expectedDescription);
        }
    }

    @SafeVarargs
    private final <T> List<T> unionLists(List<T>... lists) {
        return Stream.of(lists).flatMap(Collection::stream).collect(Collectors.toList());
    }

    private <T> void assertMatcherFailedWithDescription(
            Iterator<T> object,
            TestDataMatchers.MultipleSplitDataMatcher<T> matcher,
            String expectedDescription)
            throws Exception {
        final Method method =
                TestDataMatchers.MultipleSplitDataMatcher.class.getDeclaredMethod(
                        "matches", Iterator.class);
        method.setAccessible(true);
        assertThat((boolean) method.invoke(matcher, object)).isFalse();

        final Field errorMsg =
                TestDataMatchers.MultipleSplitDataMatcher.class.getDeclaredField(
                        "mismatchDescription");
        errorMsg.setAccessible(true);
        Assertions.assertEquals(expectedDescription, errorMsg.get(matcher));
    }
}
