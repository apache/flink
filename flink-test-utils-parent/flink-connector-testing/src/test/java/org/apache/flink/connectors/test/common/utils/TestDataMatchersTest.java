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

package org.apache.flink.connectors.test.common.utils;

import org.hamcrest.Description;
import org.hamcrest.StringDescription;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** Unit test for {@link TestDataMatchers}. */
public class TestDataMatchersTest {
    @Nested
    class SplitDataMatcherTest {
        private final List<String> testData = Arrays.asList("alpha", "beta", "gamma");

        @Test
        public void testPositiveCases() {
            assertThat(testData.iterator(), TestDataMatchers.matchesSplitTestData(testData));
            assertThat(
                    testData.iterator(),
                    TestDataMatchers.matchesSplitTestData(testData, testData.size()));
            assertThat(
                    testData.iterator(),
                    TestDataMatchers.matchesSplitTestData(testData, testData.size() - 1));
        }

        @Test
        public void testMismatch() throws Exception {
            final List<String> resultData = new ArrayList<>(testData);
            resultData.set(1, "delta");
            final Iterator<String> resultIterator = resultData.iterator();

            final TestDataMatchers.SplitDataMatcher<String> matcher =
                    TestDataMatchers.matchesSplitTestData(testData);

            assertMatcherFailedWithDescription(
                    resultIterator,
                    matcher,
                    "Mismatched record at position 1: Expected 'beta' but was 'delta'");
        }

        @Test
        public void testResultMoreThanExpected() throws Exception {
            final List<String> resultData = new ArrayList<>(testData);
            resultData.add("delta");
            final Iterator<String> resultIterator = resultData.iterator();

            final TestDataMatchers.SplitDataMatcher<String> matcher =
                    TestDataMatchers.matchesSplitTestData(testData);

            assertMatcherFailedWithDescription(
                    resultIterator, matcher, "Result data is more than test data");
        }

        @Test
        public void testResultLessThanExpected() throws Exception {
            final List<String> resultData = new ArrayList<>(testData);
            resultData.remove(testData.size() - 1);
            final Iterator<String> resultIterator = resultData.iterator();

            final TestDataMatchers.SplitDataMatcher<String> matcher =
                    TestDataMatchers.matchesSplitTestData(testData);

            assertMatcherFailedWithDescription(
                    resultIterator, matcher, "Result data is less than test data");
        }
    }

    @Nested
    class MultipleSplitDataMatcherTest {
        private final List<String> splitA = Arrays.asList("alpha", "beta", "gamma");
        private final List<String> splitB = Arrays.asList("one", "two", "three");
        private final List<String> splitC = Arrays.asList("1", "2", "3");
        private final List<Collection<String>> testDataCollection =
                Arrays.asList(splitA, splitB, splitC);

        @Test
        public void testPositiveCase() {
            final List<String> result = unionLists(splitA, splitB, splitC);
            assertThat(
                    result.iterator(),
                    TestDataMatchers.matchesMultipleSplitTestData(testDataCollection));
        }

        @Test
        public void testResultLessThanExpected() throws Exception {
            final ArrayList<String> splitATestDataWithoutLast = new ArrayList<>(splitA);
            splitATestDataWithoutLast.remove(splitA.size() - 1);
            final List<String> result = unionLists(splitATestDataWithoutLast, splitB, splitC);
            final TestDataMatchers.MultipleSplitDataMatcher<String> matcher =
                    TestDataMatchers.matchesMultipleSplitTestData(testDataCollection);
            assertMatcherFailedWithDescription(
                    result.iterator(), matcher, "Result data is less than test data");
        }

        @Test
        public void testResultMoreThanExpected() throws Exception {
            final List<String> result = unionLists(splitA, splitB, splitC);
            result.add("delta");
            final TestDataMatchers.MultipleSplitDataMatcher<String> matcher =
                    TestDataMatchers.matchesMultipleSplitTestData(testDataCollection);
            assertMatcherFailedWithDescription(
                    result.iterator(), matcher, "Unexpected record 'delta'");
        }

        @Test
        public void testOutOfOrder() throws Exception {
            List<String> reverted = new ArrayList<>(splitC);
            Collections.reverse(reverted);
            final List<String> result = unionLists(splitA, splitB, reverted);
            final TestDataMatchers.MultipleSplitDataMatcher<String> matcher =
                    TestDataMatchers.matchesMultipleSplitTestData(testDataCollection);
            assertMatcherFailedWithDescription(result.iterator(), matcher, "Unexpected record '3'");
        }
    }

    @SafeVarargs
    private final <T> List<T> unionLists(List<T>... lists) {
        return Stream.of(lists).flatMap(Collection::stream).collect(Collectors.toList());
    }

    private <T> void assertMatcherFailedWithDescription(
            T object, TypeSafeDiagnosingMatcher<T> matcher, String expectedDescription)
            throws Exception {
        final Method method =
                TypeSafeDiagnosingMatcher.class.getDeclaredMethod(
                        "matchesSafely", Object.class, Description.class);
        method.setAccessible(true);
        assertFalse((boolean) method.invoke(matcher, object, new Description.NullDescription()));

        final StringDescription actualDescription = new StringDescription();
        method.invoke(matcher, object, actualDescription);
        Assertions.assertEquals(expectedDescription, actualDescription.toString());
    }
}
