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

package org.apache.flink.test.junit5;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Temporary JUnit 5 tests for validating JUnit jupiter engine truly works. */
class JUnitJupiterTest {
    @Test
    @DisplayName("Assumption and Assertion Test")
    void assumptionAssertionTest() {
        assumeTrue(true, "This case is absolutely true");
        assertThat(true).withFailMessage("This case is absolutely true").isTrue();
    }

    @ParameterizedTest
    @DisplayName("Parameterized Test")
    @ValueSource(strings = {"racecar", "radar", "able was I ere I saw elba"})
    void parameterizedTest(String word) {
        assertThat(isPalindrome(word))
                .withFailMessage("The string in parameter should be palindrome")
                .isTrue();
    }

    @TestFactory
    @DisplayName("Dynamic Test")
    Collection<DynamicTest> dynamicTest() {
        String word = "madam";
        String anotherWord = "flink";
        return Arrays.asList(
                DynamicTest.dynamicTest(
                        "1st dynamic test", () -> assertThat(isPalindrome(word)).isTrue()),
                DynamicTest.dynamicTest(
                        "2nd dynamic test", () -> assertThat(isPalindrome(anotherWord)).isFalse()));
    }

    private boolean isPalindrome(String word) {
        return StringUtils.reverse(word).equals(word);
    }
}
