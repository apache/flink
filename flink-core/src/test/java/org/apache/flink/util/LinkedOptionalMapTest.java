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

import org.apache.flink.util.LinkedOptionalMap.MergeResult;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test {@link LinkedOptionalMap}. */
class LinkedOptionalMapTest {

    @Test
    void usageExample() {
        LinkedOptionalMap<Class<?>, String> map = new LinkedOptionalMap<>();

        map.put("java.lang.String", String.class, "a string class");
        map.put("scala.Option", null, "a scala Option");
        map.put("java.lang.Boolean", Boolean.class, null);

        assertThat(map.keyNames()).contains("java.lang.String", "scala.Option");
        assertThat(map.absentKeysOrValues()).contains("scala.Option", "java.lang.Boolean");
    }

    @Test
    void overridingKeyWithTheSameKeyName() {
        LinkedOptionalMap<Class<?>, String> map = new LinkedOptionalMap<>();

        map.put("java.lang.String", null, "a string class");
        map.put("java.lang.String", String.class, "a string class");

        assertThat(map.absentKeysOrValues()).isEmpty();
    }

    @Test
    void overridingKeysAndValuesWithTheSameKeyName() {
        LinkedOptionalMap<Class<?>, String> map = new LinkedOptionalMap<>();

        map.put("java.lang.String", null, null);
        map.put("java.lang.String", String.class, "a string class");

        assertThat(map.absentKeysOrValues()).isEmpty();
    }

    @Test
    void overridingAValueWithMissingKeyShouldBeConsideredAsAbsent() {
        LinkedOptionalMap<Class<?>, String> map = new LinkedOptionalMap<>();

        map.put("java.lang.String", null, null);
        map.put("java.lang.String", null, "a string class");

        assertThat(map.absentKeysOrValues()).contains("java.lang.String");
    }

    @Test
    void mergingMapsWithPresentEntriesLeavesNoAbsentKeyNames() {
        LinkedOptionalMap<Class<?>, String> first = new LinkedOptionalMap<>();
        first.put("b", null, null);
        first.put("c", String.class, null);

        LinkedOptionalMap<Class<?>, String> second = new LinkedOptionalMap<>();
        second.put("a", String.class, "aaa");
        second.put("b", String.class, "bbb");
        second.put("c", Void.class, "ccc");
        second.put("d", String.class, "ddd");

        first.putAll(second);

        assertThat(first.absentKeysOrValues()).isEmpty();
    }

    @Test
    void mergingMapsPreserversTheOrderOfTheOriginalMap() {
        LinkedOptionalMap<Class<?>, String> first = new LinkedOptionalMap<>();
        first.put("b", null, null);
        first.put("c", String.class, null);

        LinkedOptionalMap<Class<?>, String> second = new LinkedOptionalMap<>();
        second.put("a", String.class, "aaa");
        second.put("b", String.class, "bbb");
        second.put("c", Void.class, "ccc");
        second.put("d", String.class, "ddd");

        first.putAll(second);

        assertThat(first.keyNames()).contains("b", "c", "a", "d");
    }

    @Test
    void mergingToEmpty() {
        LinkedOptionalMap<Class<?>, String> first = new LinkedOptionalMap<>();

        LinkedOptionalMap<Class<?>, String> second = new LinkedOptionalMap<>();
        second.put("a", String.class, "aaa");
        second.put("b", String.class, "bbb");
        second.put("c", Void.class, "ccc");
        second.put("d", String.class, "ddd");

        first.putAll(second);

        assertThat(first.keyNames()).contains("a", "b", "c", "d");
    }

    @Test
    void unwrapOptionalsWithMissingValueThrows() {
        assertThatThrownBy(
                        () -> {
                            LinkedOptionalMap<Class<?>, String> map = new LinkedOptionalMap<>();

                            map.put("a", String.class, null);

                            map.unwrapOptionals();
                        })
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void unwrapOptionalsWithMissingKeyThrows() {
        assertThatThrownBy(
                        () -> {
                            LinkedOptionalMap<Class<?>, String> map = new LinkedOptionalMap<>();

                            map.put("a", null, "blabla");

                            map.unwrapOptionals();
                        })
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void unwrapOptionalsPreservesOrder() {
        LinkedOptionalMap<Class<?>, String> map = new LinkedOptionalMap<>();

        map.put("a", String.class, "aaa");
        map.put("b", Boolean.class, "bbb");

        LinkedHashMap<Class<?>, String> m = map.unwrapOptionals();

        assertThat(m).containsKeys(String.class, Boolean.class);
        assertThat(m).containsValues("aaa", "bbb");
    }

    @Test
    void testPrefix() {
        LinkedOptionalMap<Class<?>, String> left = new LinkedOptionalMap<>();

        left.put("a", String.class, "aaa");
        left.put("b", String.class, "aaa");

        LinkedOptionalMap<Class<?>, String> right = new LinkedOptionalMap<>(left);

        right.put("c", Boolean.class, "bbb");

        assertThat(LinkedOptionalMap.isLeftPrefixOfRight(left, right)).isTrue();
    }

    @Test
    void testNonPrefix() {
        LinkedOptionalMap<Class<?>, String> left = new LinkedOptionalMap<>();

        left.put("a", String.class, "aaa");
        left.put("c", String.class, "aaa");

        LinkedOptionalMap<Class<?>, String> right = new LinkedOptionalMap<>();

        right.put("b", Boolean.class, "bbb");
        right.put("c", Boolean.class, "bbb");

        assertThat(LinkedOptionalMap.isLeftPrefixOfRight(left, right)).isFalse();
    }

    @Test
    void demoMergeResult() {
        LinkedOptionalMap<Class<?>, String> left = new LinkedOptionalMap<>();
        left.put("b", null, null);
        left.put("c", String.class, null);

        LinkedOptionalMap<Class<?>, String> right = new LinkedOptionalMap<>();
        right.put("b", String.class, "bbb");
        right.put("c", Void.class, "ccc");
        right.put("a", Boolean.class, "aaa");
        right.put("d", Long.class, "ddd");

        MergeResult<Class<?>, String> result = LinkedOptionalMap.mergeRightIntoLeft(left, right);

        assertThat(result.hasMissingKeys()).isFalse();
        assertThat(result.isOrderedSubset()).isTrue();
        assertThat(result.missingKeys()).isEmpty();

        LinkedHashMap<Class<?>, String> merged = result.getMerged();
        assertThat(merged.keySet())
                .containsExactly(String.class, Void.class, Boolean.class, Long.class);
    }
}
