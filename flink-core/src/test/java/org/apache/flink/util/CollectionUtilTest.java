/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.util.CollectionUtil.HASH_MAP_DEFAULT_LOAD_FACTOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for java collection utilities. */
class CollectionUtilTest {

    @Test
    void testPartition() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4);
        Collection<List<Integer>> partitioned = CollectionUtil.partition(list, 4);

        assertThat(partitioned)
                .as("List partitioned into the an incorrect number of partitions")
                .hasSize(4);
        assertThat(partitioned).allSatisfy(partition -> assertThat(partition).hasSize(1));
    }

    @Test
    void testOfNullableWithNull() {
        assertThat(CollectionUtil.ofNullable(null)).isEmpty();
    }

    @Test
    void testFromNullableWithObject() {
        final Object element = new Object();
        assertThat(CollectionUtil.ofNullable(element)).singleElement().isEqualTo(element);
    }

    @Test
    void testComputeCapacity() {
        assertThat(CollectionUtil.computeRequiredCapacity(0, HASH_MAP_DEFAULT_LOAD_FACTOR)).isOne();
        assertThat(CollectionUtil.computeRequiredCapacity(1, HASH_MAP_DEFAULT_LOAD_FACTOR))
                .isEqualTo(2);
        assertThat(CollectionUtil.computeRequiredCapacity(2, HASH_MAP_DEFAULT_LOAD_FACTOR))
                .isEqualTo(3);
        assertThat(CollectionUtil.computeRequiredCapacity(3, HASH_MAP_DEFAULT_LOAD_FACTOR))
                .isEqualTo(4);
        assertThat(CollectionUtil.computeRequiredCapacity(4, HASH_MAP_DEFAULT_LOAD_FACTOR))
                .isEqualTo(6);
        assertThat(CollectionUtil.computeRequiredCapacity(5, HASH_MAP_DEFAULT_LOAD_FACTOR))
                .isEqualTo(7);
        assertThat(CollectionUtil.computeRequiredCapacity(6, HASH_MAP_DEFAULT_LOAD_FACTOR))
                .isEqualTo(8);
        assertThat(CollectionUtil.computeRequiredCapacity(7, HASH_MAP_DEFAULT_LOAD_FACTOR))
                .isEqualTo(10);
        assertThat(CollectionUtil.computeRequiredCapacity(8, HASH_MAP_DEFAULT_LOAD_FACTOR))
                .isEqualTo(11);
        assertThat(CollectionUtil.computeRequiredCapacity(100, HASH_MAP_DEFAULT_LOAD_FACTOR))
                .isEqualTo(134);
        assertThat(CollectionUtil.computeRequiredCapacity(1000, HASH_MAP_DEFAULT_LOAD_FACTOR))
                .isEqualTo(1334);
        assertThat(CollectionUtil.computeRequiredCapacity(10000, HASH_MAP_DEFAULT_LOAD_FACTOR))
                .isEqualTo(13334);

        assertThat(
                        CollectionUtil.computeRequiredCapacity(
                                Integer.MAX_VALUE / 2, HASH_MAP_DEFAULT_LOAD_FACTOR))
                .isEqualTo(1431655808);

        assertThat(
                        CollectionUtil.computeRequiredCapacity(
                                1 + Integer.MAX_VALUE / 2, HASH_MAP_DEFAULT_LOAD_FACTOR))
                .isEqualTo(Integer.MAX_VALUE);

        assertThatThrownBy(
                        () ->
                                CollectionUtil.computeRequiredCapacity(
                                        -1, HASH_MAP_DEFAULT_LOAD_FACTOR))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(
                        () ->
                                CollectionUtil.computeRequiredCapacity(
                                        Integer.MIN_VALUE, HASH_MAP_DEFAULT_LOAD_FACTOR))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testIsEmptyOrAllElementsNull() {
        assertThat(CollectionUtil.isEmptyOrAllElementsNull(Collections.emptyList())).isTrue();
        assertThat(CollectionUtil.isEmptyOrAllElementsNull(Collections.singletonList(null)))
                .isTrue();
        assertThat(CollectionUtil.isEmptyOrAllElementsNull(Arrays.asList(null, null))).isTrue();
        assertThat(CollectionUtil.isEmptyOrAllElementsNull(Collections.singletonList("test")))
                .isFalse();
        assertThat(CollectionUtil.isEmptyOrAllElementsNull(Arrays.asList(null, "test"))).isFalse();
        assertThat(CollectionUtil.isEmptyOrAllElementsNull(Arrays.asList("test", null))).isFalse();
        assertThat(CollectionUtil.isEmptyOrAllElementsNull(Arrays.asList(null, "test", null)))
                .isFalse();
    }

    @Test
    void testCheckedSubTypeCast() {
        List<A> list = new ArrayList<>();
        B b = new B();
        C c = new C();
        list.add(b);
        list.add(c);
        list.add(null);
        Collection<B> castSuccess = CollectionUtil.checkedSubTypeCast(list, B.class);
        Iterator<B> iterator = castSuccess.iterator();
        assertThat(iterator.next()).isEqualTo(b);
        assertThat(iterator.next()).isEqualTo(c);
        assertThat(iterator.next()).isNull();
        assertThat(iterator).isExhausted();
        assertThatThrownBy(() -> CollectionUtil.checkedSubTypeCast(list, C.class))
                .isInstanceOf(ClassCastException.class);
    }

    static class A {}

    static class B extends A {}

    static class C extends B {}
}
