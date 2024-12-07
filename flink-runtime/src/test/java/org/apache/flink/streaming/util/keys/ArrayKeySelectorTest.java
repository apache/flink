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

package org.apache.flink.streaming.util.keys;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests key selectors on arrays. */
class ArrayKeySelectorTest {

    @Test
    void testObjectArrays() {
        String[] array1 = {"a", "b", "c", "d", "e"};
        String[] array2 = {"v", "w", "x", "y", "z"};

        KeySelectorUtil.ArrayKeySelector<String[]> singleFieldSelector =
                KeySelectorUtil.getSelectorForArray(
                        new int[] {1}, BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO);

        assertThat(singleFieldSelector.getKey(array1)).isEqualTo(new Tuple1<>("b"));
        assertThat(singleFieldSelector.getKey(array2)).isEqualTo(new Tuple1<>("w"));

        KeySelectorUtil.ArrayKeySelector<String[]> twoFieldsSelector =
                KeySelectorUtil.getSelectorForArray(
                        new int[] {3, 0}, BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO);

        assertThat(twoFieldsSelector.getKey(array1)).isEqualTo(new Tuple2<>("d", "a"));
        assertThat(twoFieldsSelector.getKey(array2)).isEqualTo(new Tuple2<>("y", "v"));
    }

    @Test
    void testPrimitiveArrays() {
        int[] array1 = {1, 2, 3, 4, 5};
        int[] array2 = {-5, -4, -3, -2, -1, 0};

        KeySelectorUtil.ArrayKeySelector<int[]> singleFieldSelector =
                KeySelectorUtil.getSelectorForArray(
                        new int[] {1}, PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO);

        assertThat(singleFieldSelector.getKey(array1)).isEqualTo(new Tuple1<>(2));
        assertThat(singleFieldSelector.getKey(array2)).isEqualTo(new Tuple1<>(-4));

        KeySelectorUtil.ArrayKeySelector<int[]> twoFieldsSelector =
                KeySelectorUtil.getSelectorForArray(
                        new int[] {3, 0}, PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO);

        assertThat(twoFieldsSelector.getKey(array1)).isEqualTo(new Tuple2<>(4, 1));
        assertThat(twoFieldsSelector.getKey(array2)).isEqualTo(new Tuple2<>(-2, -5));
    }
}
