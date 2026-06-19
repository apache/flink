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

package org.apache.flink.types;

import org.apache.flink.types.bitmap.Bitmap;
import org.apache.flink.types.bitmap.RoaringBitmapData;

import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link RoaringBitmapData}. */
class RoaringBitmapDataTest {

    @Test
    void testBitmapInterfaceStaticMethods() {
        assertThat(Bitmap.from(null)).isNull();
        assertThat(Bitmap.fromBytes(null)).isNull();
        assertThat(Bitmap.fromArray(null)).isNull();
    }

    @Test
    void testStaticConstructors() {
        // empty
        assertThat(RoaringBitmapData.empty().isEmpty()).isTrue();

        // from
        assertThat(RoaringBitmapData.from(RoaringBitmapData.fromArray(new int[] {1, 2})).toArray())
                .containsExactly(1, 2);

        // fromBytes
        assertThat(
                        RoaringBitmapData.fromBytes(
                                        RoaringBitmapData.fromArray(new int[] {1, 2}).toBytes())
                                .toArray())
                .containsExactly(1, 2);
        assertThatThrownBy(() -> RoaringBitmapData.fromBytes(new byte[0]))
                .isInstanceOf(DeserializationException.class)
                .hasMessage("Failed to deserialize bitmap from bytes.");
        assertThatThrownBy(() -> RoaringBitmapData.fromBytes("invalid".getBytes()))
                .isInstanceOf(DeserializationException.class)
                .hasMessage("Failed to deserialize bitmap from bytes.");

        // fromArray
        assertThat(RoaringBitmapData.fromArray(new int[0]).toArray()).containsExactly();
        assertThat(RoaringBitmapData.fromArray(new int[] {1, 2}).toArray()).containsExactly(1, 2);
    }

    @Test
    void testInstanceMethods() {
        RoaringBitmapData rb32 = RoaringBitmapData.empty();
        assertThat(rb32.isEmpty()).isTrue();
        assertThat(Objects.equals(rb32, new RoaringBitmap())).isFalse();

        rb32.add(1);
        assertThat(rb32.toArray()).containsExactly(1);

        rb32.add(1, 3);
        assertThat(rb32.toArray()).containsExactly(1, 2);
        assertThat(rb32.getCardinality()).isEqualTo(2);

        rb32.addN(new int[] {1, 2, 3, 4, 5}, 2, 2);
        assertThat(rb32.toArray()).containsExactly(1, 2, 3, 4);

        rb32.clear();
        assertThat(rb32.isEmpty()).isTrue();

        rb32.add(10);
        rb32.add(11);
        rb32.add(12);
        assertThat(rb32.contains(0)).isFalse();
        assertThat(rb32.toArray()).containsExactly(10, 11, 12);

        List<Integer> list = new ArrayList<>();
        rb32.forEach(list::add);
        assertThat(list).containsExactly(10, 11, 12);

        rb32.remove(12);
        assertThat(rb32.toArray()).containsExactly(10, 11);
        assertThat(rb32.getLongCardinality()).isEqualTo(2);

        rb32.remove(10);
        rb32.remove(12);
        assertThat(rb32.toArray()).containsExactly(11);
    }

    @Test
    void testIllegalMethodCalls() {
        RoaringBitmapData rb32 = RoaringBitmapData.empty();

        // add
        assertThatThrownBy(() -> rb32.add(-1L, 5L)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> rb32.add(5L, 0xFFFFFFFFL + 5))
                .isInstanceOf(IllegalArgumentException.class);

        // addN
        assertThatThrownBy(() -> rb32.addN(null, 0, 1)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> rb32.addN(new int[] {1, 2}, -1, 1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> rb32.addN(new int[] {1, 2}, 0, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> rb32.addN(new int[] {1, 2}, 0, 3))
                .isInstanceOf(IllegalArgumentException.class);

        // toArray
        rb32.add(0L, 0x80000000L);
        // NegativeArraySizeException
        assertThatThrownBy(rb32::toArray).isInstanceOf(RuntimeException.class);
        rb32.clear();
        rb32.add(0L, 0xFFFFFFFFL + 1);
        // ArrayIndexOutOfBoundsException
        assertThatThrownBy(rb32::toArray).isInstanceOf(RuntimeException.class);
    }

    @Test
    void testLogicalOperations() {
        RoaringBitmapData rb32 = RoaringBitmapData.empty();
        rb32.add(1, 5);
        assertThat(rb32.toArray()).containsExactly(1, 2, 3, 4);

        // and
        rb32.and(null);
        assertThat(rb32.toArray()).containsExactly(1, 2, 3, 4);
        rb32.and(RoaringBitmapData.fromArray(new int[] {1, 2, 6, 10}));
        assertThat(rb32.toArray()).containsExactly(1, 2);

        // or
        rb32.or(null);
        assertThat(rb32.toArray()).containsExactly(1, 2);
        rb32.or(RoaringBitmapData.fromArray(new int[] {1, 6, 10}));
        assertThat(rb32.toArray()).containsExactly(1, 2, 6, 10);

        // xor
        rb32.xor(null);
        assertThat(rb32.toArray()).containsExactly(1, 2, 6, 10);
        rb32.xor(RoaringBitmapData.fromArray(new int[] {1, 2, 3, 4, 5}));
        assertThat(rb32.toArray()).containsExactly(3, 4, 5, 6, 10);

        // andNot
        rb32.andNot(null);
        assertThat(rb32.toArray()).containsExactly(3, 4, 5, 6, 10);
        rb32.andNot(RoaringBitmapData.fromArray(new int[] {1, 2, 3, 4, 5}));
        assertThat(rb32.toArray()).containsExactly(6, 10);
    }

    @Test
    void testLargeCardinality() {
        RoaringBitmapData rb32 = RoaringBitmapData.empty();
        rb32.add(1, Integer.MAX_VALUE);
        assertThat(rb32.getCardinality()).isEqualTo(Integer.MAX_VALUE - 1);
        assertThat(rb32.getLongCardinality()).isEqualTo(Integer.MAX_VALUE - 1);

        rb32.add(Integer.MAX_VALUE);
        assertThat(rb32.getCardinality()).isEqualTo(Integer.MAX_VALUE);
        assertThat(rb32.getLongCardinality()).isEqualTo(Integer.MAX_VALUE);

        rb32.add(-1);
        rb32.add(Integer.MIN_VALUE);
        assertThat(rb32.getCardinality()).isEqualTo((int) ((long) Integer.MAX_VALUE + 2));
        assertThat(rb32.getLongCardinality()).isEqualTo((long) Integer.MAX_VALUE + 2);
    }

    @Test
    void testOutputFormat() {
        RoaringBitmapData rb32 = RoaringBitmapData.empty();
        assertThat(rb32.toArray()).containsExactly();
        assertThat(rb32.toString()).isEqualTo("{}");

        rb32.add(0L, 4L);
        assertThat(rb32.toArray()).containsExactly(0, 1, 2, 3);
        assertThat(rb32.toString()).isEqualTo("{0,1,2,3}");

        rb32.add(-1);
        assertThat(rb32.toArray()).containsExactly(0, 1, 2, 3, -1);
        assertThat(rb32.toString()).isEqualTo(String.format("{0,1,2,3,%s}", 0xFFFFFFFFL));

        rb32.add(Integer.MIN_VALUE);
        assertThat(rb32.toArray()).containsExactly(0, 1, 2, 3, Integer.MIN_VALUE, -1);
        assertThat(rb32.toString())
                .isEqualTo(String.format("{0,1,2,3,%s,%s}", 0x80000000L, 0xFFFFFFFFL));

        rb32.add(0L, Integer.MAX_VALUE);
        rb32.add(Integer.MAX_VALUE);
        assertThatThrownBy(rb32::toArray).isInstanceOf(RuntimeException.class);
        String str = rb32.toString();
        assertThat(str.substring(str.length() - 5)).isEqualTo(",...}");
    }

    @Test
    void testHashCode() {
        RoaringBitmap rbm1 = new RoaringBitmap();
        RoaringBitmap rbm2 = new RoaringBitmap();
        RoaringBitmapData rb32 = RoaringBitmapData.empty();
        for (int i = 0; i < 100; i++) {
            rbm1.add(i);
            rbm2.add(i);
            rb32.add(i);
        }
        rbm2.runOptimize();
        assertThat(rbm1.hashCode()).isNotEqualTo(rbm2.hashCode());
        assertThat(rb32.hashCode()).isEqualTo(rbm2.hashCode());
    }
}
