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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link MathUtils}. */
class MathUtilTest {

    @Test
    void testLog2Computation() {
        assertThat(MathUtils.log2floor(1)).isZero();
        assertThat(MathUtils.log2floor(2)).isOne();
        assertThat(MathUtils.log2floor(3)).isOne();
        assertThat(MathUtils.log2floor(4)).isEqualTo(2);
        assertThat(MathUtils.log2floor(5)).isEqualTo(2);
        assertThat(MathUtils.log2floor(7)).isEqualTo(2);
        assertThat(MathUtils.log2floor(8)).isEqualTo(3);
        assertThat(MathUtils.log2floor(9)).isEqualTo(3);
        assertThat(MathUtils.log2floor(16)).isEqualTo(4);
        assertThat(MathUtils.log2floor(17)).isEqualTo(4);
        assertThat(MathUtils.log2floor((0x1 << 13) + 1)).isEqualTo(13);
        assertThat(MathUtils.log2floor(Integer.MAX_VALUE)).isEqualTo(30);
        assertThat(MathUtils.log2floor(-1)).isEqualTo(31);

        assertThatThrownBy(() -> MathUtils.log2floor(0)).isInstanceOf(ArithmeticException.class);
    }

    @Test
    void testRoundDownToPowerOf2() {
        assertThat(MathUtils.roundDownToPowerOf2(0)).isZero();
        assertThat(MathUtils.roundDownToPowerOf2(1)).isOne();
        assertThat(MathUtils.roundDownToPowerOf2(2)).isEqualTo(2);
        assertThat(MathUtils.roundDownToPowerOf2(3)).isEqualTo(2);
        assertThat(MathUtils.roundDownToPowerOf2(4)).isEqualTo(4);
        assertThat(MathUtils.roundDownToPowerOf2(5)).isEqualTo(4);
        assertThat(MathUtils.roundDownToPowerOf2(6)).isEqualTo(4);
        assertThat(MathUtils.roundDownToPowerOf2(7)).isEqualTo(4);
        assertThat(MathUtils.roundDownToPowerOf2(8)).isEqualTo(8);
        assertThat(MathUtils.roundDownToPowerOf2(9)).isEqualTo(8);
        assertThat(MathUtils.roundDownToPowerOf2(15)).isEqualTo(8);
        assertThat(MathUtils.roundDownToPowerOf2(16)).isEqualTo(16);
        assertThat(MathUtils.roundDownToPowerOf2(17)).isEqualTo(16);
        assertThat(MathUtils.roundDownToPowerOf2(31)).isEqualTo(16);
        assertThat(MathUtils.roundDownToPowerOf2(32)).isEqualTo(32);
        assertThat(MathUtils.roundDownToPowerOf2(33)).isEqualTo(32);
        assertThat(MathUtils.roundDownToPowerOf2(42)).isEqualTo(32);
        assertThat(MathUtils.roundDownToPowerOf2(63)).isEqualTo(32);
        assertThat(MathUtils.roundDownToPowerOf2(64)).isEqualTo(64);
        assertThat(MathUtils.roundDownToPowerOf2(125)).isEqualTo(64);
        assertThat(MathUtils.roundDownToPowerOf2(25654)).isEqualTo(16384);
        assertThat(MathUtils.roundDownToPowerOf2(34366363)).isEqualTo(33554432);
        assertThat(MathUtils.roundDownToPowerOf2(63463463)).isEqualTo(33554432);
        assertThat(MathUtils.roundDownToPowerOf2(1852987883)).isEqualTo(1073741824);
        assertThat(MathUtils.roundDownToPowerOf2(Integer.MAX_VALUE)).isEqualTo(1073741824);
    }

    @Test
    void testRoundUpToPowerOf2() {
        assertThat(MathUtils.roundUpToPowerOfTwo(0)).isZero();
        assertThat(MathUtils.roundUpToPowerOfTwo(1)).isOne();
        assertThat(MathUtils.roundUpToPowerOfTwo(2)).isEqualTo(2);
        assertThat(MathUtils.roundUpToPowerOfTwo(3)).isEqualTo(4);
        assertThat(MathUtils.roundUpToPowerOfTwo(4)).isEqualTo(4);
        assertThat(MathUtils.roundUpToPowerOfTwo(5)).isEqualTo(8);
        assertThat(MathUtils.roundUpToPowerOfTwo(6)).isEqualTo(8);
        assertThat(MathUtils.roundUpToPowerOfTwo(7)).isEqualTo(8);
        assertThat(MathUtils.roundUpToPowerOfTwo(8)).isEqualTo(8);
        assertThat(MathUtils.roundUpToPowerOfTwo(9)).isEqualTo(16);
        assertThat(MathUtils.roundUpToPowerOfTwo(15)).isEqualTo(16);
        assertThat(MathUtils.roundUpToPowerOfTwo(16)).isEqualTo(16);
        assertThat(MathUtils.roundUpToPowerOfTwo(17)).isEqualTo(32);
        assertThat(MathUtils.roundUpToPowerOfTwo(31)).isEqualTo(32);
        assertThat(MathUtils.roundUpToPowerOfTwo(32)).isEqualTo(32);
        assertThat(MathUtils.roundUpToPowerOfTwo(33)).isEqualTo(64);
        assertThat(MathUtils.roundUpToPowerOfTwo(42)).isEqualTo(64);
        assertThat(MathUtils.roundUpToPowerOfTwo(63)).isEqualTo(64);
        assertThat(MathUtils.roundUpToPowerOfTwo(64)).isEqualTo(64);
        assertThat(MathUtils.roundUpToPowerOfTwo(125)).isEqualTo(128);
        assertThat(MathUtils.roundUpToPowerOfTwo(25654)).isEqualTo(32768);
        assertThat(MathUtils.roundUpToPowerOfTwo(34366363)).isEqualTo(67108864);
        assertThat(MathUtils.roundUpToPowerOfTwo(67108863)).isEqualTo(67108864);
        assertThat(MathUtils.roundUpToPowerOfTwo(67108864)).isEqualTo(67108864);
        assertThat(MathUtils.roundUpToPowerOfTwo(0x3FFFFFFE)).isEqualTo(0x40000000);
        assertThat(MathUtils.roundUpToPowerOfTwo(0x3FFFFFFF)).isEqualTo(0x40000000);
        assertThat(MathUtils.roundUpToPowerOfTwo(0x40000000)).isEqualTo(0x40000000);
    }

    @Test
    void testPowerOfTwo() {
        assertThat(MathUtils.isPowerOf2(1)).isTrue();
        assertThat(MathUtils.isPowerOf2(2)).isTrue();
        assertThat(MathUtils.isPowerOf2(4)).isTrue();
        assertThat(MathUtils.isPowerOf2(8)).isTrue();
        assertThat(MathUtils.isPowerOf2(32768)).isTrue();
        assertThat(MathUtils.isPowerOf2(65536)).isTrue();
        assertThat(MathUtils.isPowerOf2(1 << 30)).isTrue();
        assertThat(MathUtils.isPowerOf2(1L + Integer.MAX_VALUE)).isTrue();
        assertThat(MathUtils.isPowerOf2(1L << 41)).isTrue();
        assertThat(MathUtils.isPowerOf2(1L << 62)).isTrue();

        assertThat(MathUtils.isPowerOf2(3)).isFalse();
        assertThat(MathUtils.isPowerOf2(5)).isFalse();
        assertThat(MathUtils.isPowerOf2(567923)).isFalse();
        assertThat(MathUtils.isPowerOf2(Integer.MAX_VALUE)).isFalse();
        assertThat(MathUtils.isPowerOf2(Long.MAX_VALUE)).isFalse();
    }

    @Test
    void testFlipSignBit() {
        assertThat(MathUtils.flipSignBit(Long.MIN_VALUE)).isZero();
        assertThat(MathUtils.flipSignBit(0L)).isEqualTo(Long.MIN_VALUE);
        assertThat(MathUtils.flipSignBit(Long.MAX_VALUE)).isEqualTo(-1L);
        assertThat(MathUtils.flipSignBit(-1L)).isEqualTo(Long.MAX_VALUE);
        assertThat(MathUtils.flipSignBit(42L)).isEqualTo(42L | Long.MIN_VALUE);
        assertThat(MathUtils.flipSignBit(-42L)).isEqualTo(-42L & Long.MAX_VALUE);
    }

    @Test
    void testDivideRoundUp() {
        assertThat(MathUtils.divideRoundUp(0, 1)).isZero();
        assertThat(MathUtils.divideRoundUp(0, 2)).isZero();
        assertThat(MathUtils.divideRoundUp(1, 1)).isOne();
        assertThat(MathUtils.divideRoundUp(1, 2)).isOne();
        assertThat(MathUtils.divideRoundUp(2, 1)).isEqualTo(2);
        assertThat(MathUtils.divideRoundUp(2, 2)).isOne();
        assertThat(MathUtils.divideRoundUp(2, 3)).isOne();
    }

    @Test
    void testDivideRoundUpNegativeDividend() {
        assertThatThrownBy(() -> MathUtils.divideRoundUp(-1, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDivideRoundUpNegativeDivisor() {
        assertThatThrownBy(() -> MathUtils.divideRoundUp(1, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testDivideRoundUpZeroDivisor() {
        assertThatThrownBy(() -> MathUtils.divideRoundUp(1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
