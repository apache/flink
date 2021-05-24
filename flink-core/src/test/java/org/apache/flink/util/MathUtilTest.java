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

import org.junit.Assert;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the {@link MathUtils}. */
public class MathUtilTest {

    @Test
    public void testLog2Computation() {
        assertEquals(0, MathUtils.log2floor(1));
        assertEquals(1, MathUtils.log2floor(2));
        assertEquals(1, MathUtils.log2floor(3));
        assertEquals(2, MathUtils.log2floor(4));
        assertEquals(2, MathUtils.log2floor(5));
        assertEquals(2, MathUtils.log2floor(7));
        assertEquals(3, MathUtils.log2floor(8));
        assertEquals(3, MathUtils.log2floor(9));
        assertEquals(4, MathUtils.log2floor(16));
        assertEquals(4, MathUtils.log2floor(17));
        assertEquals(13, MathUtils.log2floor((0x1 << 13) + 1));
        assertEquals(30, MathUtils.log2floor(Integer.MAX_VALUE));
        assertEquals(31, MathUtils.log2floor(-1));

        try {
            MathUtils.log2floor(0);
            fail();
        } catch (ArithmeticException ignored) {
        }
    }

    @Test
    public void testRoundDownToPowerOf2() {
        assertEquals(0, MathUtils.roundDownToPowerOf2(0));
        assertEquals(1, MathUtils.roundDownToPowerOf2(1));
        assertEquals(2, MathUtils.roundDownToPowerOf2(2));
        assertEquals(2, MathUtils.roundDownToPowerOf2(3));
        assertEquals(4, MathUtils.roundDownToPowerOf2(4));
        assertEquals(4, MathUtils.roundDownToPowerOf2(5));
        assertEquals(4, MathUtils.roundDownToPowerOf2(6));
        assertEquals(4, MathUtils.roundDownToPowerOf2(7));
        assertEquals(8, MathUtils.roundDownToPowerOf2(8));
        assertEquals(8, MathUtils.roundDownToPowerOf2(9));
        assertEquals(8, MathUtils.roundDownToPowerOf2(15));
        assertEquals(16, MathUtils.roundDownToPowerOf2(16));
        assertEquals(16, MathUtils.roundDownToPowerOf2(17));
        assertEquals(16, MathUtils.roundDownToPowerOf2(31));
        assertEquals(32, MathUtils.roundDownToPowerOf2(32));
        assertEquals(32, MathUtils.roundDownToPowerOf2(33));
        assertEquals(32, MathUtils.roundDownToPowerOf2(42));
        assertEquals(32, MathUtils.roundDownToPowerOf2(63));
        assertEquals(64, MathUtils.roundDownToPowerOf2(64));
        assertEquals(64, MathUtils.roundDownToPowerOf2(125));
        assertEquals(16384, MathUtils.roundDownToPowerOf2(25654));
        assertEquals(33554432, MathUtils.roundDownToPowerOf2(34366363));
        assertEquals(33554432, MathUtils.roundDownToPowerOf2(63463463));
        assertEquals(1073741824, MathUtils.roundDownToPowerOf2(1852987883));
        assertEquals(1073741824, MathUtils.roundDownToPowerOf2(Integer.MAX_VALUE));
    }

    @Test
    public void testRoundUpToPowerOf2() {
        assertEquals(0, MathUtils.roundUpToPowerOfTwo(0));
        assertEquals(1, MathUtils.roundUpToPowerOfTwo(1));
        assertEquals(2, MathUtils.roundUpToPowerOfTwo(2));
        assertEquals(4, MathUtils.roundUpToPowerOfTwo(3));
        assertEquals(4, MathUtils.roundUpToPowerOfTwo(4));
        assertEquals(8, MathUtils.roundUpToPowerOfTwo(5));
        assertEquals(8, MathUtils.roundUpToPowerOfTwo(6));
        assertEquals(8, MathUtils.roundUpToPowerOfTwo(7));
        assertEquals(8, MathUtils.roundUpToPowerOfTwo(8));
        assertEquals(16, MathUtils.roundUpToPowerOfTwo(9));
        assertEquals(16, MathUtils.roundUpToPowerOfTwo(15));
        assertEquals(16, MathUtils.roundUpToPowerOfTwo(16));
        assertEquals(32, MathUtils.roundUpToPowerOfTwo(17));
        assertEquals(32, MathUtils.roundUpToPowerOfTwo(31));
        assertEquals(32, MathUtils.roundUpToPowerOfTwo(32));
        assertEquals(64, MathUtils.roundUpToPowerOfTwo(33));
        assertEquals(64, MathUtils.roundUpToPowerOfTwo(42));
        assertEquals(64, MathUtils.roundUpToPowerOfTwo(63));
        assertEquals(64, MathUtils.roundUpToPowerOfTwo(64));
        assertEquals(128, MathUtils.roundUpToPowerOfTwo(125));
        assertEquals(32768, MathUtils.roundUpToPowerOfTwo(25654));
        assertEquals(67108864, MathUtils.roundUpToPowerOfTwo(34366363));
        assertEquals(67108864, MathUtils.roundUpToPowerOfTwo(67108863));
        assertEquals(67108864, MathUtils.roundUpToPowerOfTwo(67108864));
        assertEquals(0x40000000, MathUtils.roundUpToPowerOfTwo(0x3FFFFFFE));
        assertEquals(0x40000000, MathUtils.roundUpToPowerOfTwo(0x3FFFFFFF));
        assertEquals(0x40000000, MathUtils.roundUpToPowerOfTwo(0x40000000));
    }

    @Test
    public void testPowerOfTwo() {
        assertTrue(MathUtils.isPowerOf2(1));
        assertTrue(MathUtils.isPowerOf2(2));
        assertTrue(MathUtils.isPowerOf2(4));
        assertTrue(MathUtils.isPowerOf2(8));
        assertTrue(MathUtils.isPowerOf2(32768));
        assertTrue(MathUtils.isPowerOf2(65536));
        assertTrue(MathUtils.isPowerOf2(1 << 30));
        assertTrue(MathUtils.isPowerOf2(1L + Integer.MAX_VALUE));
        assertTrue(MathUtils.isPowerOf2(1L << 41));
        assertTrue(MathUtils.isPowerOf2(1L << 62));

        assertFalse(MathUtils.isPowerOf2(3));
        assertFalse(MathUtils.isPowerOf2(5));
        assertFalse(MathUtils.isPowerOf2(567923));
        assertFalse(MathUtils.isPowerOf2(Integer.MAX_VALUE));
        assertFalse(MathUtils.isPowerOf2(Long.MAX_VALUE));
    }

    @Test
    public void testFlipSignBit() {
        Assert.assertEquals(0L, MathUtils.flipSignBit(Long.MIN_VALUE));
        Assert.assertEquals(Long.MIN_VALUE, MathUtils.flipSignBit(0L));
        Assert.assertEquals(-1L, MathUtils.flipSignBit(Long.MAX_VALUE));
        Assert.assertEquals(Long.MAX_VALUE, MathUtils.flipSignBit(-1L));
        Assert.assertEquals(42L | Long.MIN_VALUE, MathUtils.flipSignBit(42L));
        Assert.assertEquals(-42L & Long.MAX_VALUE, MathUtils.flipSignBit(-42L));
    }

    @Test
    public void testDivideRoundUp() {
        assertThat(MathUtils.divideRoundUp(0, 1), is(0));
        assertThat(MathUtils.divideRoundUp(0, 2), is(0));
        assertThat(MathUtils.divideRoundUp(1, 1), is(1));
        assertThat(MathUtils.divideRoundUp(1, 2), is(1));
        assertThat(MathUtils.divideRoundUp(2, 1), is(2));
        assertThat(MathUtils.divideRoundUp(2, 2), is(1));
        assertThat(MathUtils.divideRoundUp(2, 3), is(1));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDivideRoundUpNegativeDividend() {
        MathUtils.divideRoundUp(-1, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDivideRoundUpNegativeDivisor() {
        MathUtils.divideRoundUp(1, -1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDivideRoundUpZeroDivisor() {
        MathUtils.divideRoundUp(1, 0);
    }
}
