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

/** Collection of simple mathematical routines. */
public final class MathUtils {

    /**
     * Computes the logarithm of the given value to the base of 2, rounded down. It corresponds to
     * the position of the highest non-zero bit. The position is counted, starting with 0 from the
     * least significant bit to the most significant bit. For example, <code>log2floor(16) = 4
     * </code>, and <code>log2floor(10) = 3</code>.
     *
     * @param value The value to compute the logarithm for.
     * @return The logarithm (rounded down) to the base of 2.
     * @throws ArithmeticException Thrown, if the given value is zero.
     */
    public static int log2floor(int value) throws ArithmeticException {
        if (value == 0) {
            throw new ArithmeticException("Logarithm of zero is undefined.");
        }

        return 31 - Integer.numberOfLeadingZeros(value);
    }

    /**
     * Computes the logarithm of the given value to the base of 2. This method throws an error, if
     * the given argument is not a power of 2.
     *
     * @param value The value to compute the logarithm for.
     * @return The logarithm to the base of 2.
     * @throws ArithmeticException Thrown, if the given value is zero.
     * @throws IllegalArgumentException Thrown, if the given value is not a power of two.
     */
    public static int log2strict(int value) throws ArithmeticException, IllegalArgumentException {
        if (value == 0) {
            throw new ArithmeticException("Logarithm of zero is undefined.");
        }
        if ((value & (value - 1)) != 0) {
            throw new IllegalArgumentException(
                    "The given value " + value + " is not a power of two.");
        }
        return 31 - Integer.numberOfLeadingZeros(value);
    }

    /**
     * Decrements the given number down to the closest power of two. If the argument is a power of
     * two, it remains unchanged.
     *
     * @param value The value to round down.
     * @return The closest value that is a power of two and less or equal than the given value.
     */
    public static int roundDownToPowerOf2(int value) {
        return Integer.highestOneBit(value);
    }

    /**
     * Casts the given value to a 32 bit integer, if it can be safely done. If the cast would change
     * the numeric value, this method raises an exception.
     *
     * <p>This method is a protection in places where one expects to be able to safely case, but
     * where unexpected situations could make the cast unsafe and would cause hidden problems that
     * are hard to track down.
     *
     * @param value The value to be cast to an integer.
     * @return The given value as an integer.
     * @see Math#toIntExact(long)
     */
    public static int checkedDownCast(long value) {
        int downCast = (int) value;
        if (downCast != value) {
            throw new IllegalArgumentException(
                    "Cannot downcast long value " + value + " to integer.");
        }
        return downCast;
    }

    /**
     * Checks whether the given value is a power of two.
     *
     * @param value The value to check.
     * @return True, if the value is a power of two, false otherwise.
     */
    public static boolean isPowerOf2(long value) {
        return (value & (value - 1)) == 0;
    }

    /**
     * This function hashes an integer value. It is adapted from Bob Jenkins' website <a
     * href="http://www.burtleburtle.net/bob/hash/integer.html">http://www.burtleburtle.net/bob/hash/integer.html</a>.
     * The hash function has the <i>full avalanche</i> property, meaning that every bit of the value
     * to be hashed affects every bit of the hash value.
     *
     * <p>It is crucial to use different hash functions to partition data across machines and the
     * internal partitioning of data structures. This hash function is intended for partitioning
     * internally in data structures.
     *
     * @param code The integer to be hashed.
     * @return The non-negative hash code for the integer.
     */
    public static int jenkinsHash(int code) {
        code = (code + 0x7ed55d16) + (code << 12);
        code = (code ^ 0xc761c23c) ^ (code >>> 19);
        code = (code + 0x165667b1) + (code << 5);
        code = (code + 0xd3a2646c) ^ (code << 9);
        code = (code + 0xfd7046c5) + (code << 3);
        code = (code ^ 0xb55a4f09) ^ (code >>> 16);
        return code >= 0 ? code : -(code + 1);
    }

    /**
     * This function hashes an integer value.
     *
     * <p>It is crucial to use different hash functions to partition data across machines and the
     * internal partitioning of data structures. This hash function is intended for partitioning
     * across machines.
     *
     * @param code The integer to be hashed.
     * @return The non-negative hash code for the integer.
     */
    public static int murmurHash(int code) {
        code *= 0xcc9e2d51;
        code = Integer.rotateLeft(code, 15);
        code *= 0x1b873593;

        code = Integer.rotateLeft(code, 13);
        code = code * 5 + 0xe6546b64;

        code ^= 4;
        code = bitMix(code);

        if (code >= 0) {
            return code;
        } else if (code != Integer.MIN_VALUE) {
            return -code;
        } else {
            return 0;
        }
    }

    /**
     * Round the given number to the next power of two.
     *
     * @param x number to round
     * @return x rounded up to the next power of two
     */
    public static int roundUpToPowerOfTwo(int x) {
        x = x - 1;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        return x + 1;
    }

    /**
     * Pseudo-randomly maps a long (64-bit) to an integer (32-bit) using some bit-mixing for better
     * distribution.
     *
     * @param in the long (64-bit)input.
     * @return the bit-mixed int (32-bit) output
     */
    public static int longToIntWithBitMixing(long in) {
        in = (in ^ (in >>> 30)) * 0xbf58476d1ce4e5b9L;
        in = (in ^ (in >>> 27)) * 0x94d049bb133111ebL;
        in = in ^ (in >>> 31);
        return (int) in;
    }

    /**
     * Bit-mixing for pseudo-randomization of integers (e.g., to guard against bad hash functions).
     * Implementation is from Murmur's 32 bit finalizer.
     *
     * @param in the input value
     * @return the bit-mixed output value
     */
    public static int bitMix(int in) {
        in ^= in >>> 16;
        in *= 0x85ebca6b;
        in ^= in >>> 13;
        in *= 0xc2b2ae35;
        in ^= in >>> 16;
        return in;
    }

    /**
     * Flips the sign bit (most-significant-bit) of the input.
     *
     * @param in the input value.
     * @return the input with a flipped sign bit (most-significant-bit).
     */
    public static long flipSignBit(long in) {
        return in ^ Long.MIN_VALUE;
    }

    /**
     * Divide and rounding up to integer. E.g., divideRoundUp(3, 2) returns 2, divideRoundUp(0, 3)
     * returns 0. Note that this method does not support negative values.
     *
     * @param dividend value to be divided by the divisor
     * @param divisor value by which the dividend is to be divided
     * @return the quotient rounding up to integer
     */
    public static int divideRoundUp(int dividend, int divisor) {
        Preconditions.checkArgument(dividend >= 0, "Negative dividend is not supported.");
        Preconditions.checkArgument(divisor > 0, "Negative or zero divisor is not supported.");
        return dividend == 0 ? 0 : (dividend - 1) / divisor + 1;
    }

    // ============================================================================================

    /** Prevent Instantiation through private constructor. */
    private MathUtils() {}
}
