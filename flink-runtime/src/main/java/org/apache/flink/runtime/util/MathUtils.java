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


package org.apache.flink.runtime.util;

/**
 * Collection of simple mathematical routines.
 */
public final class MathUtils {
	
	/**
	 * Computes the logarithm of the given value to the base of 2, rounded down. It corresponds to the
	 * position of the highest non-zero bit. The position is counted, starting with 0 from the least
	 * significant bit to the most significant bit. For example, <code>log2floor(16) = 4</code>, and
	 * <code>log2floor(10) = 3</code>.
	 * 
	 * @param value The value to compute the logarithm for.
	 * @return The logarithm (rounded down) to the base of 2.
	 * @throws ArithmeticException Thrown, if the given value is zero.
	 */
	public static int log2floor(int value) throws ArithmeticException {
		if (value == 0) {
			throw new ArithmeticException("Logarithm of zero is undefined.");
		}
		
		int log = 0;
		while ((value = value >>> 1) != 0) {
			log++;
		}
		
		return log;
	}
	
	/**
	 * Computes the logarithm of the given value to the base of 2. This method throws an error,
	 * if the given argument is not a power of 2.
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
			throw new IllegalArgumentException("The given value " + value + " is not a power of two.");
		}
		
		int log = 0;
		while ((value = value >>> 1) != 0) {
			log++;
		}
		
		return log;
	}
	
	/**
	 * Decrements the given number down to the closest power of two. If the argument is a
	 * power of two, it remains unchanged.
	 * 
	 * @param value The value to round down.
	 * @return The closest value that is a power of to and less or equal than the given value.
	 */
	public static int roundDownToPowerOf2(int value) {
		return Integer.highestOneBit(value);
	}
	
	/**
	 * Casts the given value to a 32 bit integer, if it can be safely done. If the cast would change the numeric
	 * value, this method raises an exception.
	 * <p>
	 * This method is a protection in places where one expects to be able to safely case, but where unexpected
	 * situations could make the cast unsafe and would cause hidden problems that are hard to track down.
	 * 
	 * @param value The value to be cast to an integer.
	 * @return The given value as an integer.
	 */
	public static int checkedDownCast(long value) {
		if (value > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Cannot downcast long value " + value + " to integer.");
		}
		return (int) value;
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
	 * This function hashes an integer value. It is adapted from Bob Jenkins' website
	 * <a href="http://www.burtleburtle.net/bob/hash/integer.html">http://www.burtleburtle.net/bob/hash/integer.html</a>.
	 * The hash function has the <i>full avalanche</i> property, meaning that every bit of the value to be hashed
	 * affects every bit of the hash value.
	 *
	 * It is crucial to use different hash functions to partition data across machines and the internal partitioning of
	 * data structures. This hash function is intended for partitioning internally in data structures.
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
	 * It is crucial to use different hash functions to partition data across machines and the internal partitioning of
	 * data structures. This hash function is intended for partitioning across machines.
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
		code ^= code >>> 16;
		code *= 0x85ebca6b;
		code ^= code >>> 13;
		code *= 0xc2b2ae35;
		code ^= code >>> 16;

		if (code >= 0) {
			return code;
		}
		else if (code != Integer.MIN_VALUE) {
			return -code;
		}
		else {
			return 0;
		}
	}

	// ============================================================================================
	
	/**
	 * Prevent Instantiation through private constructor.
	 */
	private MathUtils() {}
}
