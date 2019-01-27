/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.	See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.	You may obtain a copy of the License at
 *
 *		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.dataformat.util;

import org.apache.flink.core.memory.MemorySegment;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Util for bitSet on {@link MemorySegment}.
 */
public final class BitSetUtil {

	private static final int ADDRESS_BITS_PER_WORD = 6;
	private static final int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;
	private static final int BIT_INDEX_MASK = BITS_PER_WORD - 1;
	private static final int WORD_SIZE = 8;

	/**
	 * Returns the value of the bit with the specified index.
	 */
	public static boolean get(MemorySegment segment, int offset, int index) {
		checkArgument(index >= 0, "index < 0");
		return (segment.getLong(offset + wordBytesIndex(index))
				& (1L << index)) != 0; // Don't need index & BIT_INDEX_MASK, the compiler will help us.
	}

	/**
	 * Sets the bit at the specified index to {@code true}.
	 */
	public static void set(MemorySegment segment, int offset, int index) {
		checkArgument(index >= 0, "index < 0");
		final int wordOffset = offset + wordBytesIndex(index);
		final long word = segment.getLong(wordOffset);
		segment.putLong(wordOffset, word | (1L << index));
	}

	/**
	 * Sets the bit specified by the index to {@code false}.
	 */
	public static void clear(MemorySegment segment, int offset, int index) {
		checkArgument(index >= 0, "index < 0");
		final int wordOffset = offset + wordBytesIndex(index);
		final long word = segment.getLong(wordOffset);
		segment.putLong(wordOffset, word & ~(1L << index));
	}

	/**
	 * Word index by bytes. Eg: 0, 8, 16...
	 */
	private static int wordBytesIndex(int bitIndex) {
		return (bitIndex >> ADDRESS_BITS_PER_WORD) * WORD_SIZE;
	}
}
