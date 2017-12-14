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
package org.apache.flink.runtime.operators.util;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.util.Preconditions;

public class BitSet {
	private MemorySegment memorySegment;

	// MemorySegment byte array offset.
	private int offset;

	// The BitSet byte size.
	private int byteLength;

	// The BitSet bit size.
	private int bitLength;

	private final int BYTE_POSITION_MASK = 0xfffffff8;
	private final int BYTE_INDEX_MASK = 0x00000007;

	public BitSet(int byteSize) {
		Preconditions.checkArgument(byteSize > 0, "bits size should be greater than 0.");
		this.byteLength = byteSize;
		this.bitLength = byteSize << 3;
	}

	public void setMemorySegment(MemorySegment memorySegment, int offset) {
		Preconditions.checkArgument(memorySegment != null, "MemorySegment can not be null.");
		Preconditions.checkArgument(offset >= 0, "Offset should be positive integer.");
		Preconditions.checkArgument(offset + byteLength <= memorySegment.size(), 
			"Could not set MemorySegment, the remain buffers is not enough.");
		this.memorySegment = memorySegment;
		this.offset = offset;
	}

	/**
	 * Sets the bit at specified index.
	 *
	 * @param index - position
	 */
	public void set(int index) {
		Preconditions.checkArgument(index < bitLength && index >= 0);

		int byteIndex = (index & BYTE_POSITION_MASK) >>> 3;
		byte current = memorySegment.get(offset + byteIndex);
		current |= (1 << (index & BYTE_INDEX_MASK));
		memorySegment.put(offset + byteIndex, current);
	}

	/**
	 * Returns true if the bit is set in the specified index.
	 *
	 * @param index - position
	 * @return - value at the bit position
	 */
	public boolean get(int index) {
		Preconditions.checkArgument(index < bitLength && index >= 0);
		
		int byteIndex = (index & BYTE_POSITION_MASK) >>> 3;
		byte current = memorySegment.get(offset + byteIndex);
		return (current & (1 << (index & BYTE_INDEX_MASK))) != 0;
	}

	/**
	 * Number of bits
	 */
	public int bitSize() {
		return bitLength;
	}

	/**
	 * Clear the bit set.
	 */
	public void clear() {
		for (int i = 0; i < byteLength; i++) {
			memorySegment.put(offset + i, (byte) 0);
		}
	}

	@Override
	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append("BitSet:\n");
		output.append("\tMemorySegment:").append(memorySegment.size()).append("\n");
		output.append("\tOffset:").append(offset).append("\n");
		output.append("\tLength:").append(byteLength).append("\n");
		return output.toString();
	}
}
