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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.core.memory.MemorySegment;

/**
 * Comparator used for skip list key.
 */
class SkipListKeyComparator {

	/**
	 * Compares for order. Returns a negative integer, zero, or a positive integer
	 * as the first node is less than, equal to, or greater than the second.
	 *
	 * @param left        left skip list key's ByteBuffer
	 * @param leftOffset  left skip list key's ByteBuffer's offset
	 * @param right       right skip list key's ByteBuffer
	 * @param rightOffset right skip list key's ByteBuffer's offset
	 * @return An integer result of the comparison.
	 */
	static int compareTo(MemorySegment left, int leftOffset, MemorySegment right, int rightOffset) {
		// compare namespace
		int leftNamespaceLen = left.getInt(leftOffset);
		int rightNamespaceLen = right.getInt(rightOffset);

		int c = left.compare(right, leftOffset + Integer.BYTES, rightOffset + Integer.BYTES,
			leftNamespaceLen, rightNamespaceLen);

		if (c != 0) {
			return c;
		}

		// compare key
		int leftKeyOffset = leftOffset + Integer.BYTES + leftNamespaceLen;
		int rightKeyOffset = rightOffset + Integer.BYTES + rightNamespaceLen;
		int leftKeyLen = left.getInt(leftKeyOffset);
		int rightKeyLen = right.getInt(rightKeyOffset);

		return left.compare(right, leftKeyOffset + Integer.BYTES, rightKeyOffset + Integer.BYTES,
			leftKeyLen, rightKeyLen);
	}

	/**
	 * Compares the namespace in the memory segment with the namespace in the node .
	 * Returns a negative integer, zero, or a positive integer as the first node is
	 * less than, equal to, or greater than the second.
	 *
	 * @param namespaceSegment    memory segment to store the namespace.
	 * @param namespaceOffset     offset of namespace in the memory segment.
	 * @param namespaceLen        length of namespace.
	 * @param nodeSegment         memory segment to store the node key.
	 * @param nodeKeyOffset       offset of node key in the memory segment.
	 * @return An integer result of the comparison.
	 */
	static int compareNamespaceAndNode(
		MemorySegment namespaceSegment, int namespaceOffset, int namespaceLen,
		MemorySegment nodeSegment, int nodeKeyOffset) {

		int nodeNamespaceLen = nodeSegment.getInt(nodeKeyOffset);
		return namespaceSegment.compare(nodeSegment, namespaceOffset, nodeKeyOffset + Integer.BYTES,
			namespaceLen, nodeNamespaceLen);
	}
}
