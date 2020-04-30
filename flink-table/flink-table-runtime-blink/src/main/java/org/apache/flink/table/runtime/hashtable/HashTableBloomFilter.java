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

package org.apache.flink.table.runtime.hashtable;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.operators.util.BloomFilter;
import org.apache.flink.util.MathUtils;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Bloom filters for HashTable.
 */
class HashTableBloomFilter {

	private final MemorySegment[] buffers;
	private final int numBuffers;
	private final int numBuffersMask;
	private final BloomFilter filter;
	private final int maxSize;

	private int size = 0;

	HashTableBloomFilter(MemorySegment[] buffers, long numRecords) {
		checkArgument(buffers != null && buffers.length > 0);
		this.buffers = buffers;
		this.numBuffers = buffers.length;
		checkArgument(MathUtils.isPowerOf2(numBuffers));
		this.numBuffersMask = numBuffers - 1;
		int bufferSize = buffers[0].size();
		this.filter = new BloomFilter((int) (numRecords / numBuffers), buffers[0].size());
		filter.setBitsLocation(buffers[0], 0);

		// We assume that a BloomFilter can contain up to 2.44 elements per byte.
		// fpp roughly equal 0.2
		this.maxSize = (int) ((numBuffers * bufferSize) * 2.44);
	}

	private void setLocation(int hash) {
		if (numBuffers > 1) {
			filter.setBitsLocation(buffers[hash & numBuffersMask], 0);
		}
	}

	boolean testHash(int hash) {
		setLocation(hash);
		return filter.testHash(hash);
	}

	/**
	 * @return false if the accuracy of the BloomFilter is not high.
	 */
	boolean addHash(int hash) {
		setLocation(hash);
		filter.addHash(hash);
		size++;
		return size <= maxSize;
	}

	public MemorySegment[] getBuffers() {
		return buffers;
	}

	static int optimalSegmentNumber(long maxNum, int segSize, double fpp) {
		double numBytes = optimalNumOfBits(maxNum, fpp) / 8D;
		return (int) Math.ceil(numBytes / segSize);
	}

	private static int optimalNumOfBits(long maxNumEntries, double fpp) {
		return (int) (-maxNumEntries * Math.log(fpp) / (Math.log(2) * Math.log(2)));
	}
}
