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

package org.apache.flink.table.util;

import org.apache.flink.table.runtime.util.BloomFilter;

import java.util.Random;

/**
 * Test for {@link BloomFilter}.
 */
public class BloomFilterTest {

	public static void main(String[] args) {
		long maxNumEntries = 15_000_000L;
		long numEntries = 1000000L;
		int filterNum = 50;

		BloomFilter filter = new BloomFilter(maxNumEntries);
		filter.addHash(111);

		Random random = new Random();
		BloomFilter[] filters = new BloomFilter[filterNum];
		for (int i = 0; i < filters.length; i++) {
			filters[i] = new BloomFilter(maxNumEntries);
			for (int j = 0; j < numEntries; j++) {
				filters[i].addHash(random.nextInt());
			}
		}

		long l1 = System.currentTimeMillis();

		for (BloomFilter filter1 : filters) {
			filter.merge(filter1);
		}

		long l2 = System.currentTimeMillis();

		byte[] serialize = BloomFilter.toBytes(filter);
		System.out.println("BloomFilter bytes length: " + serialize.length);
		System.out.println("BloomFilter merge use time: " + (l2 - l1));

		byte[][] filterBytes = new byte[filterNum][];
		for (int i = 0; i < filters.length; i++) {
			filterBytes[i] = BloomFilter.toBytes(filters[i]);
		}

		long l3 = System.currentTimeMillis();

		System.out.println("BloomFilter toBytes use time: " + (l3 - l2));

		filters = null; // let it gc...

		byte[] filterByte = filterBytes[0];
		for (int i = 1; i < filterNum; i++) {
			BloomFilter.mergeBloomFilterBytes(
					filterByte, 0, filterByte.length, filterBytes[i], 0, filterBytes[i].length);
		}

		long l4 = System.currentTimeMillis();

		System.out.println("BloomFilter mergeBloomFilterBytes use time: " + (l4 - l3));

		BloomFilter[] fromBytesBf = new BloomFilter[filterNum];
		for (int i = 0; i < filterNum; i++) {
			fromBytesBf[i] = BloomFilter.fromBytes(filterBytes[i]);
		}

		long l5 = System.currentTimeMillis();

		int totalSize = 0;
		for (BloomFilter bf : fromBytesBf) {
			totalSize += bf.getBitSet().length * 8;
		}
		System.out.println(totalSize);
		System.out.println("BloomFilter fromBytes use time: " + (l5 - l4));
	}
}
