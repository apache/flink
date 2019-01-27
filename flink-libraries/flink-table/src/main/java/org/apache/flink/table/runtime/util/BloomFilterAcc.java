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

package org.apache.flink.table.runtime.util;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.util.SerializedValue;

import java.util.Arrays;

/**
 * Acc to merge {@link BloomFilter}.
 */
public class BloomFilterAcc implements Accumulator<SerializedValue, SerializedValue> {

	private byte[] bytes;

	@Override
	public void add(SerializedValue value) {
		if (bytes == null) {
			bytes = value.getByteArray();
		} else {
			BloomFilter.mergeBloomFilterBytes(bytes, value.getByteArray());
		}
	}

	@Override
	public SerializedValue getLocalValue() {
		if (bytes == null) {
			return null;
		} else {
			return SerializedValue.fromBytes(bytes);
		}
	}

	@Override
	public void resetLocal() {
		bytes = null;
	}

	@Override
	public void merge(Accumulator<SerializedValue, SerializedValue> other) {
		BloomFilter.mergeBloomFilterBytes(bytes, ((BloomFilterAcc) other).bytes);
	}

	@Override
	public Accumulator<SerializedValue, SerializedValue> clone() {
		BloomFilterAcc clone = new BloomFilterAcc();
		clone.bytes = Arrays.copyOf(bytes, bytes.length);
		return clone;
	}

	public static BloomFilterAcc fromBytes(byte[] bytes) {
		BloomFilterAcc acc = new BloomFilterAcc();
		acc.add(SerializedValue.fromBytes(bytes));
		return acc;
	}
}
