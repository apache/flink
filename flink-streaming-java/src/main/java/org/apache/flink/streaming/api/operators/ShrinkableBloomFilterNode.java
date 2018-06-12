/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * A shrinkable bloom filter node linked each other in {@link ElasticFilter} to avoid data skewed.
 */
public class ShrinkableBloomFilterNode implements TolerantFilterNode {

	public static final int DEFAULT_UNITS_NUM = 8;

	private int capacity;
	private double fpp;
	private long ttl;
	private long deleteTS = Long.MAX_VALUE;

	public BloomFilterUnit[] bloomFilterUnits;

	ShrinkableBloomFilterNode() {
	}

	public ShrinkableBloomFilterNode(int capacity, double fpp, long ttl) {
		this.capacity = capacity;
		this.fpp = fpp;
		this.ttl = ttl;

		bloomFilterUnits = new BloomFilterUnit[DEFAULT_UNITS_NUM];

		final int unitCapacity = computeUnitCapacity(capacity, bloomFilterUnits.length);

		for (int i = 0; i < bloomFilterUnits.length; ++i) {
			bloomFilterUnits[i]	 = new BloomFilterUnit(unitCapacity, fpp);
		}
	}

	@Override
	public boolean full() {

		for (BloomFilterUnit bloomFilterUnit : bloomFilterUnits) {
			if (bloomFilterUnit.full()) {
				deleteTS = System.currentTimeMillis() + ttl;
				return true;
			}
		}

		return false;
	}

	@Override
	public long capacity() {
		int capacity = 0;
		for (BloomFilterUnit bloomFilterUnit : bloomFilterUnits) {
			capacity += bloomFilterUnit.capacity();
		}
		return capacity;
	}

	@Override
	public long size() {
		int size = 0;
		for (BloomFilterUnit bloomFilterUnit : bloomFilterUnits) {
			size += bloomFilterUnit.size();
		}
		return size;
	}

	@Override
	public long ttl() {
		return ttl;
	}

	@Override
	public double fpp() {
		return fpp;
	}

	public void reSetTtl() {
		this.deleteTS = Long.MAX_VALUE;
	}

	@Override
	public long deadline() {
		return deleteTS;
	}

	@Override
	public void add(byte[] content) {

		int unitIndex = Math.abs(Arrays.hashCode(content)) % bloomFilterUnits.length;

		BloomFilterUnit bloomFilterUnit = bloomFilterUnits[unitIndex];

		bloomFilterUnit.add(content);
	}

	@Override
	public boolean contains(byte[] content) {

		int unitIndex = Math.abs(Arrays.hashCode(content)) % bloomFilterUnits.length;

		BloomFilterUnit bloomFilterUnit = bloomFilterUnits[unitIndex];

		return bloomFilterUnit.contains(content);
	}

	/**
	 * Shrinks the BF set to reduce memory consumed.
	 */
	public void shrink() {
		if (shrinkable()) {
			int mergeIndex1 = 0;
			int mergeIndex2 = bloomFilterUnits.length >>> 1;
			final int newSize = mergeIndex2;
			final BloomFilterUnit[] newBloomFilterUnits = new BloomFilterUnit[newSize];

			int index = 0;
			while(index < newSize) {
				BloomFilterUnit leftUnit = bloomFilterUnits[mergeIndex1];
				BloomFilterUnit rightUnit = bloomFilterUnits[mergeIndex2];

				// merging
				leftUnit.merge(rightUnit);

				newBloomFilterUnits[index] = leftUnit;

				++mergeIndex1;
				++mergeIndex2;
				++index;
			}

			bloomFilterUnits = newBloomFilterUnits;
		}
	}

	/**
	 * Check whether this Node is shrinkable,
	 */
	@VisibleForTesting
	boolean shrinkable() {

		// we can't shrink it when it has only one unit.
		if (bloomFilterUnits.length <= 1) {
			return false;
		}

		for (BloomFilterUnit bloomFilterUnit : bloomFilterUnits) {
			int capacity = bloomFilterUnit.capacity();
			int size = bloomFilterUnit.size();

			if (size > (capacity >>> 1)) {
				return false;
			}
		}
		return true;
	}

	private int computeUnitCapacity(int capacity, int units) {
		return Math.floorDiv(capacity, units);
	}

	@Override
	public String toString() {
		return String.format("{c:%d s:%d}", capacity, size());
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ShrinkableBloomFilterNode) {
			ShrinkableBloomFilterNode other = (ShrinkableBloomFilterNode) obj;
			if (other.capacity == this.capacity
				&& other.ttl == this.ttl
				&& other.fpp == this.fpp
				&& other.bloomFilterUnits.length == bloomFilterUnits.length) {

				for (int i = 0; i < bloomFilterUnits.length; ++i) {
					if(!Objects.equals(bloomFilterUnits[i], other.bloomFilterUnits[i])) {
						return false;
					}
				}
				return true;
			}
		}
		return false;
	}

	@Override
	public void snapshot(DataOutputView outputView) throws IOException {
		outputView.writeInt(capacity);
		outputView.writeLong(ttl);
		if (deleteTS == Long.MAX_VALUE) {
			outputView.writeLong(ttl); //rest ttl
		} else {
			outputView.writeLong(deleteTS - System.currentTimeMillis()); //rest ttl
		}
		outputView.writeDouble(fpp);

		int unitNum = bloomFilterUnits.length;
		outputView.writeInt(unitNum);

		for (int i = 0; i < unitNum; ++i) {
			BloomFilterUnit bloomFilterUnit = bloomFilterUnits[i];
			bloomFilterUnit.snapshot(outputView);
		}
	}

	@Override
	public void restore(DataInputView source) throws IOException {
		capacity = source.readInt();
		ttl = source.readLong();
		final long restTtl = source.readLong();
		if (restTtl != ttl) {
			deleteTS = System.currentTimeMillis() + restTtl;
		} else {
			deleteTS = Long.MAX_VALUE;
		}
		fpp = source.readDouble();

		int unitNum = source.readInt();
		bloomFilterUnits = new BloomFilterUnit[unitNum];

		for (int i = 0; i < unitNum; ++i) {
			BloomFilterUnit bloomFilterUnit = new BloomFilterUnit(capacity, fpp);
			bloomFilterUnit.restore(source);
			bloomFilterUnits[i] = bloomFilterUnit;
		}
	}

	/**
	 * The bloom filter unit to store records.
	 */
	static class BloomFilterUnit {

		private BloomFilter<byte[]> bloomFilter;

		private int capacity;

		private int size;

		BloomFilterUnit() {

		}

		public BloomFilterUnit(int capacity, double fpp) {

			bloomFilter = BloomFilter.create(
				Funnels.byteArrayFunnel(),
				capacity,
				fpp);

			this.capacity = capacity;
			size = 0;
		}

		public void add(byte[] content) {
			if (bloomFilter.put(content)) {
				++size;
			}
		}

		public boolean contains(byte[] content) {
			return bloomFilter.mightContain(content);
		}

		public void merge(BloomFilterUnit bloomFilterUnit) {
			bloomFilter.putAll(bloomFilterUnit.bloomFilter);
			size += bloomFilterUnit.size;
		}

		public boolean full() {
			return size >= capacity;
		}

		public int capacity() {
			return capacity;
		}

		public int size() {
			return size;
		}

		public void snapshot(DataOutputView outputView) throws IOException {
			outputView.writeInt(capacity);
			outputView.writeInt(size);

			try(ByteArrayOutputStream out = new ByteArrayOutputStream()) {
				bloomFilter.writeTo(out);
				byte[] bytes = out.toByteArray();
				outputView.writeInt(bytes.length);
				outputView.write(bytes);
			}
		}

		public void restore(DataInputView source) throws IOException {
			this.capacity = source.readInt();
			this.size = source.readInt();

			int byteLen = source.readInt();
			byte[] bytes = new byte[byteLen];
			source.read(bytes, 0, byteLen);
			try(ByteArrayInputStream input = new ByteArrayInputStream(bytes)) {
				bloomFilter = BloomFilter.readFrom(input, Funnels.byteArrayFunnel());
			}
		}

		@Override
		public boolean equals(Object obj) {

			if (obj == this) {
				return true;
			}

			if (obj == null) {
				return false;
			}

			if (!(obj instanceof BloomFilterUnit)) {
				return false;
			}

			BloomFilterUnit other = (BloomFilterUnit) obj;

			return this.size == other.size && this.capacity == other.capacity && this.bloomFilter.equals(other.bloomFilter);
		}
	}
}
