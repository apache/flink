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

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * A bloom filter node linked each other in {@link LinkedBloomFilter} to avoid data skewed.
 */
public class LinkedBloomFilterNode {

	private static final Logger LOG = LoggerFactory.getLogger(LinkedBloomFilterNode.class);

	private long capacity;
	private double fpp;
	private long size;
	private long rawTtl;
	private long ttl;
	private long deleteTS = Long.MAX_VALUE;

	private BloomFilter<byte[]> bloomFilter;

	LinkedBloomFilterNode() {

	}

	public LinkedBloomFilterNode(long capacity, double fpp, long ttl) {
		this.capacity = capacity;
		this.fpp = fpp;
		this.rawTtl = ttl;
		this.ttl = ttl;
		this.size = 0;

		LOG.info("create bf capacity:{} fpp:{}", capacity, fpp);
		bloomFilter = BloomFilter.create(
			Funnels.byteArrayFunnel(),
			(int) capacity,
			this.fpp);
	}

	public boolean isFull() {
		return size >= capacity;
	}

	public void add(byte[] content) {
		bloomFilter.put(content);

		this.size++;
		if (size >= capacity) {
			if (deleteTS == Long.MAX_VALUE) {
				long ts = System.currentTimeMillis();
				deleteTS = ts + ttl;
			}
		}
	}

	public boolean contains(byte[] content) {
		return bloomFilter.mightContain(content);
	}

	public long getCapacity() {
		return capacity;
	}

	public double getFpp() {
		return fpp;
	}

	public long getSize() {
		return size;
	}

	public long getTtl() {
		return ttl;
	}

	public void reSetTtl() {
		this.ttl = this.rawTtl;
		this.deleteTS = Long.MAX_VALUE;
	}

	public long getDeleteTS() {
		return deleteTS;
	}

	public BloomFilter getBloomFilter() {
		return bloomFilter;
	}

	public LinkedBloomFilterNode copy() {
		LinkedBloomFilterNode node = new LinkedBloomFilterNode(capacity, fpp, ttl);
		node.size = this.size;
		node.bloomFilter = this.bloomFilter.copy();
		return node;
	}

	@Override
	public String toString() {
		return String.format("{c:%d s:%d}", capacity, size);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof LinkedBloomFilterNode) {
			LinkedBloomFilterNode other = (LinkedBloomFilterNode) obj;
			if (other.capacity == this.capacity
				&& other.size == this.size
				&& other.rawTtl == this.rawTtl
				&& other.fpp == this.fpp
				&& other.bloomFilter.equals(this.bloomFilter)) {
				return true;
			}
			return false;
		}
		return false;
	}

	void snapshot(DataOutputView outputView) throws IOException {
		outputView.writeLong(capacity);
		outputView.writeLong(rawTtl);
		if (deleteTS == Long.MAX_VALUE) {
			outputView.writeLong(ttl); //rest ttl
		} else {
			outputView.writeLong(deleteTS - System.currentTimeMillis()); //rest ttl
		}
		outputView.writeLong(deleteTS);
		outputView.writeLong(size);
		outputView.writeDouble(fpp);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		bloomFilter.writeTo(out);
		byte[] bytes = out.toByteArray();
		outputView.writeInt(bytes.length);
		outputView.write(bytes);
	}

	void restore(DataInputView source) throws IOException {
		capacity = source.readLong();
		rawTtl = source.readLong();
		ttl = source.readLong();
		deleteTS = source.readLong();
		if (rawTtl != ttl) {
			deleteTS = System.currentTimeMillis() + ttl;
		}
		size = source.readLong();
		fpp = source.readDouble();

		int byteLen = source.readInt();
		byte[] bytes = new byte[byteLen];
		source.read(bytes, 0, byteLen);
		ByteArrayInputStream input = new ByteArrayInputStream(bytes);
		bloomFilter = BloomFilter.readFrom(input, Funnels.byteArrayFunnel());
	}
}
