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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.util.BloomFilter;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupsList;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Bloom filter which supports
 * - ability to handle data skew.
 * - rescaling.
 * - fail tolerant.
 * - relax ttl.
 *
 * @param <K> The type of keys.
 * @param <V> The type of values.
 */
public class ElasticBloomFilter<K, V> {

	private static final Logger LOG = LoggerFactory.getLogger(ElasticBloomFilter.class);

	private final int totalKeyGroups;
	private final KeyGroupsList localKeyGroupRange;
	private final KeyContext keyContext;
	private LinkedShrinkableBloomFilter[] linkedBloomFilters;
	private final int localKeyGroupRangeStartIdx;

	private long totalMemSize;
	private long restMemSize;

	private long miniExpectNum;
	private long maxExpectNum;
	private double growRate;

	private long capacity;
	private double fpp;
	private long ttl;

	/**
	 * Serializer for the value.
	 */
	private final TypeSerializer keySerializer;
	private final TypeSerializer<V> valueSerializer;
	private final ByteArrayOutputStreamWithPos valueSerializationStream;
	private final DataOutputView valueSerializationDataOutputView;

	public ElasticBloomFilter(TypeSerializer<K> keySerializer,
							  TypeSerializer<V> valueSerializer,
							  int totalKeyGroups,
							  KeyGroupsList localKeyGroupRange,
							  KeyContext keyContext,
							  long capacity,
							  double fpp,
							  long ttl,
							  long miniExpectNum,
							  long maxExpectNum,
							  double growRate) {
		this.capacity = capacity;
		this.fpp = fpp;
		this.ttl = ttl;
		this.totalMemSize = BloomFilter.optimalNumOfBits(capacity, fpp);
		this.restMemSize = this.totalMemSize;

		this.miniExpectNum = miniExpectNum;
		this.maxExpectNum = maxExpectNum;
		this.growRate = growRate;

		this.keyContext = keyContext;
		this.totalKeyGroups = totalKeyGroups;
		this.localKeyGroupRange = localKeyGroupRange;
		this.linkedBloomFilters = new LinkedShrinkableBloomFilter[localKeyGroupRange.getNumberOfKeyGroups()];

		// find the starting index of the local key-group range
		int startIdx = Integer.MAX_VALUE;
		for (Integer keyGroupIdx : localKeyGroupRange) {
			startIdx = Math.min(keyGroupIdx, startIdx);
		}
		this.localKeyGroupRangeStartIdx = startIdx;

		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.valueSerializationStream = new ByteArrayOutputStreamWithPos(128);
		this.valueSerializationDataOutputView = new DataOutputViewStreamWrapper(valueSerializationStream);
	}

	public void add(V content) {
		int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(keyContext.getCurrentKey(), totalKeyGroups);
		int index = getIndexForKeyGroup(keyGroupIndex);

		LinkedShrinkableBloomFilter bloomFilter = linkedBloomFilters[index];
		if (bloomFilter == null) {
			bloomFilter = new LinkedShrinkableBloomFilter(this, miniExpectNum, growRate);
			linkedBloomFilters[index] = bloomFilter;
		}
		bloomFilter.add(buildBloomFilterKey(content));
	}

	public boolean contains(V content) {
		int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(keyContext.getCurrentKey(), totalKeyGroups);
		int index = getIndexForKeyGroup(keyGroupIndex);

		LinkedShrinkableBloomFilter bloomFilter = linkedBloomFilters[index];
		if (bloomFilter == null) {
			return false;
		}
		return bloomFilter.contains(buildBloomFilterKey(content));
	}

	private int getIndexForKeyGroup(int keyGroupIdx) {
		checkArgument(localKeyGroupRange.contains(keyGroupIdx),
			"Key Group " + keyGroupIdx + " does not belong to the local range.");
		return keyGroupIdx - this.localKeyGroupRangeStartIdx;
	}

	public void snapshotStateForKeyGroup(DataOutputViewStreamWrapper stream, int keyGroupIdx) throws IOException {

		LOG.info("snapshot state for group {} ", keyGroupIdx);
		int index = getIndexForKeyGroup(keyGroupIdx);
		LinkedShrinkableBloomFilter bloomFilter = this.linkedBloomFilters[index];
		if (bloomFilter != null) {
			stream.writeBoolean(true);
			stream.writeLong(this.restMemSize);
			bloomFilter.snapshot(stream);
		} else {
			stream.writeBoolean(false);
		}
	}

	public void restoreStateForKeyGroup(
		DataInputViewStreamWrapper stream,
		int keyGroupIdx) throws IOException, ClassNotFoundException {

		LOG.info("restore state for group {} ", keyGroupIdx);
		int index = getIndexForKeyGroup(keyGroupIdx);
		if (stream.readBoolean()) {
			this.restMemSize = stream.readLong();
			LinkedShrinkableBloomFilter linkedBloomFilter = new LinkedShrinkableBloomFilter(this, miniExpectNum, growRate);
			linkedBloomFilter.restore(stream);
			this.linkedBloomFilters[index] = linkedBloomFilter;
			LOG.info("group {} restored.", keyGroupIdx);
		} else {
			LOG.info("nothing to restore.");
		}
	}

	// ---------------------

	ShrinkableBloomFilterNode allocateBloomFilterNode(long expectNum) {
		return allocateBloomFilterNode(expectNum, false);
	}

	ShrinkableBloomFilterNode allocateBloomFilterNode(long expectNum, boolean force) {
		long requestNum = expectNum;
		if (!force) {
			if (restMemSize > 0) {
				requestNum = estimatePropExpectNum(expectNum, fpp);
			} else {
				return null;
			}
		}

		if (requestNum < miniExpectNum) {
			requestNum = miniExpectNum;
		}

		if (requestNum > maxExpectNum) {
			requestNum = maxExpectNum;
		}

		restMemSize -= BloomFilter.optimalNumOfBits(requestNum, fpp);
		return new ShrinkableBloomFilterNode((int) requestNum, fpp, ttl);
	}

	void takeBack(ShrinkableBloomFilterNode node) {
		restMemSize += BloomFilter.optimalNumOfBits(node.getCapacity(), node.getFpp());
	}

	long estimatePropExpectNum(long expectNum, double fpp) {
		if (restMemSize <= 0) {
			expectNum = miniExpectNum;
		} else {
			while (BloomFilter.optimalNumOfBits(expectNum, fpp) > restMemSize) {
				expectNum >>= 1;
			}
		}
		return expectNum;
	}

	byte[] buildBloomFilterKey(V record) {
		try {
			valueSerializationStream.reset();
			keySerializer.serialize(keyContext.getCurrentKey(), valueSerializationDataOutputView);
			valueSerializer.serialize(record, valueSerializationDataOutputView);
			return valueSerializationStream.toByteArray();
		} catch (IOException e) {
			LOG.error("build bloom filter key failed {}", e);
			throw new FlinkRuntimeException(e);
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder()
			.append("\n------------------------------------------------>\n")
			.append("total memory:").append(totalMemSize).append("\t").append("rest memory:").append(restMemSize).append("\n");

		for (int i = 0; i < localKeyGroupRange.getNumberOfKeyGroups(); ++i) {
			LinkedShrinkableBloomFilter bloomFilter = this.linkedBloomFilters[i];
			if (bloomFilter != null) {
				builder.append("group ").append(i + localKeyGroupRangeStartIdx).append(":").append(bloomFilter.toString()).append("\n");
			}
		}
		return builder.toString();
	}

	@VisibleForTesting
	LinkedShrinkableBloomFilter[] getLinkedBloomFilters() {
		return this.linkedBloomFilters;
	}
}
