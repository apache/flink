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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * A list of {@link LinkedBloomFilterNode} to avoid data skewed between key ranges. The size of nodes on the list
 * grow by a {@code growRate} to avoid the list to be too long.
 */
public class LinkedBloomFilter {

	private static final Logger LOG = LoggerFactory.getLogger(LinkedBloomFilter.class);

	private long currentSize;

	private long initSize;
	private double growRate;

	private PartitionedBloomFilter partitionedBloomFilter;

	private LinkedList<LinkedBloomFilterNode> bloomFilterNodes = new LinkedList<>();

	public LinkedBloomFilter(PartitionedBloomFilter partitionedBloomFilter, long initSize, double growRate) {
		this.partitionedBloomFilter = partitionedBloomFilter;
		this.currentSize = initSize;
		this.initSize = initSize;
		this.growRate = growRate;
	}

	public void add(byte[] content) {
		synchronized (bloomFilterNodes) {
			LinkedBloomFilterNode node;
			if (bloomFilterNodes.size() > 0) {
				node = bloomFilterNodes.getLast();
				if (node.isFull()) {
					LOG.info("allocate new node.");
					currentSize = (long) (this.initSize * Math.pow(growRate, bloomFilterNodes.size()));
					node = this.partitionedBloomFilter.allocateBloomFilterNode(currentSize);
					if (node != null) {
						LOG.info("allocate new node successfully.");
						bloomFilterNodes.add(node);
					} else {
						LOG.warn("allocate new node failed (run out of configured capacity), reuse the last node.");
						node = bloomFilterNodes.getLast();
						node.reSetTtl();
					}
				}
			} else {
				LOG.info("init the first node.");
				node = this.partitionedBloomFilter.allocateBloomFilterNode(currentSize, true);
				bloomFilterNodes.add(node);
			}
			node.add(content);
		}
	}

	public boolean contains(byte[] content) {
		synchronized (bloomFilterNodes) {
			Iterator<LinkedBloomFilterNode> iter = bloomFilterNodes.descendingIterator();
			while (iter.hasNext()) {
				LinkedBloomFilterNode node = iter.next();
				if (node.contains(content)) {
					return true;
				}
			}
			return false;
		}
	}

	// for checkpoint and recovery
	public LinkedBloomFilter copy() {
		synchronized (bloomFilterNodes) {
			LinkedBloomFilter bloomFilter = new LinkedBloomFilter(partitionedBloomFilter, initSize, growRate);
			for (LinkedBloomFilterNode node : bloomFilterNodes) {
				bloomFilter.bloomFilterNodes.add(node.copy());
			}
			return bloomFilter;
		}
	}

	@VisibleForTesting
	long getCurrentSize() {
		return currentSize;
	}

	@VisibleForTesting
	long getInitSize() {
		return initSize;
	}

	@VisibleForTesting
	double getGrowRate() {
		return growRate;
	}

	@VisibleForTesting
	LinkedList<LinkedBloomFilterNode> getBloomFilterNodes() {
		return bloomFilterNodes;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		synchronized (bloomFilterNodes) {
			for (LinkedBloomFilterNode node : bloomFilterNodes) {
				if (builder.length() > 0) {
					builder.append(" -> ");
				}
				builder.append(node.toString());
			}
		}
		return builder.toString();
	}

	void snapshot(DataOutputView outputView) throws IOException {
		long ts = System.currentTimeMillis();
		outputView.writeLong(currentSize);
		outputView.writeLong(initSize);
		outputView.writeDouble(growRate);

		bloomFilterNodes.removeIf(node -> {
			if (node.getDeleteTS() <= ts) {
				partitionedBloomFilter.takeBack(node);
				return true;
			}
			return false;
		});

		outputView.writeInt(bloomFilterNodes.size());
		for (LinkedBloomFilterNode node : bloomFilterNodes) {
			node.snapshot(outputView);
		}
	}

	void restore(DataInputView source) throws IOException {
		currentSize = source.readLong();
		initSize = source.readLong();
		growRate = source.readDouble();
		int len = source.readInt();
		for (int i = 0; i < len; ++i) {
			LinkedBloomFilterNode node = new LinkedBloomFilterNode();
			node.restore(source);
			bloomFilterNodes.add(node);
		}
	}
}
