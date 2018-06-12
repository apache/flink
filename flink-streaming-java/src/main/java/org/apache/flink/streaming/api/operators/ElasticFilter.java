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

import org.apache.flink.util.FlinkRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * A list of {@link TolerantFilterNode} to avoid data skewed between key ranges. The size of nodes on the list
 * grow by a {@code growRate} to avoid the list to be too long.
 */
public class ElasticFilter {

	private static final Logger LOG = LoggerFactory.getLogger(ElasticFilter.class);

	private long currentSize;

	private long initSize;
	private double growRate;

	private ElasticFilterState partitionedBloomFilter;

	private LinkedList<TolerantFilterNode> bloomFilterNodes = new LinkedList<>();

	public ElasticFilter(ElasticFilterState partitionedBloomFilter, long initSize, double growRate) {
		this.partitionedBloomFilter = partitionedBloomFilter;
		this.currentSize = initSize;
		this.initSize = initSize;
		this.growRate = growRate;
	}

	public void add(byte[] content) {
		synchronized (bloomFilterNodes) {
			TolerantFilterNode node;
			if (bloomFilterNodes.size() > 0) {
				node = bloomFilterNodes.getLast();
				if (node.full()) {
					LOG.info("allocate new node.");
					currentSize = (long) (this.initSize * Math.pow(growRate, bloomFilterNodes.size()));
					node = this.partitionedBloomFilter.allocateBloomFilterNode(currentSize);
					if (node != null) {
						LOG.info("allocate new node successfully.");
						bloomFilterNodes.add(node);
					} else {
//						LOG.warn("allocate new node failed (run out of configured capacity), reuse the last node.");
//						node = bloomFilterNodes.getLast();
//						node.reSetTtl();
						throw new FlinkRuntimeException("memory out.");
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
			Iterator<TolerantFilterNode> iter = bloomFilterNodes.descendingIterator();
			while (iter.hasNext()) {
				TolerantFilterNode node = iter.next();
				if (node.contains(content)) {
					return true;
				}
			}
			return false;
		}
	}

	@VisibleForTesting
	long size() {
		return currentSize;
	}

	@VisibleForTesting
	long initSize() {
		return initSize;
	}

	@VisibleForTesting
	double growRate() {
		return growRate;
	}

	@VisibleForTesting
	LinkedList<TolerantFilterNode> getBloomFilterNodes() {
		return bloomFilterNodes;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		synchronized (bloomFilterNodes) {
			for (TolerantFilterNode node : bloomFilterNodes) {
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
			if (node.deadline() <= ts) {
				partitionedBloomFilter.takeBack(node);
				return true;
			}
			return false;
		});

		outputView.writeInt(bloomFilterNodes.size());
		for (TolerantFilterNode node : bloomFilterNodes) {
			node.snapshot(outputView);
		}
	}

	void restore(DataInputView source) throws IOException {
		currentSize = source.readLong();
		initSize = source.readLong();
		growRate = source.readDouble();
		int len = source.readInt();
		for (int i = 0; i < len; ++i) {
			ShrinkableBloomFilterNode node = new ShrinkableBloomFilterNode();
			node.restore(source);
			bloomFilterNodes.add(node);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ElasticFilter that = (ElasticFilter) o;

		if (currentSize != that.currentSize) return false;
		if (initSize != that.initSize) return false;
		if (Double.compare(that.growRate, growRate) != 0) return false;
		if (partitionedBloomFilter != null ? !partitionedBloomFilter.equals(that.partitionedBloomFilter) : that.partitionedBloomFilter != null)
			return false;
		return bloomFilterNodes != null ? bloomFilterNodes.equals(that.bloomFilterNodes) : that.bloomFilterNodes == null;
	}

	@Override
	public int hashCode() {
		int result;
		long temp;
		result = (int) (currentSize ^ (currentSize >>> 32));
		result = 31 * result + (int) (initSize ^ (initSize >>> 32));
		temp = Double.doubleToLongBits(growRate);
		result = 31 * result + (int) (temp ^ (temp >>> 32));
		result = 31 * result + (partitionedBloomFilter != null ? partitionedBloomFilter.hashCode() : 0);
		result = 31 * result + (bloomFilterNodes != null ? bloomFilterNodes.hashCode() : 0);
		return result;
	}
}
