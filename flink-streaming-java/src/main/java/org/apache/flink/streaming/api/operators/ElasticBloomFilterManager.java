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

import org.apache.flink.api.common.state.PartitionedBloomFilterDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Responsible for managing all {@link ElasticBloomFilter}.
 *
 * @param <K> The type of keys.
 */
public class ElasticBloomFilterManager<K> {

	private static final Logger LOG = LoggerFactory.getLogger(ElasticBloomFilterManager.class);

	private Map<String, ElasticBloomFilter> bloomFilterStates = new HashMap<>();
	private Map<String, PartitionedBloomFilterDescriptor> bloomFilterStateDescriptors = new HashMap<>();
	private int numberOfKeyGroups;
	private KeyGroupRange keyGroupRange;
	private final KeyContext keyContext;

	/** Serializer for the key. */
	private final TypeSerializer<K> keySerializer;

	public ElasticBloomFilterManager(
		KeyContext keyContext,
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange) {

		this.keyContext = Preconditions.checkNotNull(keyContext);
		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.numberOfKeyGroups = Preconditions.checkNotNull(numberOfKeyGroups);
		this.keyGroupRange = Preconditions.checkNotNull(keyGroupRange);
	}

	// ----------------------------------------------------
	public <T> ElasticBloomFilter<K, T> getOrCreateBloomFilterState(PartitionedBloomFilterDescriptor<T> stateDescriptor) {
		String stateName = stateDescriptor.getStateName();
		ElasticBloomFilter<K, T> state = bloomFilterStates.get(stateName);
		if (state == null) {
			state = new ElasticBloomFilter(keySerializer,
				stateDescriptor.getSerializer(),
				numberOfKeyGroups,
				keyGroupRange,
				keyContext,
				stateDescriptor.getCapacity(),
				stateDescriptor.getFpp(),
				stateDescriptor.getTtl(),
				stateDescriptor.getMiniExpectNum(),
				stateDescriptor.getMaxExpectNum(),
				stateDescriptor.getGrowRate());
			bloomFilterStates.put(stateName, state);
			bloomFilterStateDescriptors.put(stateName, stateDescriptor);
		}
		return state;
	}

	public void dispose() {
		try {
			bloomFilterStates.clear();
			bloomFilterStateDescriptors.clear();
		} catch (Exception e) {
			LOG.error("cancel registry close failed: {}", e);
			throw new RuntimeException("cancel registry close failed: " + e);
		}
	}

	public void snapshotStateForKeyGroup(DataOutputViewStreamWrapper stream, int keyGroupIdx) {
		try {
			stream.writeInt(this.bloomFilterStates.size());
			for (Map.Entry<String, ElasticBloomFilter> entry : this.bloomFilterStates.entrySet()) {
				PartitionedBloomFilterDescriptor desc = this.bloomFilterStateDescriptors.get(entry.getKey());

				ObjectOutputStream outputStream = new ObjectOutputStream(stream);
				outputStream.writeObject(desc);

				entry.getValue().snapshotStateForKeyGroup(stream, keyGroupIdx);
				if (keyGroupIdx == 0) {
					LOG.info("\n------------------------------------------------>\n"
						+ "Bloom filter state [{}] nodes map:"
						+ "{}", entry.getKey(), entry.getValue().toString());
				}
			}
		} catch (Exception e) {
			throw new RuntimeException("Shapshot bloom filter state failed:" + e);
		}
	}

	public void restoreStateForKeyGroup(
		DataInputViewStreamWrapper stream,
		int keyGroupIdx) throws IOException, ClassNotFoundException {
		try {
			LOG.info("restoring state for key group {}", keyGroupIdx);
			int len = stream.readInt();
			for (int i = 0; i < len; ++i) {
				ObjectInputStream inputStream = new ObjectInputStream(stream);
				PartitionedBloomFilterDescriptor desc = (PartitionedBloomFilterDescriptor) inputStream.readObject();

				ElasticBloomFilter state = bloomFilterStates.get(desc.getStateName());
				LOG.info("restoring state [{}] for key group {}", desc.getStateName(), keyGroupIdx);
				if (state == null) {
					LOG.info("c:{} f:{} t:{} mie:{} mae:{} g:{}", desc.getCapacity(), desc.getFpp(), desc.getTtl(), desc.getMiniExpectNum(), desc.getMaxExpectNum(), desc.getGrowRate());
					state = new ElasticBloomFilter(keySerializer,
						desc.getSerializer(),
						numberOfKeyGroups,
						keyGroupRange,
						keyContext,
						desc.getCapacity(),
						desc.getFpp(),
						desc.getTtl(),
						desc.getMiniExpectNum(),
						desc.getMaxExpectNum(),
						desc.getGrowRate());
					bloomFilterStates.put(desc.getStateName(), state);
					bloomFilterStateDescriptors.put(desc.getStateName(), desc);
				}
				state.restoreStateForKeyGroup(stream, keyGroupIdx);
			}
		} catch (Exception e) {
			throw new RuntimeException("Restore bloom filter state failed:" + e);
		}
	}
}
