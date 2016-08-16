/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.KeyGroupAssigner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

/**
 * Partitioner selects the target channel based on the key group index. The key group
 * index is derived from the key of the elements using the {@link KeyGroupAssigner}.
 *
 * @param <T> Type of the elements in the Stream being partitioned
 */
@Internal
public class KeyGroupStreamPartitioner<T, K> extends StreamPartitioner<T> implements ConfigurableStreamPartitioner {
	private static final long serialVersionUID = 1L;

	private final int[] returnArray = new int[1];

	private final KeySelector<T, K> keySelector;

	private final KeyGroupAssigner<K> keyGroupAssigner;

	public KeyGroupStreamPartitioner(KeySelector<T, K> keySelector, KeyGroupAssigner<K> keyGroupAssigner) {
		this.keySelector = Preconditions.checkNotNull(keySelector);
		this.keyGroupAssigner = Preconditions.checkNotNull(keyGroupAssigner);
	}

	public KeyGroupAssigner<K> getKeyGroupAssigner() {
		return keyGroupAssigner;
	}

	@Override
	public int[] selectChannels(
		SerializationDelegate<StreamRecord<T>> record,
		int numberOfOutputChannels) {

		K key;
		try {
			key = keySelector.getKey(record.getInstance().getValue());
		} catch (Exception e) {
			throw new RuntimeException("Could not extract key from " + record.getInstance().getValue(), e);
		}
		returnArray[0] = KeyGroupRange.computeOperatorIndexForKeyGroup(
				keyGroupAssigner.getNumberKeyGroups(),
				numberOfOutputChannels,
				keyGroupAssigner.getKeyGroupIndex(key));
		return returnArray;
	}

	@Override
	public StreamPartitioner<T> copy() {
		return this;
	}

	@Override
	public String toString() {
		return "HASH";
	}

	@Override
	public void configure(int maxParallelism) {
		keyGroupAssigner.setup(maxParallelism);
	}
}
