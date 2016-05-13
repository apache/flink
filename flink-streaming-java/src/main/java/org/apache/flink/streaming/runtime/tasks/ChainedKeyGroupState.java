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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.flink.runtime.state.PartitionedStateSnapshot;
import org.apache.flink.runtime.state.StateHandle;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * The aggregated key group state for one key group index belonging to a chain of operators. The
 * individual key group states are identified by the index of the operator to which they belong.
 *
 * This class is necessary because of the way the chaining works in Flink. A {@link StreamTask} can
 * be instantiated with multiple chained stream operators. Each stream operator has a set of key
 * groups assigned. However, the chaining is transparent to the checkpoint coordinator.
 * Consequently, the key groups for each key group index of the chained operators have to
 * aggregated and are only then sent to the checkpoint coordinator.
 */
public class ChainedKeyGroupState implements StateHandle<Map<Integer, PartitionedStateSnapshot>> {
	private static final long serialVersionUID = -9207708192881175094L;

	/**
	 * Key group states for the different chained operators of the {@link StreamTask}. All key
	 * groups have the same key group index. The stored key group state snapshots must not be
	 * continuous with respect to the chaining index. This means that there can be chaining indices
	 * which have no key group state associated.
	 */
	private final Map<Integer, PartitionedStateSnapshot> keyGroupStates;

	public ChainedKeyGroupState(int maxChainedStates) {
		this.keyGroupStates = new HashMap<>(maxChainedStates);
	}

	public void put(int chainIndex, PartitionedStateSnapshot partitionedStateSnapshot) {
		keyGroupStates.put(chainIndex, partitionedStateSnapshot);
	}

	@Override
	public Map<Integer, PartitionedStateSnapshot> getState(ClassLoader userCodeClassLoader) {
		return keyGroupStates;
	}

	@Override
	public void discardState() throws Exception {

		while (!keyGroupStates.isEmpty()) {
			try {
				Iterator<PartitionedStateSnapshot> iterator = keyGroupStates.values().iterator();

				while (iterator.hasNext()) {
					PartitionedStateSnapshot partitionedStateSnapshot = iterator.next();
					partitionedStateSnapshot.discardState();
					iterator.remove();
				}
			} catch (ConcurrentException e) {
				// fall through the loop
			}
		}
		for (PartitionedStateSnapshot partitionedStateSnapshot : keyGroupStates.values()) {
			partitionedStateSnapshot.discardState();
		}
	}

	@Override
	public long getStateSize() throws Exception {
		int stateSize = 0;

		for (PartitionedStateSnapshot partitionedStateSnapshot : keyGroupStates.values()) {
			stateSize += partitionedStateSnapshot.getStateSize();
		}

		return stateSize;
	}
}
