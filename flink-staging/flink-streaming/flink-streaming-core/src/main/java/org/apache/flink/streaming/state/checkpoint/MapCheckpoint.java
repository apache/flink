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

package org.apache.flink.streaming.state.checkpoint;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.flink.streaming.state.MapState;
import org.apache.flink.streaming.state.OperatorState;

public class MapCheckpoint<K, V> extends StateCheckpoint<Map<K, V>> {

	private static final long serialVersionUID = 1L;

	protected Set<K> removedItems;
	protected Map<K, V> updatedItems;
	protected boolean clear;

	@SuppressWarnings("unchecked")
	public MapCheckpoint(OperatorState<Map<K, V>> operatorState) {
		if (operatorState instanceof MapState) {
			MapState<K, V> mapState = (MapState<K, V>) operatorState;

			this.removedItems = mapState.getRemovedItems();
			this.clear = mapState.isCleared();

			this.updatedItems = new HashMap<K, V>();
			for (K key : mapState.getUpdatedItems()) {
				this.updatedItems.put(key, mapState.get(key));
			}
			this.checkpointedState = this.updatedItems;

		} else {
			throw new RuntimeException("MapCheckpoint can only be used with MapState");
		}

	}

	@SuppressWarnings("unchecked")
	@Override
	public StateCheckpoint<Map<K, V>> update(StateCheckpoint<Map<K, V>> nextCheckpoint) {
		MapCheckpoint<K, V> mapCheckpoint = (MapCheckpoint<K, V>) nextCheckpoint;
		if (this.checkpointedState == null) {
			this.checkpointedState = mapCheckpoint.updatedItems;
		} else {
			if (mapCheckpoint.clear) {
				this.checkpointedState.clear();
			}
			for (Object key : mapCheckpoint.removedItems) {
				this.checkpointedState.remove(key);
			}
			this.checkpointedState.putAll(mapCheckpoint.updatedItems);
		}
		return this;
	}
}
