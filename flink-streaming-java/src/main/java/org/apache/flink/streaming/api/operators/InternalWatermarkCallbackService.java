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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupsList;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The watermark callback service allows to register a {@link OnWatermarkCallback OnWatermarkCallback}
 * and multiple keys, for which the callback will be invoked every time a new {@link Watermark} is received
 * (after the registration of the key).
 * <p>
 * <b>NOTE: </b> This service is only available to <b>keyed</b> operators.
 *
 *  @param <K> The type of key returned by the {@code KeySelector}.
 */
@Internal
public class InternalWatermarkCallbackService<K> {

	////////////			Information about the keyed state				//////////

	private final KeyGroupsList localKeyGroupRange;
	private final int totalKeyGroups;
	private final int localKeyGroupRangeStartIdx;

	private final KeyContext keyContext;

	/**
	 * An array of sets of keys keeping the registered keys split
	 * by the key-group they belong to. Each key-group has one set.
	 */
	private final Set<K>[] keysByKeygroup;

	/** A serializer for the registered keys. */
	private TypeSerializer<K> keySerializer;

	/**
	 * The {@link OnWatermarkCallback} to be invoked for each
	 * registered key upon reception of the watermark.
	 */
	private OnWatermarkCallback<K> callback;

	public InternalWatermarkCallbackService(int totalKeyGroups, KeyGroupsList localKeyGroupRange, KeyContext keyContext) {

		this.totalKeyGroups = totalKeyGroups;
		this.localKeyGroupRange = checkNotNull(localKeyGroupRange);
		this.keyContext = checkNotNull(keyContext);

		// find the starting index of the local key-group range
		int startIdx = Integer.MAX_VALUE;
		for (Integer keyGroupIdx : localKeyGroupRange) {
			startIdx = Math.min(keyGroupIdx, startIdx);
		}
		this.localKeyGroupRangeStartIdx = startIdx;

		// the list of ids of the key-groups this task is responsible for
		int localKeyGroups = this.localKeyGroupRange.getNumberOfKeyGroups();
		this.keysByKeygroup = new Set[localKeyGroups];
	}

	/**
	 * Registers a {@link OnWatermarkCallback} with the current {@link InternalWatermarkCallbackService} service.
	 * Before this method is called and the callback is set, the service is unusable.
	 *
	 * @param watermarkCallback The callback to be registered.
	 * @param keySerializer A serializer for the registered keys.
	 */
	public void setWatermarkCallback(OnWatermarkCallback<K> watermarkCallback, TypeSerializer<K> keySerializer) {
		if (callback == null) {
			this.keySerializer = keySerializer;
			this.callback = watermarkCallback;
		} else {
			throw new RuntimeException("The watermark callback has already been initialized.");
		}
	}

	/**
	 * Registers a key with the service. This will lead to the {@link OnWatermarkCallback}
	 * being invoked for this key upon reception of each subsequent watermark.
	 *
	 * @param key The key to be registered.
	 */
	public boolean registerKeyForWatermarkCallback(K key) {
		return getKeySetForKeyGroup(key).add(key);
	}

	/**
	 * Unregisters the provided key from the service.
	 *
	 * @param key The key to be unregistered.
	 */
	public boolean unregisterKeyFromWatermarkCallback(K key) {
		Set<K> keys = getKeySetForKeyGroup(key);
		boolean res = keys.remove(key);

		if (keys.isEmpty()) {
			removeKeySetForKey(key);
		}
		return res;
	}

	/**
	 * Invokes the registered callback for all the registered keys.
	 *
	 * @param watermark The watermark that triggered the invocation.
	 */
	public void invokeOnWatermarkCallback(Watermark watermark) throws IOException {
		if (callback != null) {
			for (Set<K> keySet : keysByKeygroup) {
				if (keySet != null) {
					for (K key : keySet) {
						keyContext.setCurrentKey(key);
						callback.onWatermark(key, watermark);
					}
				}
			}
		}
	}

	/**
	 * Retrieve the set of keys for the key-group this key belongs to.
	 *
	 * @param key the key whose key-group we are searching.
	 * @return the set of registered keys for the key-group.
	 */
	private Set<K> getKeySetForKeyGroup(K key) {
		checkArgument(localKeyGroupRange != null, "The operator has not been initialized.");
		int keyGroupIdx = KeyGroupRangeAssignment.assignToKeyGroup(key, totalKeyGroups);
		return getKeySetForKeyGroup(keyGroupIdx);
	}

	/**
	 * Retrieve the set of keys for the requested key-group.
	 *
	 * @param keyGroupIdx the index of the key group we are interested in.
	 * @return the set of keys for the key-group.
	 */
	private Set<K> getKeySetForKeyGroup(int keyGroupIdx) {
		int localIdx = getIndexForKeyGroup(keyGroupIdx);
		Set<K> keys = keysByKeygroup[localIdx];
		if (keys == null) {
			keys = new HashSet<>();
			keysByKeygroup[localIdx] = keys;
		}
		return keys;
	}

	private void removeKeySetForKey(K key) {
		checkArgument(localKeyGroupRange != null, "The operator has not been initialized.");
		int keyGroupIdx = KeyGroupRangeAssignment.assignToKeyGroup(key, totalKeyGroups);
		int localKeyGroupIdx = getIndexForKeyGroup(keyGroupIdx);
		keysByKeygroup[localKeyGroupIdx] = null;
	}

	/**
	 * Computes the index of the requested key-group in the local datastructures.
	 * <li/>
	 * Currently we assume that each task is assigned a continuous range of key-groups,
	 * e.g. 1,2,3,4, and not 1,3,5. We leverage this to keep the different states
	 * key-grouped in arrays instead of maps, where the offset for each key-group is
	 * the key-group id (an int) minus the id of the first key-group in the local range.
	 * This is for performance reasons.
	 */
	private int getIndexForKeyGroup(int keyGroupIdx) {
		checkArgument(localKeyGroupRange.contains(keyGroupIdx),
			"Key Group " + keyGroupIdx + " does not belong to the local range.");
		return keyGroupIdx - localKeyGroupRangeStartIdx;
	}

	//////////////////				Fault Tolerance Methods				///////////////////

	public void snapshotKeysForKeyGroup(DataOutputViewStreamWrapper stream, int keyGroupIdx) throws Exception {
		Set<K> keySet = getKeySetForKeyGroup(keyGroupIdx);
		if (keySet != null) {
			stream.writeInt(keySet.size());

			InstantiationUtil.serializeObject(stream, keySerializer);
			for (K key : keySet) {
				keySerializer.serialize(key, stream);
			}
		} else {
			stream.writeInt(0);
		}
	}

	public void restoreKeysForKeyGroup(DataInputViewStreamWrapper stream, int keyGroupIdx,
									ClassLoader userCodeClassLoader) throws IOException, ClassNotFoundException {

		checkArgument(localKeyGroupRange.contains(keyGroupIdx),
			"Key Group " + keyGroupIdx + " does not belong to the local range.");

		int numKeys = stream.readInt();
		if (numKeys > 0) {

			TypeSerializer<K> tmpKeyDeserializer = InstantiationUtil.deserializeObject(stream, userCodeClassLoader);

			if (keySerializer != null && !keySerializer.equals(tmpKeyDeserializer)) {
				throw new IllegalArgumentException("Tried to restore timers " +
					"for the same service with different serializers.");
			}

			this.keySerializer = tmpKeyDeserializer;

			Set<K> keys = getKeySetForKeyGroup(keyGroupIdx);
			for (int i = 0; i < numKeys; i++) {
				keys.add(keySerializer.deserialize(stream));
			}
		}
	}
}
