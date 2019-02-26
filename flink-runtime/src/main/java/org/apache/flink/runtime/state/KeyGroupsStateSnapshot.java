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

package org.apache.flink.runtime.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A default implementation of {@link KeyedStateHandle} which contains the
 * complete data in the state groups.
 */
public final class KeyGroupsStateSnapshot implements StreamStateHandle, KeyedStateHandle {

	private static final long serialVersionUID = 1L;

	/**
	 * The key-group range covered by this state handle.
	 */
	private final KeyGroupRange keyGroupRange;

	/**
	 * The offsets and the number of entries of the groups in the snapshot.
	 */
	private final Map<Integer, Tuple2<Long, Integer>> metaInfos;

	/**
	 * The datum of the snapshot.
	 */
	@Nullable
	private final StreamStateHandle snapshotHandle;

	/**
	 * Constructor with the groups of the states, and the meta info of these
	 * groups in this snapshot.
	 *
	 * @param keyGroupRange The key groups in the snapshot.
	 * @param metaInfos The offsets and the number of entries of the
	 *                        groups in the snapshot.
	 * @param snapshotHandle The data of the snapshot.
	 */
	public KeyGroupsStateSnapshot(
		final KeyGroupRange keyGroupRange,
		final Map<Integer, Tuple2<Long, Integer>> metaInfos,
		final StreamStateHandle snapshotHandle
	) {
		Preconditions.checkNotNull(keyGroupRange);
		Preconditions.checkNotNull(metaInfos);
		Preconditions.checkNotNull(snapshotHandle);

		this.keyGroupRange = keyGroupRange;
		this.metaInfos = metaInfos;
		this.snapshotHandle = snapshotHandle;
	}

	/**
	 * Constructor for empty state groups.
	 */
	public KeyGroupsStateSnapshot(
		final KeyGroupRange keyGroupRange
	) {
		Preconditions.checkNotNull(keyGroupRange);

		this.keyGroupRange = keyGroupRange;
		this.metaInfos = Collections.emptyMap();
		this.snapshotHandle = null;
	}

	/**
	 * Returns the meta info of the groups(global&local).
	 *
	 * @return The meta info of the groups(global&local).
	 */
	public Map<Integer, Tuple2<Long, Integer>> getMetaInfos() {
		return metaInfos;
	}

	/**
	 * Returns the data of the snapshot.
	 *
	 * @return The data of the snapshot.
	 */
	public StreamStateHandle getSnapshotHandle() {
		return snapshotHandle;
	}

	@Override
	public KeyGroupRange getKeyGroupRange() {
		return keyGroupRange;
	}

	@Override
	public KeyedStateHandle getIntersection(KeyGroupRange otherKeyGroupRange) {
		Preconditions.checkNotNull(otherKeyGroupRange);

		KeyGroupRange intersectGroups = keyGroupRange.getIntersection(otherKeyGroupRange);

		if (snapshotHandle == null) {
			return new KeyGroupsStateSnapshot(intersectGroups);
		}

		Map<Integer, Tuple2<Long, Integer>> intersectMetaInfos = new HashMap<>();
		for (int group : intersectGroups) {
			Tuple2<Long, Integer> metaInfo = metaInfos.get(group);
			if (metaInfo != null) {
				intersectMetaInfos.put(group, metaInfo);
			}
		}

		return new KeyGroupsStateSnapshot(
			intersectGroups,
			intersectMetaInfos,
			snapshotHandle
		);
	}

	@Override
	public void discardState() throws Exception {
		if (snapshotHandle != null) {
			snapshotHandle.discardState();
		}
	}

	@Override
	public long getStateSize() {
		return snapshotHandle == null ? 0 : snapshotHandle.getStateSize();
	}

	@Override
	public void registerSharedStates(SharedStateRegistry stateRegistry) {
		// No shared states
	}

	/**
	 *
	 * @param keyGroupId the id of a key-group. the id must be contained in the range of this handle.
	 * @return offset to the position of data for the provided key-group in the stream referenced by this state handle
	 */
	public long getOffsetForKeyGroup(int keyGroupId) {
		if (!metaInfos.containsKey(keyGroupId)) {
			System.out.println();
		}
		return metaInfos.get(keyGroupId).f0;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		KeyGroupsStateSnapshot that = (KeyGroupsStateSnapshot) o;

		return Objects.equals(keyGroupRange, that.keyGroupRange) &&
			Objects.equals(metaInfos, that.metaInfos) &&
			Objects.equals(snapshotHandle, that.snapshotHandle);
	}

	@Override
	public int hashCode() {
		int result = Objects.hashCode(keyGroupRange);
		result = 31 * result + Objects.hashCode(metaInfos);
		result = 31 * result + Objects.hashCode(snapshotHandle);
		return result;
	}

	@Override
	public String toString() {
		return "KeyGroupsStateSnapshot{" +
			"keyGroupRange=" + keyGroupRange +
			", metaInfos=" + metaInfos +
			", snapshotHandle=" + snapshotHandle +
			"}";
	}

	@Override
	public FSDataInputStream openInputStream() throws IOException {
		Preconditions.checkNotNull(snapshotHandle, "snapshotHandle is null");
		return snapshotHandle.openInputStream();
	}
}
