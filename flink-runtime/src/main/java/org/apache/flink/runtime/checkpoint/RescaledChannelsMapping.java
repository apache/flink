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

package org.apache.flink.runtime.checkpoint;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;

/**
 * Contains the fine-grain channel mappings that occur when a connected operator has been rescaled.
 */
public class RescaledChannelsMapping implements Serializable {
	public static final RescaledChannelsMapping NO_CHANNEL_MAPPING = new RescaledChannelsMapping(emptyMap());

	private static final long serialVersionUID = -8719670050630674631L;

	/**
	 * For each new channel (=index), all old channels are set.
	 */
	private final Map<Integer, Set<Integer>> newToOldChannelIndexes;

	/**
	 * For each old channel (=index), all new channels are set. Lazily calculated to keep {@link OperatorSubtaskState}
	 * small in terms of serialization cost.
	 */
	private transient Map<Integer, Set<Integer>> oldToNewChannelIndexes;

	public RescaledChannelsMapping(Map<Integer, Set<Integer>> newToOldChannelIndexes) {
		this.newToOldChannelIndexes = newToOldChannelIndexes;
	}

	public int[] getNewChannelIndexes(int oldChannelIndex) {
		if (newToOldChannelIndexes.isEmpty()) {
			return new int[]{oldChannelIndex};
		}
		if (oldToNewChannelIndexes == null) {
			oldToNewChannelIndexes = invert(newToOldChannelIndexes);
		}
		return oldToNewChannelIndexes.get(oldChannelIndex).stream().mapToInt(Integer::intValue).toArray();
	}

	public int[] getOldChannelIndexes(int newChannelIndex) {
		if (newToOldChannelIndexes.isEmpty()) {
			return new int[]{newChannelIndex};
		}
		return newToOldChannelIndexes.get(newChannelIndex).stream().mapToInt(Integer::intValue).toArray();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final RescaledChannelsMapping that = (RescaledChannelsMapping) o;
		return newToOldChannelIndexes.equals(that.newToOldChannelIndexes);
	}

	@Override
	public int hashCode() {
		return newToOldChannelIndexes.hashCode();
	}

	@Override
	public String toString() {
		return "PartitionMapping{" +
			"newToOldChannelIndexes=" + newToOldChannelIndexes +
			'}';
	}

	static Map<Integer, Set<Integer>> invert(Map<Integer, Set<Integer>> mapping) {
		final Map<Integer, Set<Integer>> inverted = new HashMap<>(mapping.size());
		mapping.forEach((source, targets) -> {
			targets.forEach(target -> {
				inverted.computeIfAbsent(target, unused -> new HashSet<>(targets.size())).add(source);
			});
		});
		return inverted;
	}

}
