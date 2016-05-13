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

import org.apache.flink.runtime.state.PartitionedStateSnapshot;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * The partitioned stream operator state after it has been checkpointed. The partitioned state
 * consists of a set of key group snapshots. A key group constitutes a sub set of the available
 * key space. The key groups are indexed by their key group indices.
 */
public class StreamOperatorPartitionedState implements Serializable {
	private static final long serialVersionUID = -8070326169926626355L;

	/** Set of key group snapshots indexed by their key group index */
	private final Map<Integer, PartitionedStateSnapshot> partitionedStateSnapshots;

	public StreamOperatorPartitionedState(Map<Integer, PartitionedStateSnapshot> partitionedStateSnapshots) {
		this.partitionedStateSnapshots = Preconditions.checkNotNull(partitionedStateSnapshots);
	}

	public Map<Integer, PartitionedStateSnapshot> getPartitionedStateSnapshots() {
		return partitionedStateSnapshots;
	}

	public Collection<PartitionedStateSnapshot> values() {
		return partitionedStateSnapshots.values();
	}

	public Set<Map.Entry<Integer, PartitionedStateSnapshot>> entrySet() {
		return partitionedStateSnapshots.entrySet();
	}
}
