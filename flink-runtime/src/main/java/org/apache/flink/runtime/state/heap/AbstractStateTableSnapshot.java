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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.util.Preconditions;

/**
 * Abstract base class for snapshots of a {@link StateTable}. Offers a way to serialize the snapshot (by key-group).
 * All snapshots should be released after usage.
 */
@Internal
abstract class AbstractStateTableSnapshot<K, N, S, T extends StateTable<K, N, S>> implements StateSnapshot {

	/**
	 * The {@link StateTable} from which this snapshot was created.
	 */
	final T owningStateTable;

	/**
	 * Creates a new {@link AbstractStateTableSnapshot} for and owned by the given table.
	 *
	 * @param owningStateTable the {@link StateTable} for which this object represents a snapshot.
	 */
	AbstractStateTableSnapshot(T owningStateTable) {
		this.owningStateTable = Preconditions.checkNotNull(owningStateTable);
	}

	/**
	 * Optional hook to release resources for this snapshot at the end of its lifecycle.
	 */
	@Override
	public void release() {
	}
}
