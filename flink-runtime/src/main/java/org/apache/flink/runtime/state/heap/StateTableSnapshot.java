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

import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/**
 * Abstract class to encapsulate the logic to take snapshots of {@link StateTable} implementations.
 */
public abstract class StateTableSnapshot<K, N, S, T extends StateTable<K, N, S>> {

	/**
	 * The {@link StateTable} from which this snapshot was created.
	 */
	final T owningStateTable;


	/**
	 * Creates a new {@link StateTableSnapshot} for and owned by the given table.
	 *
	 * @param owningStateTable the {@link StateTable} for which this object represents a snapshot.
	 */
	StateTableSnapshot(T owningStateTable) {
		this.owningStateTable = Preconditions.checkNotNull(owningStateTable);
	}

	/**
	 * Writes the data for the specified key-group to the output.
	 *
	 * @param dov the output
	 * @param keyGroupId the key-group to write
	 * @throws IOException on write related problems
	 */
	public abstract void writeMappingsInKeyGroup(DataOutputView dov, int keyGroupId) throws IOException;

	/**
	 * Optional hook to release resources for this snapshot at the end of its lifecycle.
	 */
	public void release() {
	}
}