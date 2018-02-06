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

package org.apache.flink.runtime.state;

import javax.annotation.Nonnull;

/**
 * This class is a keyed state handle based on a directory. It combines a {@link DirectoryStateHandle} and a
 * {@link KeyGroupRange}.
 */
public class DirectoryKeyedStateHandle implements KeyedStateHandle {

	private static final long serialVersionUID = 1L;

	/** The directory state handle. */
	@Nonnull
	private final DirectoryStateHandle directoryStateHandle;

	/** The key-group range. */
	@Nonnull
	private final KeyGroupRange keyGroupRange;

	public DirectoryKeyedStateHandle(
		@Nonnull DirectoryStateHandle directoryStateHandle,
		@Nonnull KeyGroupRange keyGroupRange) {

		this.directoryStateHandle = directoryStateHandle;
		this.keyGroupRange = keyGroupRange;
	}

	@Nonnull
	public DirectoryStateHandle getDirectoryStateHandle() {
		return directoryStateHandle;
	}

	@Nonnull
	@Override
	public KeyGroupRange getKeyGroupRange() {
		return keyGroupRange;
	}

	@Override
	public void discardState() throws Exception {
		directoryStateHandle.discardState();
	}

	@Override
	public long getStateSize() {
		return directoryStateHandle.getStateSize();
	}

	@Override
	public KeyedStateHandle getIntersection(KeyGroupRange otherKeyGroupRange) {
		return this.keyGroupRange.getIntersection(otherKeyGroupRange).getNumberOfKeyGroups() > 0 ? this : null;
	}

	@Override
	public void registerSharedStates(SharedStateRegistry stateRegistry) {
		// Nothing to do, this is for local use only.
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		DirectoryKeyedStateHandle that = (DirectoryKeyedStateHandle) o;

		if (!getDirectoryStateHandle().equals(that.getDirectoryStateHandle())) {
			return false;
		}
		return getKeyGroupRange().equals(that.getKeyGroupRange());
	}

	@Override
	public int hashCode() {
		int result = getDirectoryStateHandle().hashCode();
		result = 31 * result + getKeyGroupRange().hashCode();
		return result;
	}

	@Override
	public String toString() {
		return "DirectoryKeyedStateHandle{" +
			"directoryStateHandle=" + directoryStateHandle +
			", keyGroupRange=" + keyGroupRange +
			'}';
	}
}
