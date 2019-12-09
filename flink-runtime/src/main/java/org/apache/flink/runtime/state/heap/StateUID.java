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

import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * Unique identifier for registered state in this backend.
 */
final class StateUID {

	@Nonnull
	private final String stateName;

	@Nonnull
	private final StateMetaInfoSnapshot.BackendStateType stateType;

	StateUID(@Nonnull String stateName, @Nonnull StateMetaInfoSnapshot.BackendStateType stateType) {
		this.stateName = stateName;
		this.stateType = stateType;
	}

	@Nonnull
	public String getStateName() {
		return stateName;
	}

	@Nonnull
	public StateMetaInfoSnapshot.BackendStateType getStateType() {
		return stateType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		StateUID uid = (StateUID) o;
		return Objects.equals(getStateName(), uid.getStateName()) &&
			getStateType() == uid.getStateType();
	}

	@Override
	public int hashCode() {
		return Objects.hash(getStateName(), getStateType());
	}

	public static StateUID of(@Nonnull String stateName, @Nonnull StateMetaInfoSnapshot.BackendStateType stateType) {
		return new StateUID(stateName, stateType);
	}
}
