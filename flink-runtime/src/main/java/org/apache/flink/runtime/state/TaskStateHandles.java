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

import org.apache.flink.runtime.checkpoint.SubtaskState;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * This class encapsulates all state handles for a task.
 */
public class TaskStateHandles implements Serializable {

	public static final TaskStateHandles EMPTY = new TaskStateHandles();

	private static final long serialVersionUID = 267686583583579359L;

	/**
	 * State handle with the (non-partitionable) legacy operator state
	 *
	 * @deprecated Non-repartitionable operator state that has been deprecated.
	 * Can be removed when we remove the APIs for non-repartitionable operator state.
	 */
	@Deprecated
	private final ChainedStateHandle<StreamStateHandle> legacyOperatorState;

	/** Collection of handles which represent the managed keyed state of the head operator */
	private final Collection<KeyedStateHandle> managedKeyedState;

	/** Collection of handles which represent the raw/streamed keyed state of the head operator */
	private final Collection<KeyedStateHandle> rawKeyedState;

	/** Outer list represents the operator chain, each collection holds handles for managed state of a single operator */
	private final List<Collection<OperatorStateHandle>> managedOperatorState;

	/** Outer list represents the operator chain, each collection holds handles for raw/streamed state of a single operator */
	private final List<Collection<OperatorStateHandle>> rawOperatorState;

	public TaskStateHandles() {
		this(null, null, null, null, null);
	}

	public TaskStateHandles(SubtaskState checkpointStateHandles) {
		this(checkpointStateHandles.getLegacyOperatorState(),
				transform(checkpointStateHandles.getManagedOperatorState()),
				transform(checkpointStateHandles.getRawOperatorState()),
				transform(checkpointStateHandles.getManagedKeyedState()),
				transform(checkpointStateHandles.getRawKeyedState()));
	}

	public TaskStateHandles(
			ChainedStateHandle<StreamStateHandle> legacyOperatorState,
			List<Collection<OperatorStateHandle>> managedOperatorState,
			List<Collection<OperatorStateHandle>> rawOperatorState,
			Collection<KeyedStateHandle> managedKeyedState,
			Collection<KeyedStateHandle> rawKeyedState) {

		this.legacyOperatorState = legacyOperatorState;
		this.managedKeyedState = managedKeyedState;
		this.rawKeyedState = rawKeyedState;
		this.managedOperatorState = managedOperatorState;
		this.rawOperatorState = rawOperatorState;
	}

	/**
	 * @deprecated Non-repartitionable operator state that has been deprecated.
	 * Can be removed when we remove the APIs for non-repartitionable operator state.
	 */
	@Deprecated
	public ChainedStateHandle<StreamStateHandle> getLegacyOperatorState() {
		return legacyOperatorState;
	}

	public Collection<KeyedStateHandle> getManagedKeyedState() {
		return managedKeyedState;
	}

	public Collection<KeyedStateHandle> getRawKeyedState() {
		return rawKeyedState;
	}

	public List<Collection<OperatorStateHandle>> getRawOperatorState() {
		return rawOperatorState;
	}

	public List<Collection<OperatorStateHandle>> getManagedOperatorState() {
		return managedOperatorState;
	}

	private static List<Collection<OperatorStateHandle>> transform(ChainedStateHandle<OperatorStateHandle> in) {
		if (null == in) {
			return Collections.emptyList();
		}
		List<Collection<OperatorStateHandle>> out = new ArrayList<>(in.getLength());
		for (int i = 0; i < in.getLength(); ++i) {
			OperatorStateHandle osh = in.get(i);
			out.add(osh != null ? Collections.singletonList(osh) : null);
		}
		return out;
	}

	private static <T> List<T> transform(T in) {
		return in == null ? Collections.<T>emptyList() : Collections.singletonList(in);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		TaskStateHandles that = (TaskStateHandles) o;

		if (legacyOperatorState != null ?
				!legacyOperatorState.equals(that.legacyOperatorState)
				: that.legacyOperatorState != null) {
			return false;
		}
		if (managedKeyedState != null ?
				!managedKeyedState.equals(that.managedKeyedState)
				: that.managedKeyedState != null) {
			return false;
		}
		if (rawKeyedState != null ?
				!rawKeyedState.equals(that.rawKeyedState)
				: that.rawKeyedState != null) {
			return false;
		}

		if (rawOperatorState != null ?
				!rawOperatorState.equals(that.rawOperatorState)
				: that.rawOperatorState != null) {
			return false;
		}
		return managedOperatorState != null ?
				managedOperatorState.equals(that.managedOperatorState)
				: that.managedOperatorState == null;
	}

	@Override
	public int hashCode() {
		int result = legacyOperatorState != null ? legacyOperatorState.hashCode() : 0;
		result = 31 * result + (managedKeyedState != null ? managedKeyedState.hashCode() : 0);
		result = 31 * result + (rawKeyedState != null ? rawKeyedState.hashCode() : 0);
		result = 31 * result + (managedOperatorState != null ? managedOperatorState.hashCode() : 0);
		result = 31 * result + (rawOperatorState != null ? rawOperatorState.hashCode() : 0);
		return result;
	}
}
