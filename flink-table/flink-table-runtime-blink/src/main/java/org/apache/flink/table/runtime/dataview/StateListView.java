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

package org.apache.flink.table.runtime.dataview;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.table.api.dataview.ListView;

import java.util.Collections;
import java.util.List;

/**
 * {@link StateListView} is a {@link ListView} which implemented using state backend.
 *
 * @param <T> the type of element
 */
public abstract class StateListView<N, T> extends ListView<T> implements StateDataView<N> {

	private static final long serialVersionUID = 1L;

	private final Iterable<T> emptyList = Collections.emptyList();

	@Override
	public Iterable<T> get() throws Exception {
		Iterable<T> original = getListState().get();
		return original != null ? original : emptyList;
	}

	@Override
	public void add(T value) throws Exception {
		getListState().add(value);
	}

	@Override
	public void addAll(List<T> list) throws Exception {
		getListState().addAll(list);
	}

	@Override
	public boolean remove(T value) throws Exception {
		List<T> list = (List<T>) getListState().get();
		boolean success = list.remove(value);
		if (success) {
			getListState().update(list);
		}
		return success;
	}

	@Override
	public void clear() {
		getListState().clear();
	}

	protected abstract ListState<T> getListState();

	/**
	 * {@link KeyedStateListView} is an default implementation of {@link StateListView} whose
	 * underlying is a keyed state.
	 */
	public static final class KeyedStateListView<N, T> extends StateListView<N, T> {

		private static final long serialVersionUID = 6526065473887440980L;
		private final ListState<T> listState;

		public KeyedStateListView(ListState<T> listState) {
			this.listState = listState;
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected ListState<T> getListState() {
			return listState;
		}
	}

	/**
	 * {@link NamespacedStateListView} is an {@link StateListView} whose underlying is a keyed
	 * and namespaced state. It also support to change current namespace.
	 */
	public static final class NamespacedStateListView<N, T> extends StateListView<N, T> {
		private static final long serialVersionUID = 1423184510190367940L;
		private final InternalListState<?, N, T> listState;
		private N namespace;

		public NamespacedStateListView(InternalListState<?, N, T> listState) {
			this.listState = listState;
		}

		@Override
		public void setCurrentNamespace(N namespace) {
			this.namespace = namespace;
		}

		@Override
		protected ListState<T> getListState() {
			listState.setCurrentNamespace(namespace);
			return listState;
		}
	}

}
