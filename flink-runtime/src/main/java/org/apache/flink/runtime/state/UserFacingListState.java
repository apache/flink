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

import org.apache.flink.api.common.state.ListState;

import java.util.Collections;
import java.util.List;

/**
 * Simple wrapper list state that exposes empty state properly as an empty list.
 * 
 * @param <T> The type of elements in the list state.
 */
class UserFacingListState<T> implements ListState<T> {

	private final ListState<T> originalState;

	private final Iterable<T> emptyState = Collections.emptyList();

	UserFacingListState(ListState<T> originalState) {
		this.originalState = originalState;
	}

	// ------------------------------------------------------------------------

	@Override
	public Iterable<T> get() throws Exception {
		Iterable<T> original = originalState.get();
		return original != null ? original : emptyState;
	}

	@Override
	public void add(T value) throws Exception {
		originalState.add(value);
	}

	@Override
	public void clear() {
		originalState.clear();
	}

	@Override
	public void update(List<T> values) throws Exception {
		originalState.update(values);
	}

	@Override
	public void addAll(List<T> values) throws Exception {
		originalState.addAll(values);
	}
}
